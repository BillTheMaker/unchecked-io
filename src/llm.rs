// src/llm.rs
#![cfg(feature = "gpu")]

use std::sync::Arc;
use tokenizers::Tokenizer;
use cudarc::driver::{CudaDevice, DeviceSlice, CudaSlice, DevicePtr};
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyCapsule;
use std::ffi::CStr;
use rayon::prelude::*;

// Import raw CUDA symbols. Note the _v2 suffixes which are required by the sys crate.
use cuda_driver_sys::{
    cuMemHostRegister_v2,
    cuMemHostUnregister,
    cuMemcpyHtoD_v2,
    cudaError_enum
};

// --- DLPack v0.4 ABI Definitions ---
#[repr(C)]
struct DLDataType { code: u8, bits: u8, lanes: u16 }
#[repr(C)]
struct DLDevice { device_type: i32, device_id: i32 }
#[repr(C)]
struct DLTensor {
    data: *mut std::ffi::c_void,
    device: DLDevice,
    ndim: i32,
    dtype: DLDataType,
    shape: *mut i64,
    strides: *mut i64,
    byte_offset: u64,
}
#[repr(C)]
struct DLManagedTensor {
    dl_tensor: DLTensor,
    manager_ctx: *mut std::ffi::c_void,
    deleter: Option<unsafe extern "C" fn(*mut DLManagedTensor)>,
}

const CAPSULE_NAME: &CStr = unsafe { CStr::from_bytes_with_nul_unchecked(b"dltensor\0") };

struct PinnedHostBuffer {
    data: Vec<i64>,
}

impl PinnedHostBuffer {
    fn new(capacity: usize) -> PyResult<Self> {
        let data = vec![0i64; capacity];
        let ptr = data.as_ptr() as *mut std::ffi::c_void;
        let bytes = capacity * std::mem::size_of::<i64>();

        unsafe {
            // Use _v2 variant
            let result = cuMemHostRegister_v2(ptr, bytes, 0);
            if result != cudaError_enum::CUDA_SUCCESS {
                return Err(PyValueError::new_err(format!("Failed to Pin Memory: {:?}", result)));
            }
        }

        Ok(Self { data })
    }
}

impl Drop for PinnedHostBuffer {
    fn drop(&mut self) {
        unsafe {
            let ptr = self.data.as_ptr() as *mut std::ffi::c_void;
            cuMemHostUnregister(ptr);
        }
    }
}

pub struct PinnedBatcher {
    tokenizer: Tokenizer,
    device: Arc<CudaDevice>,
    gpu_buffer: CudaSlice<i64>,
    host_buffer: PinnedHostBuffer,
    max_elements: usize,
}

impl PinnedBatcher {
    pub fn new(model_path: &str) -> PyResult<Self> {
        let tokenizer = Tokenizer::from_file(model_path)
            .map_err(|e| PyValueError::new_err(format!("Failed to load tokenizer: {}", e)))?;

        let device = CudaDevice::new(0)
            .map_err(|e| PyValueError::new_err(format!("No CUDA GPU found: {:?}", e)))?;

        let max_elements = 4096 * 512;

        let gpu_buffer = device.alloc_zeros::<i64>(max_elements)
            .map_err(|e| PyValueError::new_err(format!("GPU Alloc Failed: {:?}", e)))?;

        let host_buffer = PinnedHostBuffer::new(max_elements)?;

        Ok(PinnedBatcher {
            tokenizer,
            device,
            gpu_buffer,
            host_buffer,
            max_elements,
        })
    }

    pub fn batch_encode_to_gpu(&mut self, py: Python, texts: Vec<String>) -> PyResult<Py<PyAny>> {
        let encodings = self.tokenizer.encode_batch(texts, true)
            .map_err(|e| PyValueError::new_err(format!("Tokenization failed: {}", e)))?;

        if encodings.is_empty() {
            return Err(PyValueError::new_err("Empty batch provided"));
        }

        let batch_size = encodings.len();
        let seq_len = encodings[0].len();
        let total_elements = batch_size * seq_len;

        if total_elements > self.max_elements {
            return Err(PyValueError::new_err(format!(
                "Batch too large: {} tokens (Max: {})",
                total_elements, self.max_elements
            )));
        }

        // Parallel Write into Pinned Memory
        // Explicit types added to closure to satisfy type inference
        self.host_buffer.data[..total_elements].par_chunks_mut(seq_len)
            .zip(encodings.par_iter())
            .for_each(|(dest, enc): (&mut [i64], &tokenizers::Encoding)| {
                let ids = enc.get_ids();
                let len = ids.len().min(dest.len());
                for i in 0..len {
                    dest[i] = ids[i] as i64;
                }
            });

        // Direct DMA Copy (Pinned Host -> Device)
        unsafe {
            let src_ptr = self.host_buffer.data.as_ptr() as *const std::ffi::c_void;
            let dst_ptr = *self.gpu_buffer.device_ptr();
            let bytes = total_elements * std::mem::size_of::<i64>();

            let result = cuMemcpyHtoD_v2(dst_ptr, src_ptr, bytes);

            if result != cudaError_enum::CUDA_SUCCESS {
                return Err(PyValueError::new_err(format!("Direct GPU Copy Failed: {:?}", result)));
            }
        }

        // Zero-Copy Handover
        let ptr_address = *self.gpu_buffer.device_ptr() as *mut std::ffi::c_void;
        let shape = Box::into_raw(vec![batch_size as i64, seq_len as i64].into_boxed_slice()) as *mut i64;

        let dl_tensor = DLTensor {
            data: ptr_address,
            device: DLDevice { device_type: 2, device_id: 0 },
            ndim: 2,
            dtype: DLDataType { code: 0, bits: 64, lanes: 1 },
            shape,
            strides: std::ptr::null_mut(),
            byte_offset: 0,
        };

        let managed_tensor = Box::new(DLManagedTensor {
            dl_tensor,
            manager_ctx: std::ptr::null_mut(),
            deleter: Some(dlpack_deleter),
        });

        let managed_ptr = Box::into_raw(managed_tensor);

        unsafe {
            let capsule_ptr = pyo3::ffi::PyCapsule_New(
                managed_ptr as *mut _,
                CAPSULE_NAME.as_ptr(),
                None
            );

            if capsule_ptr.is_null() {
                return Err(PyValueError::new_err("Failed to create PyCapsule"));
            }

            Ok(Bound::from_owned_ptr(py, capsule_ptr).into_any().unbind())
        }
    }
}

unsafe extern "C" fn dlpack_deleter(managed_ptr: *mut DLManagedTensor) {
    if managed_ptr.is_null() { return; }
    let managed = Box::from_raw(managed_ptr);
    if !managed.dl_tensor.shape.is_null() {
        let _ = Box::from_raw(std::slice::from_raw_parts_mut(
            managed.dl_tensor.shape,
            managed.dl_tensor.ndim as usize
        ));
    }
}
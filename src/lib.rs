mod config;
mod parser;

#[cfg(feature = "gpu")]
mod llm;

use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use tokio;
use pyo3::types::{PyModule, PyAny};
use pyo3::Bound;
use pyo3_arrow::export::Arro3RecordBatch;

#[cfg(not(target_env = "msvc"))]
use mimalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use crate::config::{load_and_validate_config, ConnectorConfig};
use crate::parser::{run_db_logic, run_profiler_logic};

#[cfg(feature = "profiling")]
use tracing_subscriber::layer::SubscriberExt;
#[cfg(feature = "profiling")]
use tracing_subscriber::util::SubscriberInitExt;

#[pyfunction]
#[pyo3(signature = (config_path, blast_radius=0))]
fn load_data_from_config<'py>(
    py: Python<'py>,
    config_path: String,
    blast_radius: i64,
) -> PyResult<Bound<'py, PyAny>> {

    let config: ConnectorConfig = match load_and_validate_config(&config_path) {
        Ok(c) => c,
        Err(e) => return Err(PyValueError::new_err(format!("Configuration Error: {:?}", e))),
    };

    let record_batch = py.allow_threads(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                run_db_logic(config, blast_radius).await
            })
    }).map_err(|e| PyValueError::new_err(format!("Database/Runtime Error: {:?}", e)))?;

    let py_record_batch = Arro3RecordBatch::from(record_batch);
    py_record_batch.into_pyobject(py)
}

#[pyfunction]
#[pyo3(signature = (arrow_table))]
fn to_pandas_dataframe<'py>(_py: Python<'py>, arrow_table: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    arrow_table.call_method0("to_pandas")
}

#[pyfunction]
#[pyo3(signature = (config_path))]
fn profile_data(config_path: String) -> PyResult<String> {
    let output = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                run_profiler_logic(&config_path).await
            })
    }).join().unwrap().map_err(|e| PyValueError::new_err(format!("Profiling Error: {:?}", e)))?;

    Ok(output)
}

#[cfg(feature = "gpu")]
#[pyclass(name = "TokenizerEngine")]
struct TokenizerEngine {
    inner: llm::PinnedBatcher,
}

#[cfg(feature = "gpu")]
#[pymethods]
impl TokenizerEngine {
    #[new]
    fn new(model_path: String) -> PyResult<Self> {
        Ok(TokenizerEngine {
            inner: llm::PinnedBatcher::new(&model_path)?
        })
    }

    fn encode_batch(&mut self, py: Python, texts: Vec<String>) -> PyResult<Py<PyAny>> {
        self.inner.batch_encode_to_gpu(py, texts)
    }
}

#[pymodule]
fn unchecked_io(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {

    #[cfg(feature = "profiling")]
    {
        let _ = tracing_subscriber::registry()
            .with(tracing_tracy::TracyLayer::default())
            .try_init();
        println!("UncheckedIO: Profiling Mode ENABLED 🚀");
    }

    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;
    m.add_function(wrap_pyfunction!(to_pandas_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(profile_data, m)?)?;

    #[cfg(feature = "gpu")]
    {
        m.add_class::<TokenizerEngine>()?;
        println!("UncheckedIO: GPU Acceleration ENABLED ⚡");
    }

    Ok(())
}
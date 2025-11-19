// --- Declare our new modules ---
mod config;
mod parser;

// --- External Crates ---
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use tokio;
use pyo3::types::{PyModule, PyAny};
use pyo3::Bound;
use pyo3_arrow::PyRecordBatch;
// Required for global allocator (mimalloc)
#[cfg(not(target_env = "msvc"))]
use mimalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;


// --- Internal Crates ---
use crate::config::{load_and_validate_config, ConnectorConfig};
// NOTE: run_profiler_logic added, requires definition in parser.rs
use crate::parser::{run_db_logic, run_profiler_logic};

use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
// --- THE PYTHON-CALLABLE ENTRY POINT (Load Data) ---

#[pyfunction]
#[pyo3(signature = (config_path, blast_radius=312500))]
#[allow(unsafe_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(rust_2024_compatibility)]
fn load_data_from_config<'py>(
    py: Python<'py>,
    config_path: String,
    blast_radius: i64,
) -> PyResult<Bound<'py, PyAny>> {

    let config: ConnectorConfig = match load_and_validate_config(&config_path) {
        Ok(c) => c,
        Err(e) => return Err(PyValueError::new_err(format!("Configuration Error: {:?}", e))),
    };

    println!("--- UncheckedIO: Schema Accepted ---");
    println!("Database: {}", config.connection_string);
    println!("Columns (in order): {:?}", config.schema.iter().map(|c| &c.column_name).collect::<Vec<_>>());

    let record_batch = py.allow_threads(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                run_db_logic(config, blast_radius).await
            })
    }).map_err(|e| PyValueError::new_err(format!("Database/Runtime Error: {:?}", e)))?;

    let py_record_batch = PyRecordBatch::new(record_batch);
    py_record_batch.into_pyarrow(py)
}

// --- NEW PYTHON-CALLABLE FUNCTION FOR PANDAS CONVERSION (FIXED SIGNATURE) ---
#[pyfunction]
#[pyo3(signature = (arrow_table))] // FIX: Removed 'py' from the signature macro
fn to_pandas_dataframe<'py>(py: Python<'py>, arrow_table: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    // This calls the 'to_pandas' method on the PyArrow object.
    arrow_table.call_method0("to_pandas")
}

// --- NEW PYTHON-CALLABLE ENTRY POINT FOR SCHEMA PROFILING ---
#[pyfunction]
#[pyo3(signature = (config_path))]
fn profile_data(config_path: String) -> PyResult<String> {
    // Note: The profiler uses a small, current_thread tokio runtime since it's sequential I/O.
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


// --- PYTHON MODULE EXPORT ---
#[pymodule]
fn unchecked_io(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {

    let _ = tracing_subscriber::registry()
        .with(tracing_tracy::TracyLayer::default())
        .try_init();

    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;
    m.add_function(wrap_pyfunction!(to_pandas_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(profile_data, m)?)?; // NEW: Schema profiler
    Ok(())
}
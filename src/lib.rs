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

// --- Internal Crates ---
use crate::config::{load_and_validate_config, ConnectorConfig};
use crate::parser::run_db_logic;


// The function signature sets the default for blast_radius and DANGER_MODE
#[pyfunction]
#[pyo3(signature = (config_path, blast_radius=62500, danger_mode=true))]
#[allow(unsafe_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(rust_2024_compatibility)]
fn load_data_from_config<'py>(
    py: Python<'py>,
    config_path: String,
    blast_radius: i64,
    danger_mode: bool // Defaults to true if not passed from Python
) -> PyResult<Bound<'py, PyAny>> {

    let config: ConnectorConfig = match load_and_validate_config(&config_path) {
        Ok(c) => c,
        Err(e) => return Err(PyValueError::new_err(format!("Configuration Error: {:?}", e))),
    };

    println!("--- UncheckedIO: Schema Accepted ---");
    println!("Database: {}", config.connection_string);

    let record_batch = py.allow_threads(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // Passes the defaulted danger_mode value
                run_db_logic(config, blast_radius, danger_mode).await
            })
    }).map_err(|e| PyValueError::new_err(format!("Database/Runtime Error: {:?}", e)))?;

    let py_record_batch = PyRecordBatch::new(record_batch);
    py_record_batch.into_pyarrow(py)
}


// --- PYTHON MODULE EXPORT ---
#[pymodule]
fn unchecked_io(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;
    Ok(())
}
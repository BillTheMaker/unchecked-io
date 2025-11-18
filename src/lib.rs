// --- Declare our new modules ---
mod config;
mod parser;

// --- External Crates ---
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use tokio;
use pyo3::types::{PyModule, PyAny}; // Added PyAny
use pyo3::Bound;

// FIX 1: Import PyRecordBatch.
use pyo3_arrow::PyRecordBatch;


// --- Internal Crates ---
use crate::config::{load_and_validate_config, ConnectorConfig};
use crate::parser::run_db_logic;


// --- THE PYTHON-CALLABLE ENTRY POINT ---

#[pyfunction]
#[allow(unsafe_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(rust_2024_compatibility)]
// FIX 2: Updated signature to accept 'blast_radius' and return 'Bound<'py, PyAny>'
// This matches what you confirmed works with .into_pyarrow(py).
fn load_data_from_config<'py>(
    py: Python<'py>,
    config_path: String,
    blast_radius: i64
) -> PyResult<Bound<'py, PyAny>> {

    // --- Phase 1: Load and Validate Configuration ---
    let config: ConnectorConfig = match load_and_validate_config(&config_path) {
        Ok(c) => c,
        Err(e) => return Err(PyValueError::new_err(format!("Configuration Error: {:?}", e))),
    };

    println!("--- UncheckedIO: Schema Accepted ---");
    println!("Database: {}", config.connection_string);
    println!("Columns (in order): {:?}", config.schema.iter().map(|c| &c.column_name).collect::<Vec<_>>());

    // --- Phase 2: Run Core Logic ---
    let record_batch = py.allow_threads(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // FIX 3: Pass the 'blast_radius' argument to the parser logic
                run_db_logic(config, blast_radius).await
            })
    }).map_err(|e| PyValueError::new_err(format!("Database/Runtime Error: {:?}", e)))?;

    // --- Phase 3: Return Data to Python ---

    // Create the wrapper. .new() takes one argument in this version.
    let py_record_batch = PyRecordBatch::new(record_batch);

    // Call the correct conversion method which returns PyResult<Bound<'py, PyAny>>
    py_record_batch.into_pyarrow(py)
}


// --- PYTHON MODULE EXPORT ---

#[pymodule]
fn unchecked_io(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // FIX 4: Use the two-argument wrap_pyfunction! macro pattern which you confirmed works.
    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;

    Ok(())
}
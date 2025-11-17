// --- Declare our new modules ---
mod config;
mod parser;

// --- External Crates ---
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use tokio;

// --- Internal Crates ---
use crate::config::{load_and_validate_config, ConnectorConfig};
use crate::parser::run_db_logic;

// FIX: Import the Arrow bridge trait
use pyo3_arrow::PyArrowConvert;

// --- THE PYTHON-CALLABLE ENTRY POINT ---

#[pyfunction]
#[allow(unsafe_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(rust_2024_compatibility)]
// FIX: Change the return type from () to PyObject
fn load_data_from_config(py: Python, config_path: String) -> PyResult<PyObject> {
    // --- Phase 1: Load and Validate Configuration ---
    let config: ConnectorConfig = match load_and_validate_config(&config_path) {
        Ok(c) => c,
        Err(e) => return Err(PyValueError::new_err(format!("Configuration Error: {:?}", e))),
    };

    println!("--- UncheckedIO: Schema Accepted ---");
    println!("Database: {}", config.connection_string);
    println!("Columns (in order): {:?}", config.schema.iter().map(|c| &c.column_name).collect::<Vec<_>>());

    // --- Phase 2: Run Core Logic ---
    // We release the GIL and start our own Tokio runtime
    let record_batch = py.allow_threads(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                run_db_logic(config).await
            })
    }).map_err(|e| PyValueError::new_err(format!("Database/Runtime Error: {:?}", e)))?;

    // --- Phase 3: Return Data to Python ---
    // FIX: Convert the Rust RecordBatch to a Python PyObject (a pyarrow.Table)
    // This is a ZERO-COPY operation.
    record_batch.to_pyarrow(py)
}


// --- PYTHON MODULE EXPORT ---

#[pymodule]
// FIX: Add the attribute to enable Arrow <-> Python conversion
#[pyo3(with_arrow)]
fn unchecked_io(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;
    Ok(())
}
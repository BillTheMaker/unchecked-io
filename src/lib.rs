// --- Declare our new modules ---
// This tells Rust to look for src/config.rs and src/parser.rs
mod config;
mod parser;

// --- External Crates ---
use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use tokio; // We need the tokio runtime here

// --- Internal Crates ---
// We 'use' the public functions from our new modules
use crate::config::{load_and_validate_config, ConnectorConfig};
use crate::parser::run_db_logic;


// --- THE PYTHON-CALLABLE ENTRY POINT ---

#[pyfunction]
#[allow(unsafe_code)]
#[allow(unsafe_op_in_unsafe_fn)]
#[allow(rust_2024_compatibility)]
fn load_data_from_config(py: Python, config_path: String) -> PyResult<()> {
    // --- Phase 1: Load and Validate Configuration ---
    // We call the function that now lives in src/config.rs
    let config: ConnectorConfig = match load_and_validate_config(&config_path) {
        Ok(c) => c,
        Err(e) => return Err(PyValueError::new_err(format!("Configuration Error: {:?}", e))),
    };

    println!("--- UncheckedIO: Schema Accepted ---");
    println!("Database: {}", config.connection_string);
    println!("Columns (in order): {:?}", config.schema.iter().map(|c| &c.column_name).collect::<Vec<_>>());

    // --- Phase 2: Run Core Logic ---
    // We release the GIL and start our own Tokio runtime
    py.allow_threads(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                // We call the function that now lives in src/parser.rs
                run_db_logic(config).await
            })
    }).map_err(|e| PyValueError::new_err(format!("Database/Runtime Error: {:?}", e)))?;

    Ok(())
}


// --- PYTHON MODULE EXPORT ---

#[pymodule]
fn unchecked_io(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;
    Ok(())
}
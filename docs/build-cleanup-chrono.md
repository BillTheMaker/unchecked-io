Technical Task: Build Fixes & Profiler Cleanup
Date: 2025-11-21 Goal: Fix PyPI build failures and remove heavy profiling code from production wheels.

1. Fix PyPI Build Failure (chrono-tz)
   Issue: The build fails on Linux/Mac/Windows CI because chrono-tz requires complex C-bindings that are often missing in standard Python environments. Analysis: UncheckedIO returns Apache Arrow RecordBatch objects (where timestamps are raw i64). We do not need pyo3 to convert Rust chrono types directly to Python datetime objects. The chrono-tz feature in pyo3 is dead weight causing build failures.

Action: Update Cargo.toml to remove the conflicting feature.

Ini, TOML

# Cargo.toml

# OLD
# pyo3 = { version = "0.27.1", features = ["extension-module", "chrono-tz"] }

# NEW (Remove "chrono-tz")
pyo3 = { version = "0.27.1", features = ["extension-module"] }
2. Productionize the Profiler (Remove Tracy)
   Issue: The tracy-client and tracing-tracy crates add significant binary size and runtime overhead. They should not be present in the production library installed by users. Solution: Use Rust Feature Flags to make profiling "opt-in" for development only.

Step A: Update Cargo.toml Define a profiling feature and make the heavy dependencies optional.

Ini, TOML

[features]
default = []
# The "profiling" feature enables these two optional crates
profiling = ["dep:tracing-tracy", "dep:tracy-client"]

[dependencies]
# Keep this (Lightweight facade)
tracing = "0.1.41"

# Make these OPTIONAL (Heavy implementation)
tracing-tracy = { version = "0.11.2", optional = true }
tracy-client = { version = "0.17.6", optional = true }

# ... other dependencies ...
Step B: Update src/lib.rs Wrap the profiler initialization logic so it only compiles when the flag is active.

Rust

// src/lib.rs

#[pymodule]
fn unchecked_io(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {

    // --- START PROFILER LOGIC ---
    // This block now ONLY compiles if you run with `--features profiling`
    #[cfg(feature = "profiling")] 
    {
        use tracing_subscriber::layer::SubscriberExt;
        use tracing_subscriber::util::SubscriberInitExt;
        
        // Initialize the Tracy layer
        let _ = tracing_subscriber::registry()
            .with(tracing_tracy::TracyLayer::default())
            .try_init();
            
        println!("--- UncheckedIO: Profiling Enabled (Tracy) ---");
    }
    // --- END PROFILER LOGIC ---

    m.add_function(wrap_pyfunction!(load_data_from_config, m)?)?;
    m.add_function(wrap_pyfunction!(to_pandas_dataframe, m)?)?;
    m.add_function(wrap_pyfunction!(profile_data, m)?)?;
    Ok(())
}
3. How to Build
   For Development (With Profiler): Use this when you want to see the tracks in the Tracy GUI.

Bash

maturin develop --features profiling
For Production / PyPI (Zero Overhead): This builds the "clean" version for users. The profiler code is stripped out entirely.

Bash

maturin publish

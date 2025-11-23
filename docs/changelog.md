Changelog
[Unreleased] - Sprint 4 Optimization
🚀 Performance
Speedup: Achieved 1.46x speedup over ConnectorX (19s vs 28s for 20M rows) on local benchmarks.
Worker Pool: Implemented a thread-per-core architecture using async_channel to decouple task generation from execution.
Auto-Tuning: Added blast_radius=0 support to dynamically calculate optimal partition sizes based on available CPU cores.
🛠️ Fixes & Stability
Self-Healing: Schema now defaults to nullable=true. If a partition fails (network error, bad data), the worker logs the error and returns a batch of NULLs instead of crashing the entire process.
Dependency Resolution: Fixed build errors with pyo3-arrow 0.15.0 by migrating to Arro3RecordBatch::from and into_pyobject.
Profiling Safety: tracing and tracy-client are now optional dependencies behind the profiling feature flag. This prevents overhead and crashes in production environments like Google Colab.
⚠️ Known Issues / Constraints
Severe Partition Switching Overhead: Increasing partition counts causes non-linear runtime growth.
Observation: 160 partitions took ~50s longer than 16 partitions.
Analysis: Each worker thread incurs a ~5-second setup penalty when switching to a new partition (establishing a new COPY stream).
Implication: While CPU-bound logic scales perfectly, the COPY protocol handshake or Docker networking layer is forcing massive latency spikes per task.
Recommendation: Avoid small partitions on high-latency networks (like Docker-on-Windows). Stick to num_cpus partitions until this handshake cost is optimized.
I/O Bound: Benchmarks with 8 vs 16 workers showed similar performance, indicating the system is currently Network/IO latency bound, not CPU bound.
📦 Build Instructions
Production: maturin build --release (Default)
Debug/Profile: maturin build --release --features profiling



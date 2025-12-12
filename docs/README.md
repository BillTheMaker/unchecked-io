UncheckedIO 🚀
The world's fastest, most dangerous PostgreSQL-to-Arrow loader.
UncheckedIO is an opinionated, high-performance data connector designed for bulk-loading massive datasets (Terabytes+) from PostgreSQL into Python/Apache Arrow. It achieves extreme speed by skipping runtime schema validation and using a zero-copy streaming parser.
⚠️ WARNING: This library assumes you know what you are doing. If your config.yaml schema does not match your database schema, it will produce garbage data or crash. The "Unchecked" in the name is not a suggestion; it is a promise.
Features
🚀 1.5x - 2x Faster than ConnectorX when Benchmarked on 20M+ row dataset in default Colab runtime.
🧵 Parallel Worker Pool: Automatically scales to available CPU cores.
🧠 Auto-Tuning: Dynamically calculates optimal partition sizes (blast_radius).
📉 Low Memory Footprint: Streaming parser with zero-copy string slicing.
🛠️ "Self-Healing" (Nullable): If a partition fails (e.g., network flake), it returns NULLs for that chunk instead of crashing the entire job.
🔬 Optional Profiling: Built-in integration with the Tracy Profiler for deep I/O analysis.
Installation
pip install unchecked-io

Quick Start
Create a config.yaml file:
connection_string: postgresql://user:password@localhost:5432/dbname
# query MUST be a COPY ... TO STDOUT (FORMAT binary) command
query: >
COPY (SELECT id, name, score, created_at FROM users)
TO STDOUT (FORMAT binary)
schema:
- column_name: id
  arrow_type: Int64
- column_name: name
  arrow_type: Utf8
- column_name: score
  arrow_type: Float32
- column_name: created_at
  arrow_type: Timestamp(Nanosecond, None)


Run it in Python:
import unchecked_io

# blast_radius=0 enables auto-tuning
arrow_table = unchecked_io.load_data_from_config("config.yaml", blast_radius=0)

# Convert to Pandas (Zero-Copy)
df = unchecked_io.to_pandas_dataframe(arrow_table)
print(df.head())


🔬 Profiling with Tracy
UncheckedIO includes optional instrumentation for the Tracy Profiler to visualize I/O starvation and thread contention.
This feature is DISABLED by default to prevent overhead and crashes in production environments (like Google Colab).
How to Enable Profiling
You must build the library from source with the profiling feature enabled.
Install Rust & Maturin:
curl --proto '=https' --tlsv1.2 -sSf [https://sh.rustup.rs](https://sh.rustup.rs) | sh
pip install maturin


Build with Feature Flag:
# This builds the wheel and installs it in your current venv
maturin develop --release --features profiling


Run Tracy (GUI):
Download Tracy v0.11.1 (Must match protocol v0.11.x).
Open Tracy.exe and click Connect.
Run your Python script. You will see real-time thread timelines and "IO_WAIT_STARVATION" blocks.
Configuration Reference
Supported Types
PostgreSQL Type
Config arrow_type
bigint / int8
Int64
integer / int4
Int32
double precision
Float64
real / float4
Float32
text / varchar / uuid
Utf8
boolean
Boolean
timestamp
Timestamp(Nanosecond, None)
date
Date32

blast_radius Parameter
0: Auto-Tune (Recommended). Calculates partition size based on row count / (cores * 4).
> 0: Manual Override. Sets specific number of rows per partition. Use for fine-tuning on specific hardware.
License
Apache 2.0
Free for non-production use and production use.

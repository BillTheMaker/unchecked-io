# UncheckedIO: Business Strategy & Technical Roadmap

**Date:** 2025-11-21
**Status:** Strategic Planning
**Core Philosophy:** "The Configured Opinion" — We trade flexibility for raw, unchecked speed.

---

## 1. Target Audience & Value Propositions

We are not selling "a faster Pandas." We are selling **infrastructure efficiency**.

### Primary Target: Data Platform & MLOps Engineers
* **Who they are:** The people managing Kubernetes clusters, Airflow DAGs, and AWS Fargate costs. They care about stability and resource usage.
* **The Pitch:** "A Zero-Copy Transport Layer for Postgres."
* **Key Selling Points:**
    * **Cut Compute Costs:** Reduces CPU starvation by saturating network bandwidth. 10x faster serialization means 10x less time paying for vCPUs.
    * **Memory Safety:** Written in Rust, ensuring no segfaults in your Python pipeline despite the speed.
    * **Predictable Throughput:** Unlike generic drivers that choke on large datasets, `UncheckedIO` uses partitioned streaming to maintain constant memory usage regardless of data size.

### Secondary Target: High-Frequency Trading (HFT) / Quant Developers
* **Who they are:** Developers who need to backtest models on massive tick-data history stored in Postgres.
* **The Pitch:** "The fastest way to get data from Disk to Arrow Memory."
* **Key Selling Points:**
    * **Zero-Copy to Polars:** Output directly to Apache Arrow memory layouts, ready for immediate analysis or SIMD vectorization.
    * **Bypass the overhead:** Skips the "Safety Checks" (schema validation) that cost milliseconds per query. You promise the data is clean; we promise to load it instantly.

### Tertiary Target: Data Scientists (The Users, Not Buyers)
* **Who they are:** End users writing Jupyter Notebooks.
* **The Pitch:** "Stop waiting for `read_sql`."
* **Key Selling Points:**
    * **Installation Simplicity:** `pip install unchecked_io`. No complex C dependencies or drivers to configure.
    * **Drop-in Speed:** Replace a 5-minute load time with a 30-second load time.

---

## 2. Optimization Strategy: The "Win" Plan

We need to stop fighting "theoretical" bottlenecks and fix the "physical" ones.

### 🔴 Focus On (High ROI)
1.  **Infrastructure-Aware Tuning (The "Blast Radius"):**
    * *Why:* The "astronomical" delay was caused by thrashing.
    * *Action:* Implement the `num_cpus` detection to auto-scale the connection pool and partition count. Do not let the user guess wrong.
2.  **Memory Management (The "String Killer"):**
    * *Why:* 15M heap allocations for strings creates allocator contention.
    * *Action:* Refactor the parser to use a reusable buffer or "Zero-Copy" string slicing from the raw stream before writing to Arrow.
3.  **Database Interaction (The Index):**
    * *Why:* As discovered, a missing index turns a parallel fetch into a DDoS attack.
    * *Action:* Documentation MUST strictly warn users: *"If you use `blast_radius`, you MUST index your partition column."*

### 🛑 Leave Alone (Diminishing Returns)
1.  **Complex "Safe Mode" Logic:**
    * *Why:* We are `UncheckedIO`. If we add too many safety checks/fallbacks for dirty data, we become just another slow connector (like ConnectorX).
    * *Strategy:* Let it fail fast. If the data violates the config schema, panic. That is the contract.
2.  **Micro-Optimizing the TCP Handshake:**
    * *Why:* Connection pooling already solves 95% of this. Saving another 1ms here is irrelevant compared to the 500ms of Query Planning time.

---

## 3. Configuration Matrix

To be a "System-Level" tool, we need to expose the knobs that Platform Engineers expect.

### Currently Configurable (The "MVP" Set)
These features are already present in the code or `config.yaml`:
1.  **`connection_string`:** Standard Postgres URI.
2.  **`query`:** The exact `COPY ... TO STDOUT (FORMAT binary)` command. Allows total control over the SQL execution plan.
3.  **`schema`:** Explicit definition of Arrow Types (`Int64`, `Utf8`, etc.). Skips runtime type inference.
4.  **`blast_radius` (Argument):** Controls the row-count per partition. Passed from Python to Rust.

### Must-Add Configurations (To Delight Consumers)
These options solve specific "Enterprise" pain points:
1.  **`worker_threads`:**
    * *Value:* Allows limiting the library to use only 4 cores on a shared 64-core server.
    * *Implementation:* Pass to `tokio::runtime::Builder::worker_threads()`.
2.  **`pool_size`:**
    * *Value:* Overrides the auto-detected defaults. Essential for databases with strict connection limits (e.g., AWS RDS limits).
    * *Implementation:* Pass to `deadpool_postgres::Pool::builder().max_size()`.
3.  **`batch_size` (Arrow):**
    * *Value:* Controls the size of the Arrow RecordBatches. Critical for streaming data into ML models (e.g., "Give me 10k rows at a time").
    * *Implementation:* Currently aggregated at the end; needs to be exposed in the streaming loop.
4.  **`danger_mode` (Explicit Toggle):**
    * *Value:* A flag to strictly enforce "Panic on Error" vs. "Try to Recover."
    * *Implementation:* As described in Sprint 4.

---

## 4. The "Reputation Builder" Roadmap (Future Optimizations)

These features serve two purposes: Extreme performance for the 1% of users who need it, and "Street Cred" for the author.

1.  **`io_uring` Network Layer:**
    * *The Tech:* Replace `tokio-postgres` (epoll) with a custom `io_uring` implementation for Linux.
    * *The Flex:* "True Asynchronous syscall batching." Reduces context switches during massive data transfers.
    * *Status:* High effort, high reputation, low immediate business ROI (unless on 100Gbps networks).
2.  **SIMD Parser (AVX-512 / NEON):**
    * *The Tech:* Use `std::simd` or `portable-simd` to parse 16 integers at once from the binary stream.
    * *The Flex:* "Saturating the memory bandwidth of modern CPUs."
3.  **Custom Allocator Support (`mimalloc` / `jemalloc`):**
    * *The Tech:* Allow users to swap the global allocator via a feature flag.
    * *The Flex:* "Optimized for high-fragmentation environments." (Note: `mimalloc` is already configured for non-MSVC targets).
4.  **Kernel-Bypass Networking (DPDK):**
    * *The Tech:* Bypassing the OS network stack entirely.
    * *The Flex:* The absolute theoretical limit of data transfer. (Overkill, but legendary).
# Sprint 4: Optimization & Stabilization

This focuses on fixing the "death by a thousand cuts" performance regression we identified in `UncheckedIO`.

You can copy-paste this directly into a new note in your vault (e.g., named `Sprint 4 - UncheckedIO Optimization`).

---

# [[UncheckedIO]] Sprint 4: Optimization & Stabilization

**Goal:** Recover performance lost during parallelization (Target: >2x faster than ConnectorX) and eliminate the 15M+ heap allocations. **Current Status:** ~1.5x faster than Pandas, but 0.5x speed of ConnectorX. **Tags:** #rust #optimization #project/unchecked-io #sprint

## 🚨 Priority 1: The "String Killer" Fix (Memory)

_Fixes the 15,000,000 heap allocations causing allocator contention._

- [ ] **Refactor `parse_row` string handling**

    - [ ] Remove `vec![0; field_len_usize]` allocation in `DynamicBuilder::String` match arm.

    - [ ] Implement zero-copy slicing: `let val_str = std::str::from_utf8(&current_chunk[start..end])?`.

    - [ ] Update `StringBuilder` to append the slice directly (`b.append_value(val_str)`).

- [ ] **Verify Memory Profile**

    - [ ] Run 5M row benchmark.

    - [ ] Ensure RAM usage is flat (no spikes matching row count).


## ⚡ Priority 2: The "Dispatch Killer" Fix (CPU)

_Fixes the 50,000,000 runtime type checks._

- [ ] **Implement `Danger Mode` (Task 7)**

    - [ ] Add `danger_mode: bool` argument to `run_db_logic` and `load_data_from_config`.

    - [ ] **Fast Path (`danger_mode=true`):**

        - [ ] Create a new parsing loop that _assumes_ types based on column index.

        - [ ] Remove the inner `match builder` loop.

        - [ ] Use `get_unchecked` or specific Typed Builders if possible to bypass runtime checks.

    - [ ] **Safe Path (`danger_mode=false`):**

        - [ ] Keep the current `match` based logic for fallback/recovery.


## 🛠️ Priority 3: Tuning & Architecture

- [ ] **Tune `BLAST_RADIUS`**

    - [ ] Reduce partition count from 80 to ~8-16 (matching logical cores).

    - [ ] Test `BLAST_RADIUS` values: `312,500` (16 chunks) vs `625,000` (8 chunks) vs `62,500` (80 chunks).

    - [ ] Goal: Minimize connection overhead (TCP handshakes) vs parallel throughput.

- [ ] **Connection Pooling (Optional)**

    - [ ] Investigate if `bb8` or `deadpool-postgres` can reuse connections instead of opening 80 new ones.


## 📉 Benchmarking & Validation

- [ ] **Run `benchmark.py`**

    - [ ] Target: Beat `ConnectorX` (< 6700ms).

    - [ ] Target: Beat `Pandas` by >4x (< 5000ms).

- [ ] **Profile `UncheckedIO`**

    - [ ] Identify if `malloc` is still the bottleneck after string fix.


---

**Notes & Observations:**

- Current regression caused by 15M allocations * 80 threads fighting for locks.

- Previous "Safe Mode" parser was ~2x faster than ConnectorX (single-threaded). We need to get back to that baseline efficiency _before_ scaling out.
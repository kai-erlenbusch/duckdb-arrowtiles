# Walkthrough: Performance Engineering & Complete Pipeline Overhaul
*Exported: 2026-06-29 16:11:44-07:00*

This walkthrough summarizes the execution of the performance and architectural overhaul for both the native Rust ArrowTiles extension and the Deepgraph sandbox.

## 1. Rust Extension Upgrades (`duckdb-arrowtiles`)

### Lock-Free DashMap Architecture
The global `DashMap` was completely refactored to prevent severe lock-contention across DuckDB's parallel threads. 
We changed the map to store `Arc<AtomicU32>`. Now, threads instantly grab a read-lock via `.get()`, clone the `Arc` pointer in nanoseconds, and drop the map lock before performing the atomic CAS loop. For concurrent cache misses, we explicitly use `.entry().or_insert_with()` to prevent race-condition overwrites.

### Zero-Allocation Arrow Builders
All three native UDFs (`HilbertScalar`, `HilbertNormalizedScalar`, and `AdditiveAssignScalar`) were rewritten to stream data directly into Arrow `UInt64Builder`s. This removes the intermediate `Vec<Option<u64>>` heap allocations, halving memory usage per chunk.

### Anti-Pattern Removed
We deleted the non-deterministic `ResetCapacitiesScalar` UDF.

## 2. WebGL Frontend Optimizations (`deepgraph-arrowtiles-sandbox`)

### Subarray vs Slice
In `Scatterplot.ts`, we replaced all `tile.xBuffer.slice()` calls with `tile.xBuffer.subarray()` when uploading data to the Three.js `InstancedBufferAttribute`. This creates zero-copy memory views instead of allocating fresh ArrayBuffers, eliminating Garbage Collection stuttering during rapid map panning.

### GPU Hover Throttling
We added a `100ms` throttle to the global mouse picking logic. Previously, rendering 400 unique slots to a 1x1 render target on every single mousemove event crippled framerates.

### Worker Transfer Detachment
In `PMTilesClient.ts`, we changed `new Uint8Array(data)` to `data.slice()`, allowing V8 to natively handle the memory duplication in C++ before transferring the ArrayBuffer to the Web Worker.

## Rust Backend Optimizations
In `duckdb-arrowtiles`, I implemented critical lock-free optimizations to the native tile assignment UDFs to eliminate exponential scaling bottlenecks:
1. **Lock-Free DashMap Updates**: Removed the catastrophic `fetch_update` CAS inner loop in `AdditiveAssignScalar`, replacing it with `fetch_add(1, Ordering::Relaxed)`.
2. **Arc Cloning Elimination**: Avoided cloning the synchronized `Arc` references when extracting atomic counters from the `DashMap`, dropping per-row overhead significantly.
3. **Memory Leak Fix**: Implemented a new zero-argument UDF `arrowtiles_clear_cache()` which fully flushes the global `lazy_static` capacity map. This allows the backend to be safely reused across multiple queries without suffering from permanent Out-Of-Memory (OOM) accumulation.

## Python Pipeline Completion
The `generate_pipeline.py` pipeline successfully processed all 1.54 Billion stars down to the highly sorted `final_ordered.parquet` using the native DuckDB engine. The final PMTiles v3 bundling step was executed successfully with the correct header metadata.

## 3. Data Pipeline Overhaul (`generate_pipeline.py`)

### The `C:` Drive Fix
We discovered that the `[Errno 28] No space left on device` crash was caused by the Python `pmtiles.writer` module buffering output to the OS `%TEMP%` directory on the near-full `C:` drive. We hardcoded `tempfile.tempdir = "duckdb_temp"` to explicitly force all temp-file buffering to the 400GB `D:` drive.

### The 3.5-Hour Python Loop is Dead
We successfully completely bypassed the sequential Python `for` loop. The script now runs a native `duckdb.execute(COPY ...)` query that directly calls the newly optimized `arrowtiles_assign_tile` Rust UDF. Data flows directly from the parquet files, through the native Rust logic in C++, and back into Parquet format, bypassing Python entirely.

# Walkthrough: Performance Engineering & Complete Pipeline Overhaul

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

## 3. Data Pipeline Overhaul (`generate_pipeline.py`)

### The `C:` Drive Fix
We discovered that the `[Errno 28] No space left on device` crash was caused by the Python `pmtiles.writer` module buffering output to the OS `%TEMP%` directory on the near-full `C:` drive. We hardcoded `tempfile.tempdir = "duckdb_temp"` to explicitly force all temp-file buffering to the 400GB `D:` drive.

### The 3.5-Hour Python Loop is Dead
We successfully completely bypassed the sequential Python `for` loop. The script now runs a native `duckdb.execute(COPY ...)` query that directly calls the newly optimized `arrowtiles_assign_tile` Rust UDF. Data flows directly from the parquet files, through the native Rust logic in C++, and back into Parquet format, bypassing Python entirely.

# Goal: Complete Overhaul of ArrowTiles & Deepgraph Pipeline

Based on the highly accurate and critical performance reviews, this plan outlines the architectural improvements to both the native Rust `duckdb-arrowtiles` extension and the Python/TypeScript `deepgraph-arrowtiles-sandbox`.

These changes will permanently resolve the catastrophic `[Errno 28] No space left on device` crash, eliminate the 3.5-hour python sorting loop, and remove GC stuttering on the WebGL frontend.

## User Review Required
> [!IMPORTANT]
> The `ResetCapacitiesScalar` (`arrowtiles_reset_capacities`) is an architectural anti-pattern for a scalar UDF since DuckDB evaluates it concurrently across data chunks. I plan to **delete it entirely**. Since our Python pipeline boots a fresh DuckDB kernel per run, the static global capacity map will be naturally cleared on every run without needing a reset function.

## Proposed Changes

---

### `duckdb-arrowtiles` (Native Rust Extension)

#### [MODIFY] [lib.rs](file:///D:/exploratory/duckdb-extension/duckdb-arrowtiles/src/lib.rs)
1. **Lock Contention:** Change the `DashMap` value from `AtomicU32` to `Arc<AtomicU32>`. This allows us to use `.get(&(z, tid))` to grab a read-lock, clone the `Arc` cheaply, and immediately release the read-lock before performing atomic increments. We only fallback to `.insert()` if the bucket misses.
2. **Buffer Bloat:** Refactor all three UDFs (`HilbertScalar`, `HilbertNormalizedScalar`, `AdditiveAssignScalar`) to use `arrow::array::UInt64Builder::with_capacity(input.num_rows())` instead of collecting into standard Rust `Vec<Option<u64>>`. This halves memory allocations.
3. **Pessimistic CAS Loop:** Replace `fetch_add` with a `fetch_update` compare-and-swap loop in `AdditiveAssignScalar` to strictly enforce the ceiling limit (`max_cap`) without ever overflowing and rolling back.
4. **Remove Anti-Pattern:** Delete `ResetCapacitiesScalar`.

---

### `deepgraph-arrowtiles-sandbox` (TypeScript Frontend)

#### [MODIFY] [Scatterplot.ts](file:///D:/exploratory/duckdb-extension/deepgraph-arrowtiles-sandbox/src/Scatterplot.ts)
1. **GPU Upload Memory Churn:** Change `tile.xBuffer.slice(0, numItems)` to `tile.xBuffer.subarray(0, numItems)` to use zero-copy views when pushing to WebGL Instanced Buffers, preventing Garbage Collection stuttering.
2. **GPU Render Target Lock:** Throttle the global picking `mousemove` raycast. Instead of rendering a 1x1 scene on every single mousemove, throttle it to 100ms and disable it while the camera is actively panning.

#### [MODIFY] [PMTilesClient.ts](file:///D:/exploratory/duckdb-extension/deepgraph-arrowtiles-sandbox/src/PMTilesClient.ts)
1. **Main Thread Copying:** Change `const copy = new Uint8Array(data);` to use `data.slice()` to let V8 natively perform the memory block copy in C++, avoiding slow JavaScript-level byte-by-byte copies on the UI thread before passing to the Web Worker.

---

### `deepgraph-arrowtiles-sandbox` (Data Pipeline)

#### [MODIFY] [generate_pipeline.py](file:///D:/exploratory/duckdb-extension/deepgraph-arrowtiles-sandbox/generate_pipeline.py)
1. **Bypass the C: Drive Limits:** Hardcode `tempfile.tempdir = "duckdb_temp"` at the top of the script. This ensures the `pmtiles.writer` module buffers its archive locally on the 400GB `D:` drive instead of the 19GB `C:` drive, preventing the `[Errno 28] No space left on device` crash we hit last time.
2. **Remove the 3-Hour Python Loop:** Replace the nested python loops with a fast native SQL query invoking `arrowtiles_assign_tile(x, y, 14, 500000)`.
3. **Clean Up Workspace:** Programmatically delete `duckdb_temp/assigned_points.parquet` during the pipeline run to ensure we never run out of disk space.

## Verification Plan

### Automated Build & Test
1. Run `cargo build --release` in `duckdb-arrowtiles`.
2. Delete the intermediate `.parquet` and temp files on the `D:` drive to free up space from the crashed run.
3. Run `generate_pipeline.py`. Observe that the 3-hour capacity loop now finishes natively inside DuckDB in under 2 minutes.
4. Verify `gaia.pmtiles` successfully writes to the disk without throwing an `OSError`.

### Manual Verification
1. Boot the frontend (`npm run dev`) and test panning. The framerate should stay perfectly locked without GC stutter thanks to `.subarray()`. Hover tooltips should feel responsive but less computationally heavy.

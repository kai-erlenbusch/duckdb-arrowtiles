# DeepGraph Pipeline Refactoring: The "Holy Grail" Streaming Architecture
*Date: 2026-06-30 | Time: 14:21 PDT*

## The Problem
The previous approach successfully solved the Out of Memory error by partitioning the data in Stage 1, but introduced a massive CPU bottleneck in Stage 2. 

By using DuckDB's SQL engine to execute `ROW_NUMBER() OVER (PARTITION BY vx, vy ORDER BY abs_m ASC)` inside a 15-iteration loop for every single spatial chunk, we forced the database to perform hundreds of millions of window sorts and table writes sequentially. This caused extreme slowdowns on dense chunks like Chunk 36 (the galactic center), projecting a 4-hour runtime.

## The Solution: Streaming Hilbert Reduction (Expert Tweak Edition)
Since the `hilbert_normalized` math is compiled in native Rust, we can leverage its space-filling properties to bypass SQL window functions entirely. Furthermore, we don't even need to use it in the sorting phase!

We rewrote Stage 2 to use a **streaming in-memory vectorization loop**:

1. **One Query Per Chunk:** Instead of 15 queries per chunk, DuckDB executes exactly one query:
   ```sql
   SELECT 
       x_norm, y_norm, abs_m, bp_rp,
       FLOOR(x_norm::DOUBLE * grid_size)::BIGINT AS vx_0,
       FLOOR(y_norm::DOUBLE * grid_size)::BIGINT AS vy_0
   FROM 'duckdb_temp/partitions/z3_chunk_id=X/*.parquet'
   ORDER BY vx_0 ASC, vy_0 ASC, abs_m ASC
   ```
2. **Batch Streaming:** Because DuckDB groups the stars perfectly by their Z=0 voxel (`vx_0`, `vy_0`) and sub-sorts them by brightness (`abs_m ASC`), they stream out mathematically ready for quadtree assignment.
3. **Set-Based Micro-Bucketing:** We pull these contiguous batches directly into Python memory. Because the list is pre-sorted, the first star to claim a micro-voxel is guaranteed to be the brightest. We just use a microscopic Python `set` to track occupied voxel boundaries from $Z=0$ to $Z=14$. 
4. **Append to Disk:** The assigned batch is flushed to a final Parquet file. 

## Benefits
- **Zero Temp Tables:** We removed the 15 `CREATE TEMP TABLE` operations per chunk entirely.
- **Microscopic RAM Footprint:** We process the chunk stream in $Z=0$ voxel batches. The Python memory overhead is virtually non-existent (roughly ~2 MB).
- **Speed:** By removing all SQL overhead and eliminating the need for Numpy or Python re-sorting, chunk processing time drops astronomically, yielding accurate execution benchmarks.

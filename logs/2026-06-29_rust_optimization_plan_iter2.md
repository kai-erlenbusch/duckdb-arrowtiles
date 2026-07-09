# Rust Extension Optimization Plan (Iteration 2)
*Generated on: 2026-06-29 16:57:48-07:00*

This plan addresses the critical points raised in the **ArrowTiles Code Review (Iteration 2)** for `duckdb-arrowtiles/src/lib.rs`. 

## User Review Required

> [!IMPORTANT]
> **Deprecating Dead Code vs Fixing It**
> The code review correctly identifies a massive lock contention issue in `AdditiveAssignScalar`. However, because we just pivoted to pure SQL Geometric Voxel Bucketing in `generate_pipeline.py`, we no longer use `AdditiveAssignScalar` or `ClearCacheScalar`. 
> 
> Instead of fixing the locks in `AdditiveAssignScalar`, **I propose we delete it entirely**, along with `ClearCacheScalar` and the global `DashMap`. This completely eliminates all lock contention and memory leaks by design. Do you agree with this deletion?

## Proposed Changes

Even though we are deleting the stateful assignment UDF, the performance optimizations in the review (Items 2, 3, and 4) are highly applicable to the UDFs we *are* still using (`HilbertNormalizedScalar` and `HilbertScalar`).

### `duckdb-arrowtiles/src/lib.rs`

#### [MODIFY] Optimizing Arrow Iteration (Raw Slices)
The review rightly points out that `.iter()` on Arrow arrays involves Option branching on every single row. I will update `HilbertNormalizedScalar` and `HilbertScalar` to use raw slices:
1. Check `if input.null_count() == 0`.
2. If true, extract the raw slice via `.values()`.
3. Loop over the raw slice, which allows LLVM to auto-vectorize the operations because the branch logic is removed.

#### [MODIFY] Constant Extraction
In `generate_pipeline.py`, we pass a constant zoom level to the UDF: `hilbert_normalized(x, y, {z}::UTINYINT)`. Currently, the Rust code iterates over the `zoom_array` row-by-row. I will extract the first value of `zoom_array` outside the loop, saving memory bandwidth since it is a constant for the entire chunk.

#### [DELETE] Dead Code
I will remove:
- `AdditiveAssignScalar` struct and implementation.
- `ClearCacheScalar` struct and implementation.
- The global `lazy_static! CAPACITIES` DashMap.

## Verification Plan

### Automated Tests
- I will run `cargo build --release` in the `duckdb-arrowtiles` directory to verify the UDF compiles successfully with the raw slice optimizations.

### Performance Impact
By switching to raw slices (`.values()`) and removing the Option branches, the `hilbert_normalized` evaluation (which executes 1.7 Billion times in our new pipeline) will benefit from LLVM SIMD auto-vectorization, significantly reducing DuckDB query time.

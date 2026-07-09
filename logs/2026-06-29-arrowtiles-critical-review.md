# Critical Code & Performance Review: ArrowTiles
_Date: June 29, 2026 13:38 PM_

Based on a thorough review of the `duckdb-arrowtiles` repository, here is an evaluation focusing on code quality, architecture, and performance engineering.

> [!WARNING]
> **Missing Core Implementation**
> The `README.md` and `test_load.py` heavily reference features like the `arrowtiles_export` Table Function, PMTiles archive generation, zero-copy Arrow IPC streaming, and background thread channel architectures. 
> However, **`src/lib.rs` only contains the four Scalar UDFs** (`hilbert_xy`, `hilbert_normalized`, `arrowtiles_assign_tile`, `arrowtiles_reset_capacities`). The core functionality that makes this a "PMTiles for scatterplots" competitor to Entwine is currently absent from the source tree.

---

## 🏎️ Performance Engineering Findings

If ArrowTiles is meant to process massive spatial datasets natively and out-perform Python, resolving the following performance bottlenecks is critical.

### 1. Severe Lock Contention in `AdditiveAssignScalar`
In `AdditiveAssignScalar::invoke`, a global `DashMap` is used to track capacities:
```rust
let counter_ref = CAPACITIES.entry((z, tid)).or_insert_with(|| AtomicU32::new(0));
```

> [!CAUTION]
> Calling `DashMap::entry()` acquires a **write lock** on the corresponding map shard. 
> Because DuckDB evaluates UDFs in parallel across all CPU cores on chunks of vectors, calling `.entry()` sequentially per row across dozens of threads will cause catastrophic lock contention. Your CPU cores will spend all their time waiting on locks rather than computing Hilbert curves.

**Recommendation:**
Optimize the lock pattern. Most of the time, the entry already exists. You should attempt a read-only `.get()` first, and only fallback to `.entry()` if it doesn't exist:
```rust
let counter_ref = if let Some(r) = CAPACITIES.get(&(z, tid)) {
    r.clone() // DashMap Ref clones are cheap
} else {
    CAPACITIES.entry((z, tid)).or_insert_with(|| AtomicU32::new(0)).downgrade()
};
```

### 2. Intermediate Memory Allocations (Buffer Bloat)
In all UDFs, you are collecting results into a standard Rust `Vec` before converting to an Arrow Array:
```rust
let tile_ids: Vec<Option<u64>> = x_iter.zip(y_iter)...collect();
Ok(Arc::new(UInt64Array::from(tile_ids)))
```

> [!TIP]
> This allocates an intermediate `Vec` on the heap, fills it, and then Arrow allocates *another* buffer to copy the data into its IPC-compatible format. This doubles memory allocation overhead per chunk.

**Recommendation:**
Use Arrow's `UInt64Builder` to write directly to the native Arrow memory buffer:
```rust
use arrow::array::UInt64Builder;

let mut builder = UInt64Builder::with_capacity(input.num_rows());
for ((x_opt, y_opt), zoom_opt) in x_iter.zip(y_iter).zip(zoom_iter) {
    // ... calculate ...
    match result {
        Some(val) => builder.append_value(val),
        None => builder.append_null(),
    }
}
Ok(Arc::new(builder.finish()))
```

### 3. Missed Vectorization Opportunities
Arrow is designed for columnar, SIMD-accelerated processing. The current implementation unpacks the columns and iterates row-by-row, doing heavy scalar math (`tan()`, `asinh()`, `floor()`) on every single iteration.

**Recommendation:**
While the `fast_hilbert::xy2h` logic requires row-by-row execution, the coordinate transformations (clamping, projecting to Web Mercator) can be performed using `arrow::compute` math kernels before iterating. This pushes the mathematical heavy lifting down to optimized SIMD C++ routines.

---

## 🏗️ Architectural & Code Quality Findings

### 1. Anti-Pattern: Side-Effects in Scalar UDFs
The `ResetCapacitiesScalar` is built as a row-wise Arrow UDF:
```rust
fn invoke(_: &Self::State, input: RecordBatch) -> ... {
    CAPACITIES.clear();
    // ... loop over rows to return NULLs
}
```

> [!IMPORTANT]
> Scalar UDFs are evaluated in parallel by DuckDB over data chunks. If a user runs `SELECT arrowtiles_reset_capacities(1) FROM my_table`, DuckDB will concurrently evaluate this function across multiple threads. `CAPACITIES.clear()` will be called repeatedly and non-deterministically, potentially clearing capacities that another thread *just* incremented. 

**Recommendation:**
Global state resets should not be bound to row-level execution. This should either be exposed as a **Pragma**, a **Table Function** with no inputs that runs exactly once, or entirely managed by the lifecycle of the query rather than static global memory.

### 2. Compare-and-Swap (CAS) Logic Improvements
The atomic increment logic in `AdditiveAssignScalar` works but is slightly pessimistic:
```rust
let current = counter_ref.load(Ordering::Relaxed);
if current < max_cap {
    let old = counter_ref.fetch_add(1, Ordering::Relaxed);
    if old >= max_cap {
        counter_ref.fetch_sub(1, Ordering::Relaxed); // rollback
    }
}
```
If threads are highly contentious at the capacity limit, multiple threads might push the counter far beyond `max_cap` before rolling back. Consider using a `fetch_update` loop for strict ceiling enforcement without rollback states.

## Summary Conclusion
The foundational approach of mapping Arrow UDFs natively inside DuckDB to bypass Python overhead is excellent. However, to truly serve as a high-performance successor to Entwine, the threading model around `DashMap` must be re-engineered, and memory buffers should be constructed directly via Arrow Builders. Finally, the repository needs the actual PMTiles writer implementation to be pushed so the core export mechanism can be evaluated!

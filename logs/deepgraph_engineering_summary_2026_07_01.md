# DeepGraph ArrowTiles: Engineering Summary

**Date:** July 1, 2026
**Subject:** Pipeline Reliability, Spatial Density Preservation, and WebGPU Optimization

This document summarizes the engineering efforts undertaken to stabilize the DeepGraph ArrowTiles pipeline (the successor to Deepscatter/Quadfeather) and optimize its WebGPU renderer for a seamless, 60 FPS experience when visualizing 1.8 billion stars from the Gaia dataset.

## 1. Pipeline Reliability & Performance (Stage 2)

### The Challenge
The original Python-based pipeline for "Stage 2: Voxel Bucketing" was highly unstable. It relied on keeping large structures in memory and struggled to efficiently process the 64 partitioned macro-chunks generated in Stage 1. It frequently froze, hit memory limits, or failed to clean up temporary files correctly.

### The Solution
We replaced the Python loops with a dedicated, high-performance Rust CLI tool (`arrowtiles_bucketer.rs`). 
*   **Memory-Mapped I/O:** The Rust tool reads the input parquet chunks and streams the output directly into Arrow IPC files.
*   **Parallelization:** Leveraging the `rayon` crate, it processes millions of rows per second across all CPU cores.
*   **Integration:** `generate_pipeline.py` was updated to orchestrate this Rust binary, resulting in a highly robust pipeline that can process a 24.5 GB dataset and yield a perfectly formed 17.9 GB `gaia.pmtiles` archive in about an hour without freezing.

## 2. Resolving the "Patchy" Checkerboard Seams

### The Challenge
Upon successfully rendering the tiles, the visualization exhibited harsh, square seams between tiles. The galactic core looked completely disconnected from the sparser regions of the sky.

### The Solution
We realized this was a fundamental divergence from how Ben Schmidt's original Deepscatter handled Level of Detail (LOD). 
*   Our WebGPU frontend was enforcing a hard limit of `rowsPerTile = 100000`.
*   Because the tiles were generated using a spatial Voxel Grid (which allowed up to 262,144 stars per tile), the dense galactic core easily hit the 100,000 limit, forcing the renderer to discard its faint stars.
*   Meanwhile, the sparse empty sky tiles *didn't* hit the limit, meaning they successfully rendered their faint stars. 
*   When placed side-by-side, the sparse tiles had a dense background of faint noise, while the dense tiles did not, creating a stark visual seam.

We fixed this by **removing the frontend truncation limit**, increasing it to `262144`. This allowed the continuous spatial Voxel Grid to seamlessly span across tile boundaries, eliminating the artificial checkerboard pattern.

## 3. WebGPU Overdraw & FPS Restoration

### The Challenge
By removing the truncation limit, the engine was allowed to render the full Voxel Grid. However, due to a bug in the Quadtree traversal logic (`Math.max(3, ...)`), the engine was forcing the load of all 64 Z=3 tiles even when fully zoomed out. 
This resulted in **17.51 million points** being rendered simultaneously. Because the points use additive blending without depth testing, the massive overdraw caused the GPU fragment shader to choke, plummeting the framerate to **6 FPS**.

### The Solution
We implemented **Global Magnitude Culling** directly in the WebGPU Node material, solving the overdraw without re-introducing tile seams.
*   The shader now maps the camera's zoom level to a global magnitude cutoff (`maxMagUniform`).
*   At Zoom 0, the GPU physically discards any star fainter than Magnitude 14. 
*   Because this cutoff is based on a *global physical property* rather than a local row index, it perfectly preserves the natural density gradient of the galaxy. Both the dense core and the empty sky are subjected to the exact same brightness threshold.
*   Combined with fixing the quadtree traversal (allowing Z=1 and Z=2 to render natively at low zoom) and subtly shrinking the point sizes to save fill rate, the GPU workload dropped drastically, restoring a silky smooth 60 FPS.

## 4. Network Connection Exhaustion

### The Challenge
At intermediate zoom levels, the `PMTilesClient` was aggressively pre-fetching tiles multiple levels deep into the quadtree. This resulted in over 200 concurrent HTTP range requests being fired at once, exhausting the browser's connection pool and throwing `net::ERR_INSUFFICIENT_RESOURCES` errors, which stalled the entire visualization.

### The Solution
We implemented a strict depth-capping logic based on the current zoom level (`overfetch`). By dynamically tuning how deep the quadtree is allowed to fetch, we ensured that the active tile queue never exceeds ~85 tiles at any given time. This keeps the network queue healthy and ensures tiles load consistently without stalling the browser.

## Conclusion

The DeepGraph ArrowTiles engine is now a robust, native successor to Deepscatter. It leverages the raw I/O performance of DuckDB and Rust for data generation, and the modern power of WebGPU and Arrow IPC for zero-copy, 60 FPS streaming in the browser.

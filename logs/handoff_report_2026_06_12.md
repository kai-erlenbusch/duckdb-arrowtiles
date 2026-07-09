# Deepgraph ArrowTiles Sandbox - Handoff Report
**Date:** June 12, 2026

## 1. Executive Summary & Work Completed
This phase of the sandbox focused on migrating the WebGPU visualization engine away from a fragmented S3 bucket of thousands of `.feather` files to a unified, high-performance architecture using **DuckDB**, **PMTiles**, and **Apache Arrow IPC**. 

### Key Accomplishments:
* **DuckDB Pipeline (`generate_pipeline.py`)**: Built an automated out-of-core pipeline capable of projecting and sorting 175M stars (Steve Berardi's Gaia DR3 dataset) globally by magnitude (brightness) and crushing them into a single `muni_health.pmtiles` archive in under 80 seconds.
* **PMTiles + Arrow IPC Client**: Rewrote the frontend data fetcher (`PMTilesClient.ts`) to execute fast HTTP Range Requests against the `.pmtiles` archive and directly map the binary Arrow IPC chunks into zero-copy GPU `Float32Arrays`.
* **Astronomical Spatial Correction**: Fixed the Hammer projection math to perfectly map the Milky Way galaxy's true astronomical coordinates, bringing landmarks like the Large and Small Magellanic Clouds to their correct locations.
* **Density Culling (`ix` column)**: Upgraded the WebGPU Vertex Shader to dynamically drop faint background stars based on a zooming density threshold (`maxIxUniform`), preserving framerates and preventing GPU starvation.
* **Photorealistic Additive Blending**: Dropped base vertex opacity to an extreme sub-pixel low (`0.005`) to naturally diffuse high-density equatorial star clusters, melting away artificial quadtree block boundaries and restoring photorealistic dust-cloud rendering.
* **Repository Health**: Cleaned up the local git state, built a robust `.gitignore` to prevent gigabyte-sized binary files from stalling version control, and established the foundation for the GitHub repository.

---

## 2. Project Strengths

* **WebGPU Instanced Performance**: By bypassing the CPU completely and mapping Arrow buffers directly to GPU memory, the engine easily hits stable 60 FPS while rendering millions of distinct vertices.
* **Zero-Copy Architecture**: The data remains in a pure columnar format from the moment it leaves the DuckDB pipeline until it enters the GPU silicon. There is no slow JSON parsing or JavaScript object instantiation in the critical path.
* **Deployment Simplicity**: The entire 175M point dataset is now encapsulated in a single `.pmtiles` file. This means the engine can be deployed on standard CDNs without complex database servers or S3 permission headaches.
* **DuckDB Speed**: Relying on DuckDB for the data preparation allows us to crunch datasets larger than system RAM in under two minutes, enabling incredibly fast iteration cycles.

---

## 3. Current Weaknesses & Architectural Limitations

* **Naive Global Quadtree Partitioning (The "Flares" Issue)**: Currently, the pipeline assigns stars to zoom levels based on a *global* magnitude threshold. Because the Milky Way is heavily clustered along its equator, the tiles that cover the equator receive disproportionately massive star payloads. This creates unnatural, sharp density cutoffs (rectangular blocks or "flares") along physical tile boundaries. We are currently "hiding" this flaw using extreme low-opacity diffusion.
* **Buffer Reallocation Stutter**: The WebGPU geometry dynamically re-allocates its internal buffer arrays to match the unpredictable point count of incoming tiles. Rapid zooming triggers heavy Garbage Collection (GC) pressure in V8, causing noticeable micro-stutters.
* **Additive Popping**: When deep-zoom tiles finish downloading, millions of new stars appear instantly at 100% of their calculated opacity, resulting in a distracting "pop" rather than a smooth fade-in.
* **Browser HTTP Throttling**: Because the `PMTiles` client needs to fetch 16+ tiles simultaneously during deep zooms, it hits the browser's maximum concurrent connection limit (usually 6 requests per domain), creating an artificial network queue bottleneck.

---

## 4. Next Steps & Recommended Path Forward

1. **Implement True Recursive Push-Down (Highest Priority)**:
   * To fix the density cutoffs, the DuckDB `arrowtiles_export` extension (or the python pipeline) must be refactored to enforce a strict local point-limit per tile (e.g., 50,000 points max). 
   * The builder must use a "greedy" algorithm: Fill the parent tile with its 50,000 brightest stars, then dynamically push all remaining overflow stars to its child tiles. This guarantees perfectly uniform visual density across the quadtree, regardless of the physical clustering of the galaxy.
2. **Fixed-Size GPU Ring Buffers**:
   * Pre-allocate a massive WebGPU buffer pool during initialization (e.g., exactly 800 slots of exactly 100,000 floats). When tiles load, overwrite the data in the existing slots rather than calling `new Float32Array()`. This will completely eliminate GC stutters.
3. **Temporal Anti-Aliasing (Fade-ins)**:
   * Add a `spawnTime` timestamp to incoming tiles in the `PMTilesClient`. In the WebGPU fragment shader, calculate the `age` of the vertex relative to the system clock and gracefully fade its alpha from `0.0` to its target opacity over 300 milliseconds.
4. **Scale to Sam Fatnassi (1.8 Billion)**:
   * Once the recursive quadtree builder is implemented and proven on the Steve Berardi subset, run the pipeline on the full 650GB Sam Fatnassi Gaia DR3 dataset to achieve parity with the world's best data visualizations.

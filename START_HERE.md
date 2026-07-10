# START_HERE: The DeepGraph Philosophy (Architecture & Philosophy) 🌌

**ATTENTION AI AGENTS AND DEVELOPERS:** If you are new to this repository, read this document first. It contains the core philosophy, history, and architectural decisions that define this project. Understanding this will save you hours of context-gathering.

---

## 1. The Lineage & Inspiration
This ecosystem is a direct successor to the groundbreaking work by Ben Schmidt (Nomic AI). To understand what we are building, you must understand what came before it:

- **[Deepscatter](https://github.com/nomic-ai/deepscatter/)**: A custom WebGL renderer that loads tiles on-demand, using instanced geometry and additive blending to visualize billions of points.
- **[Quadfeather](https://github.com/bmschmidt/quadfeather/)**: The C++ pipeline that partitioned giant CSV/Parquet datasets into a spatial quadtree, saving them as thousands of tiny Apache Arrow `.feather` files for Deepscatter to read.
- **[The Gaia Sandbox](https://benschmidt.org/gaia/gaia.html)**: Ben Schmidt's stress test rendering 1.8 billion stars from the ESA Gaia catalog. It is the benchmark we are measuring ourselves against.

Deepscatter was revolutionary because it solved a massive problem in data visualization: **How do you render over a billion points in a web browser?** Traditional WebGL engines choke on a few hundred thousand points. Deepscatter and Quadfeather solved this by streaming quadtrees of Arrow buffers directly to the GPU.

---

## 2. Why We Are Building The Successor
While Deepscatter is brilliant, it has severe infrastructural and architectural limitations that prevent it from being a ubiquitous standard:

### The "File Count" Problem
Quadfeather outputs a full quadtree as **thousands (sometimes hundreds of thousands) of individual `.feather` files**. 
- Uploading 100,000 files to an AWS S3 bucket is incredibly slow and expensive.
- The browser must fire thousands of individual HTTP `GET` requests, which easily exhausts browser connection limits and causes stalling.
- Managing, moving, or sharing the dataset requires dealing with a massive directory tree.

### The "Builder" Problem
Quadfeather is a complex C++ tool that can be difficult to compile, extend, or integrate into modern data engineering pipelines (which primarily run on Python, Rust, and SQL).

---

## 3. Our Solution: DeepGraph + ArrowTiles
To solve these limitations, we built a modern ecosystem with a direct 1-to-1 lineage to its predecessors:

- **DeepGraph**: The WebGPU successor to the *Deepscatter* frontend.
- **ArrowTiles**: The DuckDB/Rust successor to the *Quadfeather* data pipeline.
- **Sandbox**: The exact repository you are in right now—our playground to stress test DeepGraph and ArrowTiles together, exactly the same way Nomic AI did with their Gaia dataset.

The core concept is **"PMTiles for Scatterplots"**.

Instead of writing thousands of files, we write a **single, unified `.arrowtiles` archive**. 

### The Backend: DuckDB + Rust
We leverage **DuckDB** for out-of-core data processing and a custom **Rust** CLI tool (`arrowtiles_bucketer`) for spatial partitioning. 
1. **Global Sorting:** DuckDB reads the raw Parquet files and performs a global sort by magnitude (brightness), ensuring that the most significant points across the *entire dataset* are prioritized.
2. **Spatial Voxel Bucketing:** The Rust tool reads the data streams and bins the points into a spatial quadtree, keeping them strictly ordered by brightness.
3. **Single Archive Packing:** The quadtree chunks are written as pure Apache Arrow IPC binaries and packed into a single `.arrowtiles` file.

### The Frontend: WebGPU & HTTP Range Requests
Instead of WebGL, we built a modern **WebGPU** engine using Three.js.
- **Range Requests:** The `PMTilesClient` parses the `.arrowtiles` directory structure and uses **HTTP Range Requests** to fetch only the exact byte-ranges of the Arrow IPC chunks it needs directly from the single file. This is highly efficient and CDN-friendly.
- **Zero-Copy Arrays & Multithreading:** The Apache Arrow IPC chunks are decompressed (Zstd) and parsed natively into `Float32Arrays` using a multi-threaded Round-Robin Web Worker pool. These TypedArrays are then passed directly to the GPU buffers with zero overhead, keeping the Main UI thread completely unblocked.

---

## 4. The "Seam" Problem & Global Magnitude Culling (LOD)
One of the hardest challenges in building an out-of-core renderer is **Level of Detail (LOD)**. 

### The Pitfall of Local Capping
Initially, we limited each tile to a maximum of 100,000 points on the frontend to prevent GPU crashes. This caused a fatal visual artifact: **Checkerboard Seams**. 
Because the dense galactic core had millions of points, the 100,000 limit chopped off all its faint stars. Meanwhile, the empty sky *didn't* hit the limit, meaning it rendered all its faint stars. When placed side-by-side, the empty sky had a thick background of "faint noise" while the dense core did not, creating a harsh square boundary.

### The Holy Grail: Global Culling
We fixed this by removing the local point limit and implementing **Global Magnitude Culling** in the WebGPU shader:
1. Because the points in the Arrow IPC chunks are strictly sorted by absolute magnitude, the shader has access to their true global brightness.
2. We pass a `maxMagUniform` to the shader based on the camera zoom.
3. When zoomed out, the GPU physically discards any star fainter than a specific global threshold (e.g., Magnitude 14). 
4. Because a magnitude 14 star in the empty sky is treated exactly the same as a magnitude 14 star in the galactic core, the density transitions perfectly smoothly without any tile seams, while simultaneously restoring 60 FPS by eliminating billions of pixel overdraws.

---

## 5. LLM Onboarding & Agent Handoff Instructions
**CRITICAL RULE FOR AI AGENTS:** This ecosystem is split into two physical sibling repositories on the filesystem. Depending on your task, you MUST navigate to the correct directory:

1. **Frontend & Pipeline (`deepgraph` sandbox):**
   - **Repository:** `https://github.com/kai-erlenbusch/deepgraph-arrowtiles-sandbox`
   - **What lives here:** The WebGPU application (`src/`) and all web assets. (Strictly the web client).
   - **Caution (Agent Memory):** The `logs/` directory contains transcripts, implementation plans, research reports, and walkthroughs from past agent sessions. If you need historical context on *why* a decision was made, read the files in `logs/` before asking the user.
   - **Constraint:** Performance over everything. Do not introduce O(N) JavaScript loops over the data arrays on the main thread. Additive blending without depth testing is extremely expensive on fill rate. Keep point quad sizes small at low zooms.

2. **Backend Engine (`arrowtiles`):**
   - **Repository:** `https://github.com/kai-erlenbusch/duckdb-arrowtiles`
   - **What lives here:** The core Rust logic for spatial partitioning (`src/bin/bucketer.rs`), Arrow IPC packing (`src/bin/packer.rs`), and the Python pipeline coordinator (`arrowtiles.py`).
   - **Constraint:** If Stage 2 data processing needs to be modified, do it here. Do not attempt to process 24 GB of data natively in Python loops.

**Handoff Protocol:** If a user asks you to review or modify `deepgraph`, you stay in the sandbox folder. If the user asks you to review or modify `arrowtiles`, you must immediately traverse to the `duckdb-arrowtiles` sibling directory before searching for files or proposing edits.

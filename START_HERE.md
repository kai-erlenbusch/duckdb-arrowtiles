# START_HERE: ArrowTiles Backend Philosophy 🛠️

**ATTENTION AI AGENTS AND DEVELOPERS:** If you are new to this repository, read this document first. It outlines the architectural decisions and history behind the `duckdb-arrowtiles` data processing pipeline.

---

## 1. The Context: Replacing Quadfeather
This project is the backend data pipeline designed to replace **Quadfeather** (a C++ tool by Ben Schmidt). 
Quadfeather was revolutionary for partitioning giant spatial datasets (like the 1.8 billion star ESA Gaia catalog) into a quadtree. However, it had a fatal limitation for modern web deployment: **It output thousands of tiny `.feather` files.**
Uploading, managing, and firing HTTP GET requests for 100,000 individual files completely exhausts browser connection limits and S3 throughput.

## 2. The Solution: PMTiles + Apache Arrow
The goal of this backend toolkit is to implement **"PMTiles for Scatterplots"**. 
Instead of writing thousands of files, we write a **single, unified `.pmtiles` archive**. The PMTiles format is inherently friendly to **HTTP Range Requests**, allowing a frontend client to fetch only the exact byte ranges it needs from the massive archive without downloading the whole file.

Our chunks inside the PMTiles archive are raw **Apache Arrow IPC streams**. When fetched by the frontend, they require zero-copy parsing and can be uploaded directly to WebGPU buffers.

---

## 3. The Architecture Pivot
Early iterations of this project attempted to perform the entire partitioning and packaging pipeline purely inside DuckDB via stateful Table Functions (`arrowtiles_export`). 
**This architecture failed** due to memory bloat, single-threaded bottlenecks during IPC serialization, and poor memory map handling in DuckDB extensions.

We abandoned the stateful DuckDB extension and pivoted to a **Hybrid Toolkit** approach:

### Component A: The Stateless DuckDB Extension
We kept the DuckDB extension (`src/lib.rs`) but stripped it down to just pure, mathematical scalar UDFs (like `hilbert_normalized`). 
This allows us to leverage DuckDB to quickly sort 24+ GB of raw Parquet by global significance (e.g., brightness) and compute basic SIMD math.

### Component B: Standalone Rust CLIs
The heavy lifting of partitioning and packaging was moved out of DuckDB into two standalone Rust binaries:
1. **`arrowtiles_bucketer` (Stage 2):** Reads the DuckDB-sorted Parquet, assigns spatial voxels, resolves the Quadtree Z-levels to prevent visual overcrowding, and outputs a bucketed Parquet file.
2. **`arrowtiles_packer` (Stage 3):** Reads the finalized Parquet, serializes the data into Zstd-compressed Apache Arrow IPC chunks, and writes the final `.pmtiles` archive.

---

## 4. Instructions for Agents
If you are an AI agent tasked with modifying this codebase, keep these constraints in mind:
1. **Do not put state back into DuckDB:** The DuckDB extension must remain a pure, stateless mathematical library. All complex file I/O or stateful aggregations must happen in the standalone binaries (`bucketer.rs` and `packer.rs`).
2. **Performance is Critical:** You are processing gigabytes of data. Use `AHashSet`, avoid `.collect()` on `Option` types in tight loops, and prefer direct `.values()` slice iteration for LLVM auto-vectorization.
3. **Coordination:** The Python orchestrator (`generate_pipeline.py`) lives in the frontend sandbox repository (`deepgraph-arrowtiles-sandbox`). This repository (`duckdb-arrowtiles`) only provides the compiled tools that the Python script calls.

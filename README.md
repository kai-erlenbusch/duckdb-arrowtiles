# ArrowTiles (DuckDB Rust Extension)

[![DuckDB](https://img.shields.io/badge/DuckDB-v1.5.3-orange.svg)](https://duckdb.org)
[![Rust](https://img.shields.io/badge/Rust-1.80+-blue.svg)](https://www.rust-lang.org/)

ArrowTiles is a highly experimental, high-performance **native DuckDB extension** written in Rust. 

The goal of this project is to eliminate Python and standard IPC overhead from spatial tiling workflows. By running directly inside DuckDB's C++ memory space, ArrowTiles allows data scientists to ingest massive Parquet/CSV datasets and natively export them into a single PMTiles archive filled with zero-copy Apache Arrow IPC buffers—all using a single SQL command.

## 🏗️ Architecture

ArrowTiles leverages `duckdb-rs` and `cargo-duckdb-ext-tools` to bridge the gap between DuckDB's internal C++ execution engine and the modern Rust geospatial ecosystem (`arrow-rs` and `pmtiles-rs`).

```mermaid
graph TD
    A[(Massive Dataset\nParquet/CSV)] -->|SQL COPY / SELECT| B(DuckDB C++ Engine)
    
    subgraph "ArrowTiles Extension (Rust cdylib)"
        B -->|VArrowScalar hilbert_xy| UDF[Native Spatial Binning\nlon/lat -> TileID]
        B -->|Multi-threaded ORDER BY TileID| B
        B -->|FFI C-Data Interface| C[DataChunks / Vectors]
        C -->|Zero-Copy / Memory Map| D[Apache Arrow RecordBatches]
        D -->|arrow_ipc::writer| E[Arrow IPC Byte Buffers]
        E -->|pmtiles_writer| G[PMTiles Directory Packaging]
    end
    
    G --> H[(output.pmtiles)]
    
    classDef duckdb fill:#ff9900,stroke:#333,stroke-width:2px;
    classDef rust fill:#000000,stroke:#fff,stroke-width:2px,color:#fff;
    class B duckdb;
    class C,D,E,G,UDF rust;
```

## 🚀 Phases & Roadmap

### ✅ Phase 1: Extension Skeleton & DuckDB Interop (Completed)
- [x] Initialized a standard Cargo `cdylib` library.
- [x] Crossed the C ABI boundary using the `#[duckdb_entrypoint_c_api]` macro to allow DuckDB's C++ engine to natively invoke our Rust code.
- [x] Successfully compiled the `.dll` and injected DuckDB's strict cryptographic metadata footer using `cargo-duckdb-ext-tools`.
- [x] Extension loads flawlessly in DuckDB (`LOAD 'arrowtiles'`) without IPC or Python bridging.

### ✅ Phase 2: Structural Stability & Arrow Serialization (Completed)
- [x] Hooked into DuckDB's internal vectors by creating a custom `TableFunction` (`arrowtiles_export`).
- [x] Implemented a **Thread-Safe Channel Architecture**: Bypassed DuckDB's parallel worker thread constraints by spawning a dedicated background worker communicating with the `VTab` via `mpsc` channels.
- [x] **Zero-Row Safeguard**: Arrow `FileWriter` pre-initializes using schema to safely handle empty queries.
- [x] **Graceful Error Bubbling**: Extracted execution logic to eliminate panics; errors safely traverse the channel to surface natively in DuckDB CLI.
- [x] **Backpressure**: Migrated to `mpsc::sync_channel(1)` to structurally prevent OOM exhaustion.
- [x] Extracted `RecordBatch` streams using DuckDB's optimized Arrow C-Data interface and serialized zero-copy batches into an Apache Arrow IPC `.feather` file.

### ✅ Phase 3: DuckDB Native UDF Pivot (Completed)
- [x] Shifted spatial processing directly into DuckDB's multi-threaded C++ execution engine via the Native Scalar UDF Architecture.
- [x] Registered `hilbert_xy(lon, lat)` natively using `VArrowScalar` (`duckdb::vscalar::arrow::VArrowScalar`).
- [x] Automatically maps function over internal C++ vectors in parallel across all CPU cores.
- [x] The background worker is now a highly optimized, strictly sequential "dumb pipe" that blindly converts pre-sorted Arrow IPC buffers into PMTiles format.

### 🗺️ Phase 4: PMTiles Packaging (Next)
- [ ] Implement spatial quadtree math to determine Z/X/Y bounds for every row.
- [ ] Connect exact `fast_hilbert` or `pmtiles::TileId` math into the `hilbert_xy` UDF.
- [ ] Feed the Arrow IPC buffers into `pmtiles-rs` to construct the PMTiles directory structure.
- [ ] Flush the final `.pmtiles` archive to disk.

## ⚠️ Known Limitations

- **Temporary Tables & Uncommitted Transactions**: Because DuckDB requires Table Functions to be completely thread-safe, `arrowtiles_export` passes your query to an isolated background connection worker. This means the extension cannot currently export `TEMP` tables or data from uncommitted transactions. Please ensure you are querying physical tables or persistent views!
- **Sequential Execution**: The extension utilizes a single background worker to prevent Out-Of-Memory (OOM) crashes when attempting concurrent exports of massive geospatial datasets. Multiple concurrent `arrowtiles_export` calls will be queued and executed sequentially.

## 🛠️ Building & Loading

### Prerequisites
* Rust toolchain (cargo)
* `cargo-duckdb-ext-tools`

```bash
# Install the extension packaging tool
cargo install cargo-duckdb-ext-tools

# Build the extension with the required metadata footer
cargo duckdb-ext build -- --release
```

### Usage

Open your DuckDB CLI or Python environment and load the extension:

```sql
LOAD 'target/release/duckdb_arrowtiles.duckdb_extension';
```

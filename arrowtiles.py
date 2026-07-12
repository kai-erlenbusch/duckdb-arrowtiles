import duckdb
import pyarrow as pa

import os
import time
import math
import argparse
from tqdm import tqdm

class ArrowTilesBuilder:
    def __init__(self, memory_limit="40GB", temp_dir=None, threads=None):
        import multiprocessing
        import os
        import tempfile
        
        if threads is None:
            total_threads = max(2, multiprocessing.cpu_count() - 2)
            duckdb_threads = int(total_threads * 0.6)
            rayon_threads = total_threads - duckdb_threads
        else:
            total_threads = max(2, threads)
            duckdb_threads = int(total_threads * 0.6)
            rayon_threads = total_threads - duckdb_threads
            
        # Explicitly throttle Rust's Rayon pool before the extension is invoked
        os.environ["RAYON_NUM_THREADS"] = str(rayon_threads)
        self.rayon_threads = rayon_threads
            
        if temp_dir is None:
            self._temp_dir_obj = tempfile.TemporaryDirectory(dir=".", prefix="duckdb_temp_")
            self.temp_dir = self._temp_dir_obj.name
        else:
            self._temp_dir_obj = None
            os.makedirs(temp_dir, exist_ok=True)
            self.temp_dir = temp_dir
            
        self.con = duckdb.connect(config={
            'allow_unsigned_extensions': 'true', 
            'temp_directory': temp_dir, 
            'max_memory': memory_limit,
            'threads': str(duckdb_threads)
        })
        # We don't need Lindel because Rust does the Hilbert calculations!
        self.con.execute("PRAGMA max_temp_directory_size='400GB';")
        # Enable native DuckDB terminal progress bars for Pass 1 (sorting)
        self.con.execute("PRAGMA enable_progress_bar;")
        self.con.execute("PRAGMA enable_print_progress_bar;")

    def format_time(self, seconds):
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        if h > 0: return f"{h}h {m}m {s}s"
        elif m > 0: return f"{m}m {s}s"
        return f"{s}s"

    def get_engine_path(self):
        # We no longer use a standalone binary, but we keep this method interface 
        # in case we want to return the package path or version in the future.
        return "NATIVE_PYO3"

    def build(self, input_query: str, output_path: str, sort_col: str = "abs_m", x_col: str = "x_norm", y_col: str = "y_norm", sort_dir: str = "ASC", max_capacity: int = 100000, max_zoom: int = 14, resume: bool = False, z_buffer_limit: int = 100000, chunk_buffer_limit: int = 500000, custom_metadata: str = None):
        """
        Executes the 2-Pass DuckLake Pipeline.
        `input_query` must return `x_col` (FLOAT 0-1), `y_col` (FLOAT 0-1), and `sort_col`.
        """
        t_global = time.time()
        start_time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t_global))
        print(f"--- ArrowTiles Pipeline Started at {start_time_str} ---", flush=True)
        if resume:
            print(f"⚠️  RESUME MODE ENABLED. Attempting to skip Pass 1...", flush=True)
        
        grid_size = 1 << int(math.ceil(math.log2(math.sqrt(max_capacity))))
        print(f"Target capacity {max_capacity} -> Grid Size {grid_size}x{grid_size}", flush=True)
        
        try:
            import arrowtiles_core
            arrowtiles_core.init_threadpool(self.rayon_threads)
        except ImportError:
            raise ImportError("Native engine not found. Run `maturin build --release` and `pip install` the resulting wheel.")

        temp_partition_dir = os.path.join(self.temp_dir, "partitions")
        os.makedirs(temp_partition_dir, exist_ok=True)
        
        # ==========================================
        # PASS 1: DuckDB -> Rust Bucketer
        # ==========================================
        import arrowtiles_core
        import glob
        existing_partitions = glob.glob(os.path.join(temp_partition_dir, "z_*.parquet"))
        if resume and len(existing_partitions) > 0:
            print(f"\n✅ Found existing partitions in '{temp_partition_dir}'. SKIPPING PASS 1!", flush=True)
            total_rows = self.con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_partition_dir}/z_*.parquet')").fetchone()[0]
        else:
            t_pass1 = time.time()
            print("\n[Pass 1/2] Sorting globally by Magnitude and calculating Quadtree...", flush=True)
            
            # We need total row count for progress bar
            print("Calculating total rows for benchmark...", flush=True)
            total_rows = self.con.execute(f"SELECT COUNT(*) FROM ({input_query})").fetchone()[0]
            print(f"Total Rows to process: {total_rows:,}", flush=True)
            
            query_pass1 = f"""
                SELECT * 
                FROM ({input_query}) 
                ORDER BY {sort_col} {sort_dir}
            """
            reader_pass1 = self.con.execute(query_pass1).to_arrow_reader(100000)
            
            import pyarrow.cffi as pc
            
            # Export the PyArrow stream to C so Rust can safely read it without holding the GIL
            c_stream_pass1 = pc.ffi.new("struct ArrowArrayStream*")
            ptr_pass1 = int(pc.ffi.cast("uintptr_t", c_stream_pass1))
            reader_pass1._export_to_c(ptr_pass1)
            
            # Call Rust engine natively in Python via PyO3!
            arrowtiles_core.run_bucketer(
                ptr_pass1,
                temp_partition_dir,
                float(grid_size),
                int(max_zoom),
                x_col,
                y_col,
                int(z_buffer_limit)
            )
            
            print(f"\n[OK] Pass 1 completed in {self.format_time(time.time() - t_pass1)}", flush=True)

        # ==========================================
        # PASS 2: DuckDB -> Rust Packer
        # ==========================================
        t_pass2 = time.time()
        print("\n[Pass 2/2] Sorting globally by Spatial Index (Z, Hilbert) and Packing...", flush=True)
        
        
        def get_z(f):
            basename = os.path.basename(f)
            # e.g. z_12.parquet -> 12
            try:
                return int(basename.split('_')[1].split('.')[0])
            except (IndexError, ValueError):
                return 0
                
        partitions = sorted(glob.glob(os.path.join(temp_partition_dir, "z_*.parquet")), key=get_z)
        
        import pyarrow.cffi as pc
        
        # Instantiate the Stateful PyO3 Rust Class.
        # We must provide the schema for the final output, which we can get by 
        # extracting it from the first partition if available.
        # Wait, the Rust `ArrowTilesPacker` expects a stream pointer in `new()` just to read the schema!
        # We can construct a dummy reader from an empty table just to pass the schema.
        
        # Let's peek the schema from the first partition to initialize the packer.
        if not partitions:
            print("No partitions found to pack!", flush=True)
            return
            
        first_part_schema = duckdb.query(f"SELECT * FROM read_parquet('{partitions[0]}') LIMIT 0").to_arrow_table().schema
        empty_table = pa.Table.from_arrays([pa.array([], type=t) for t in first_part_schema.types], schema=first_part_schema)
        dummy_reader = pa.RecordBatchReader.from_batches(first_part_schema, empty_table.to_batches())
        
        c_schema_stream = pc.ffi.new("struct ArrowArrayStream*")
        ptr_schema = int(pc.ffi.cast("uintptr_t", c_schema_stream))
        dummy_reader._export_to_c(ptr_schema)
        
        packer = arrowtiles_core.ArrowTilesPacker(output_path, ptr_schema, int(chunk_buffer_limit), custom_metadata)
        
        for part in tqdm(partitions, desc="Packing Tiles"):
            query_pass2 = f"""
                SELECT * 
                FROM read_parquet('{part}')
                ORDER BY final_tile_id ASC
            """
            reader_pass2 = self.con.execute(query_pass2).to_arrow_reader(100000)
            
            c_stream_pass2 = pc.ffi.new("struct ArrowArrayStream*")
            ptr_pass2 = int(pc.ffi.cast("uintptr_t", c_stream_pass2))
            reader_pass2._export_to_c(ptr_pass2)
            
            # Pass zero-copy stream to Rust natively!
            packer.process_batch(ptr_pass2)
            
        packer.finalize()
        
        print(f"\n[OK] Pass 2 completed in {self.format_time(time.time() - t_pass2)}", flush=True)
        
        # Cleanup
        try:
            import shutil
            shutil.rmtree(temp_partition_dir)
        except OSError:
            print(f"WARNING: Could not delete temp dir. Please manually delete {temp_partition_dir}", flush=True)
            
        print(f"\n--- Pipeline Complete! Total Time: {self.format_time(time.time() - t_global)} ---", flush=True)
        if os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"Final archive size: {size_mb:.2f} MB", flush=True)

def build_gaia(input_parquet: str, output_path: str, resume: bool = False, memory_limit: str = "40GB", temp_dir: str = None):
    """
    Specific helper for the Gaia dataset using the Hammer projection.
    """
    gaia_query = f"""
        WITH raw_data AS (
            SELECT 
                ra, dec, magnitude, bv, parallax, pmra, pmdec, radial_velocity, teff_gspphot,
                RADIANS(ra) AS ra_rad,
                RADIANS(dec) AS dec_rad,
                RADIANS(192.85948) AS a_g,
                RADIANS(27.12825) AS d_g,
                RADIANS(122.93192) AS l_ncp
            FROM read_parquet('{input_parquet}')
        ),
        galactic AS (
            SELECT 
                *,
                ASIN(SIN(d_g)*SIN(dec_rad) + COS(d_g)*COS(dec_rad)*COS(ra_rad - a_g)) AS b_rad,
                l_ncp - ATAN2(
                    COS(dec_rad)*SIN(ra_rad - a_g), 
                    COS(d_g)*SIN(dec_rad) - SIN(d_g)*COS(dec_rad)*COS(ra_rad - a_g)
                ) AS l_rad_raw
            FROM raw_data
        ),
        wrapped AS (
            SELECT *, ((l_rad_raw + 5*PI()) % (2*PI())) - PI() AS l_rad FROM galactic
            WHERE ra IS NOT NULL AND dec IS NOT NULL
        )
        SELECT 
            CAST(( ( -2 * sqrt(2) * cos(b_rad) * sin(l_rad / 2) ) / sqrt(1 + cos(b_rad) * cos(l_rad / 2)) + 2.8284271247461903 ) / 5.6568542494923806 AS FLOAT) AS x_norm,
            1.0 - CAST(( ((sqrt(2) * sin(b_rad)) / sqrt(1 + cos(b_rad) * cos(l_rad / 2))) + 1.4142135623730951 ) / 2.8284271247461903 AS FLOAT) AS y_norm,
            CAST(magnitude AS FLOAT) AS abs_m,
            CAST(bv AS FLOAT) AS bp_rp,
            CAST(parallax AS FLOAT) AS parallax,
            CAST(pmra AS FLOAT) AS pmra,
            CAST(pmdec AS FLOAT) AS pmdec,
            CAST(radial_velocity AS FLOAT) AS radial_velocity,
            CAST(teff_gspphot AS FLOAT) AS teff_gspphot
        FROM wrapped
    """
    
    import json
    gaia_config = {
        "colorField": "bp_rp",
        "colorMin": -1.0,
        "colorMax": 4.0,
        "sizeField": "abs_m",
        "sizeMin": 0.0,
        "sizeMax": 2000.0,
        "xField": "x_u16",
        "yField": "y_u16",
        "mode": "Gaia Baseline",
        "colorScale": "viridis"
    }
    builder = ArrowTilesBuilder(memory_limit=memory_limit, temp_dir=temp_dir)
    builder.build(input_query=gaia_query, output_path=output_path, resume=resume, custom_metadata=json.dumps(gaia_config))

def build_generic(input_parquet: str, output_path: str, x_col: str, y_col: str, sort_col: str, resume: bool = False, memory_limit: str = "40GB", temp_dir: str = None, custom_config: str = None):
    """
    Generic helper for standard spatial datasets.
    Automatically normalizes x_col and y_col to [0, 1] bounds using DuckDB.
    """
    
    # First, get the bounds to normalize the data and stats for numeric columns
    con = duckdb.connect()
    
    # 1. Determine numeric columns dynamically
    schema_query = f"DESCRIBE SELECT * FROM read_parquet('{input_parquet}')"
    schema = con.execute(schema_query).fetchall()
    numeric_types = ['TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT', 'FLOAT', 'DOUBLE', 'DECIMAL', 'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT']
    numeric_cols = [row[0] for row in schema if str(row[1]).split('(')[0].upper() in numeric_types]
    
    # 2. Build aggregation query
    aggs = [
        f"MIN({x_col}) as min_x, MAX({x_col}) as max_x",
        f"MIN({y_col}) as min_y, MAX({y_col}) as max_y",
        f"MIN({sort_col}) as min_sort, MAX({sort_col}) as max_sort"
    ]
    for col in numeric_cols:
        aggs.append(f"MIN({col}), MAX({col})")
        
    bounds_query = f"SELECT {', '.join(aggs)} FROM read_parquet('{input_parquet}')"
    bounds_row = con.execute(bounds_query).fetchone()
    
    min_x, max_x, min_y, max_y, min_sort, max_sort = bounds_row[0:6]
    range_x = max_x - min_x
    range_y = max_y - min_y
    if range_x == 0: range_x = 1.0
    if range_y == 0: range_y = 1.0
    
    # 3. Extract stats dictionary
    stats = {}
    for i, col in enumerate(numeric_cols):
        min_val = bounds_row[6 + i*2]
        max_val = bounds_row[6 + i*2 + 1]
        if min_val is not None and max_val is not None:
            stats[col] = {"min": float(min_val), "max": float(max_val)}
    
    import json
    config_dict = {
        "colorField": sort_col,
        "colorMin": float(min_sort) if min_sort is not None else 0.0,
        "colorMax": float(max_sort) if max_sort is not None else 100.0,
        "sizeField": sort_col,
        "sizeMin": float(min_sort) if min_sort is not None else 0.0,
        "sizeMax": float(max_sort) if max_sort is not None else 100.0,
        "stats": stats,
        "xField": "x_u16",
        "yField": "y_u16",
        "mode": "Chart Mode",
        "colorScale": "viridis"
    }
    
    if custom_config:
        try:
            user_config = json.loads(custom_config)
            config_dict.update(user_config)
        except Exception as e:
            print(f"Warning: Failed to parse custom_config: {e}")
            
    metadata_json_str = json.dumps(config_dict)
    
    generic_query = f"""
        SELECT *,
            ({x_col} - {min_x}) / {range_x} AS x_norm,
            ({y_col} - {min_y}) / {range_y} AS y_norm
        FROM read_parquet('{input_parquet}')
        WHERE {x_col} IS NOT NULL AND {y_col} IS NOT NULL
        LIMIT 10000000
    """
    
    builder = ArrowTilesBuilder(memory_limit=memory_limit, temp_dir=temp_dir)
    builder.build(
        input_query=generic_query, 
        output_path=output_path, 
        sort_col=sort_col,
        x_col='x_norm',
        y_col='y_norm',
        resume=resume,
        custom_metadata=metadata_json_str
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ArrowTiles 2-Pass DuckLake Pipeline")
    
    # Core arguments
    parser.add_argument("--dataset", type=str, choices=["gaia", "generic"], default="generic", help="Which pipeline to run.")
    parser.add_argument("--input", type=str, required=True, help="Input glob of parquet files")
    parser.add_argument("--output", type=str, required=True, help="Output .arrowtiles file")
    
    # Generic Schema Arguments (Only used if --dataset generic)
    parser.add_argument("--x-col", type=str, default="x_norm", help="The column name for X coordinates (0.0 to 1.0)")
    parser.add_argument("--y-col", type=str, default="y_norm", help="The column name for Y coordinates (0.0 to 1.0)")
    parser.add_argument("--sort-col", type=str, default="abs_m", help="The column to globally sort by for LOD/culling")
    parser.add_argument("--config", type=str, default=None, help="JSON string for visualization config overrides")
    
    # System arguments
    parser.add_argument("--resume", action="store_true", help="Resume from Pass 2 if Pass 1 completed")
    parser.add_argument("--memory-limit", type=str, default="40GB", help="DuckDB max_memory limit")
    parser.add_argument("--temp-dir", type=str, default=None, help="Explicit temp directory (defaults to tempfile if not resuming, or ./duckdb_temp if resuming)")
    
    args = parser.parse_args()
    
    # If resuming and no temp dir was explicitly provided, default to ./duckdb_temp 
    # to find previous partitions
    if args.resume and args.temp_dir is None:
        args.temp_dir = "./duckdb_temp"
    
    if args.dataset == "gaia":
        print("🌌 Running ESA Gaia Pipeline...")
        build_gaia(args.input, args.output, args.resume, args.memory_limit, args.temp_dir)
    elif args.dataset == "generic":
        print(f"📦 Running Generic Pipeline (X: {args.x_col}, Y: {args.y_col}, Sort: {args.sort_col})...")
        build_generic(
            input_parquet=args.input, 
            output_path=args.output,
            x_col=args.x_col,
            y_col=args.y_col,
            sort_col=args.sort_col,
            resume=args.resume,
            memory_limit=args.memory_limit,
            temp_dir=args.temp_dir,
            custom_config=args.config
        )

import duckdb
import pyarrow as pa
import subprocess
import os
import time
import math
import argparse
from tqdm import tqdm

class ArrowTilesBuilder:
    def __init__(self, memory_limit="40GB", temp_dir="./duckdb_temp"):
        os.makedirs(temp_dir, exist_ok=True)
        self.temp_dir = temp_dir
        self.con = duckdb.connect(config={
            'allow_unsigned_extensions': 'true', 
            'temp_directory': temp_dir, 
            'max_memory': memory_limit
        })
        # We don't need Lindel because Rust does the Hilbert calculations!
        self.con.execute("PRAGMA max_temp_directory_size='400GB';")

    def format_time(self, seconds):
        m, s = divmod(int(seconds), 60)
        h, m = divmod(m, 60)
        if h > 0: return f"{h}h {m}m {s}s"
        elif m > 0: return f"{m}m {s}s"
        return f"{s}s"

    def get_engine_path(self):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        exe = "arrowtiles_engine.exe" if os.name == 'nt' else "arrowtiles_engine"
        return os.path.join(base_dir, "target", "release", exe)

    def build(self, input_query: str, output_path: str, sort_col: str = "abs_m", x_col: str = "x_norm", y_col: str = "y_norm", sort_dir: str = "ASC", max_capacity: int = 100000, max_zoom: int = 14, resume: bool = False):
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
        
        engine_path = self.get_engine_path()
        if not os.path.exists(engine_path):
            raise FileNotFoundError(f"Rust Engine not found at {engine_path}. Run `cargo build --release` first.")

        temp_partition_dir = os.path.join(self.temp_dir, "partitions")
        os.makedirs(temp_partition_dir, exist_ok=True)
        
        # ==========================================
        # PASS 1: DuckDB -> Rust Bucketer
        # ==========================================
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
            
            process1 = subprocess.Popen(
                [engine_path, "--bucketer", temp_partition_dir, str(grid_size), str(max_zoom), "--x-col", x_col, "--y-col", y_col],
                stdin=subprocess.PIPE
            )
            
            with process1.stdin:
                writer = pa.ipc.new_stream(process1.stdin, reader_pass1.schema)
                with tqdm(total=total_rows, desc="Pass 1: Bucketing", unit="rows") as pbar:
                    for batch in reader_pass1:
                        writer.write_batch(batch)
                        pbar.update(batch.num_rows)
                writer.close()
                
            process1.wait()
            if process1.returncode != 0:
                raise RuntimeError("Pass 1: Rust Bucketer failed!")
                
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
        
        process2 = subprocess.Popen(
            [engine_path, "--packer", output_path],
            stdin=subprocess.PIPE
        )
        
        with process2.stdin:
            writer = None
            with tqdm(total=total_rows, desc="Pass 2: Packing", unit="rows") as pbar:
                for part in partitions:
                    query_pass2 = f"""
                        SELECT * 
                        FROM read_parquet('{part}')
                        ORDER BY final_tile_id ASC
                    """
                    reader_pass2 = self.con.execute(query_pass2).to_arrow_reader(100000)
                    
                    if writer is None:
                        writer = pa.ipc.new_stream(process2.stdin, reader_pass2.schema)
                        
                    for batch in reader_pass2:
                        writer.write_batch(batch)
                        pbar.update(batch.num_rows)
            
            if writer is not None:
                writer.close()
            
        process2.wait()
        if process2.returncode != 0:
            raise RuntimeError("Pass 2: Rust Packer failed!")
            
        print(f"\n[OK] Pass 2 completed in {self.format_time(time.time() - t_pass2)}", flush=True)
        
        # Cleanup
        try:
            import shutil
            shutil.rmtree(temp_partition_dir)
        except OSError:
            pass
            
        print(f"\n--- Pipeline Complete! Total Time: {self.format_time(time.time() - t_global)} ---", flush=True)
        if os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"Final archive size: {size_mb:.2f} MB", flush=True)

def build_gaia(input_parquet: str, output_path: str, resume: bool = False, memory_limit: str = "40GB"):
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
    
    builder = ArrowTilesBuilder(memory_limit=memory_limit)
    builder.build(input_query=gaia_query, output_path=output_path, resume=resume)

def build_generic(input_parquet: str, output_path: str, x_col: str, y_col: str, sort_col: str, resume: bool = False, memory_limit: str = "40GB"):
    """
    Generic helper for standard spatial datasets.
    Automatically normalizes x_col and y_col to [0, 1] bounds using DuckDB.
    """
    
    # First, get the bounds to normalize the data
    con = duckdb.connect()
    bounds = con.execute(f"""
        SELECT 
            MIN({x_col}) as min_x, MAX({x_col}) as max_x,
            MIN({y_col}) as min_y, MAX({y_col}) as max_y
        FROM read_parquet('{input_parquet}')
    """).fetchone()
    
    min_x, max_x, min_y, max_y = bounds
    range_x = max_x - min_x
    range_y = max_y - min_y
    if range_x == 0: range_x = 1.0
    if range_y == 0: range_y = 1.0
    
    generic_query = f"""
        SELECT *,
            ({x_col} - {min_x}) / {range_x} AS x_norm,
            ({y_col} - {min_y}) / {range_y} AS y_norm
        FROM read_parquet('{input_parquet}')
        WHERE {x_col} IS NOT NULL AND {y_col} IS NOT NULL
        LIMIT 10000000
    """
    
    builder = ArrowTilesBuilder(memory_limit=memory_limit)
    builder.build(
        input_query=generic_query, 
        output_path=output_path, 
        sort_col=sort_col,
        x_col='x_norm',
        y_col='y_norm',
        resume=resume
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
    
    # System arguments
    parser.add_argument("--resume", action="store_true", help="Resume from Pass 2 if Pass 1 completed")
    parser.add_argument("--memory-limit", type=str, default="40GB", help="DuckDB max_memory limit")
    
    args = parser.parse_args()
    
    if args.dataset == "gaia":
        print("🌌 Running ESA Gaia Pipeline...")
        build_gaia(args.input, args.output, args.resume, args.memory_limit)
    elif args.dataset == "generic":
        print(f"📦 Running Generic Pipeline (X: {args.x_col}, Y: {args.y_col}, Sort: {args.sort_col})...")
        build_generic(
            input_parquet=args.input, 
            output_path=args.output,
            x_col=args.x_col,
            y_col=args.y_col,
            sort_col=args.sort_col,
            resume=args.resume,
            memory_limit=args.memory_limit
        )

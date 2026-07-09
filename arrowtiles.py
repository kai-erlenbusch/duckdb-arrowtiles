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
        
        # Install and load Lindel for Hilbert curves
        self.con.execute("INSTALL lindel FROM community; LOAD lindel;")
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

    def build(self, input_query: str, output_path: str, max_capacity: int = 100000, max_zoom: int = 14, resume: bool = False):
        """
        Executes the 2-Pass DuckLake Pipeline.
        `input_query` must return `x_norm` (FLOAT 0-1), `y_norm` (FLOAT 0-1), and `abs_m` (FLOAT).
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

        temp_parquet = os.path.join(self.temp_dir, "bucketed_temp.parquet")
        
        # ==========================================
        # PASS 1: DuckDB -> Rust Bucketer
        # ==========================================
        if resume and os.path.exists(temp_parquet):
            print(f"\n✅ Found existing '{temp_parquet}'. SKIPPING PASS 1!", flush=True)
            total_rows = self.con.execute(f"SELECT COUNT(*) FROM read_parquet('{temp_parquet}')").fetchone()[0]
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
                ORDER BY abs_m ASC
            """
            
            reader_pass1 = self.con.execute(query_pass1).fetch_arrow_reader(batch_size=100000)
            
            process1 = subprocess.Popen(
                [engine_path, "--bucketer", temp_parquet, str(grid_size), str(max_zoom)],
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
                
            print(f"\n✅ Pass 1 completed in {self.format_time(time.time() - t_pass1)}", flush=True)

        # ==========================================
        # PASS 2: DuckDB -> Rust Packer
        # ==========================================
        t_pass2 = time.time()
        print("\n[Pass 2/2] Sorting globally by Spatial Index (Z, Hilbert) and Packing...", flush=True)
        
        query_pass2 = f"""
            SELECT * 
            FROM read_parquet('{temp_parquet}')
            ORDER BY z ASC, final_tile_id ASC
        """
        
        reader_pass2 = self.con.execute(query_pass2).fetch_arrow_reader(batch_size=100000)
        
        process2 = subprocess.Popen(
            [engine_path, "--packer", output_path],
            stdin=subprocess.PIPE
        )
        
        with process2.stdin:
            writer = pa.ipc.new_stream(process2.stdin, reader_pass2.schema)
            with tqdm(total=total_rows, desc="Pass 2: Packing", unit="rows") as pbar:
                for batch in reader_pass2:
                    writer.write_batch(batch)
                    pbar.update(batch.num_rows)
            writer.close()
            
        process2.wait()
        if process2.returncode != 0:
            raise RuntimeError("Pass 2: Rust Packer failed!")
            
        print(f"\n✅ Pass 2 completed in {self.format_time(time.time() - t_pass2)}", flush=True)
        
        # Cleanup
        if not resume: # Only delete if we didn't resume, to be safe. Or actually we want to clean it up either way if pass 2 finishes.
            try:
                os.remove(temp_parquet)
            except OSError:
                pass
            
        print(f"\n--- Pipeline Complete! Total Time: {self.format_time(time.time() - t_global)} ---", flush=True)
        if os.path.exists(output_path):
            size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"Final archive size: {size_mb:.2f} MB", flush=True)

def build_gaia(input_parquet: str, output_path: str):
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
    
    builder = ArrowTilesBuilder()
    builder.build(input_query=gaia_query, output_path=output_path, resume=args.resume)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ArrowTiles 2-Pass DuckLake Pipeline")
    parser.add_argument("--input", type=str, required=True, help="Input glob of parquet files")
    parser.add_argument("--output", type=str, required=True, help="Output .arrowtiles file")
    parser.add_argument("--resume", action="store_true", help="Resume from Pass 2 if Pass 1 completed")
    
    args = parser.parse_args()
    build_gaia(args.input, args.output)

import sys
import duckdb
import pyarrow as pa
import os

con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
con.execute("SET temp_directory = './duckdb_tmp'")
con.execute("SET memory_limit = '32GB'")
con.execute("SET threads = 8")
script_dir = os.path.dirname(os.path.abspath(__file__))
ext_path = os.path.join(script_dir, 'target', 'release', 'arrowtiles.duckdb_extension').replace('\\', '/')
con.execute(f"LOAD '{ext_path}'")

query = f"""
    SELECT *, hilbert_xy(ra, dec, 10) AS final_tile_id 
    FROM read_parquet('D:/exploratory/duckdb-extension/deepgraph-arrowtiles-sandbox/s3_cache/**/*.parquet') 
    LIMIT 1000
"""

reader = con.execute(query).fetch_record_batch()
schema = reader.schema

with pa.ipc.RecordBatchStreamWriter(sys.stdout.buffer, schema) as writer:
    while True:
        try:
            batch = reader.read_next_batch()
            writer.write_batch(batch)
        except StopIteration:
            break

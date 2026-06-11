import duckdb
import os

print("Starting DuckDB...")
conn = duckdb.connect(config={'allow_unsigned_extensions': 'true'})

print("Loading arrowtiles extension...")
try:
    conn.execute("LOAD 'target/release/arrowtiles.duckdb_extension'")
    print("SUCCESS! ArrowTiles extension loaded.")
except Exception as e:
    print("FAILED to load extension:", e)
    exit(1)

# Create some dummy spatial data
conn.execute("CREATE TABLE spatial_data AS SELECT * FROM (VALUES (1, 40.7128, -74.0060), (2, 34.0522, -118.2437), (3, 51.5074, -0.1278)) t(id, lat, lon)")

print("Testing ArrowTiles export...")
try:
    # We call the table function arrowtiles_export(query, filepath)
    if os.path.exists("output.feather"):
        os.remove("output.feather")
    
    res = conn.execute("SELECT * FROM arrowtiles_export('SELECT * FROM spatial_data', 'output.feather')").fetchall()
    print("Export Result:", res)
    
    if os.path.exists("output.feather"):
        size = os.path.getsize("output.feather")
        print(f"SUCCESS! Arrow IPC file 'output.feather' generated. Size: {size} bytes")
    else:
        print("FAILED! output.feather was not created.")
except Exception as e:
    print("Export failed:", e)

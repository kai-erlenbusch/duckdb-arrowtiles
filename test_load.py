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

# Create some dummy spatial data (with a NULL row to test Arrow NULL handling)
conn.execute("CREATE TABLE spatial_data AS SELECT * FROM (VALUES (1, -74.0060, 40.7128), (2, -118.2437, 34.0522), (3, -0.1278, 51.5074), (4, NULL, 51.5074)) t(id, lon, lat)")

print("Testing UDF hilbert_xy...")
udf_res = conn.execute("SELECT id, lon, lat, hilbert_xy(lon, lat, 10::UTINYINT) as tile_id FROM spatial_data ORDER BY tile_id").fetchall()
for row in udf_res:
    print(f"Row {row[0]}: lon={row[1]}, lat={row[2]} -> tile_id={row[3]}")

print("\nTesting ArrowTiles export with UDF sorting...")
try:
    # We call the table function arrowtiles_export(query, filepath)
    if os.path.exists("output.feather"):
        os.remove("output.feather")
        
    query = "SELECT *, hilbert_xy(lon, lat, 10::UTINYINT) as tile_id FROM spatial_data ORDER BY tile_id"
    res = conn.execute(f"SELECT * FROM arrowtiles_export('{query}', 'output.feather')").fetchall()
    print("Export Result:", res)
    
    if os.path.exists("output.feather"):
        size = os.path.getsize("output.feather")
        print(f"SUCCESS! Arrow IPC file 'output.feather' generated. Size: {size} bytes")
    else:
        print("FAILED! output.feather was not created.")
except Exception as e:
    print("Export failed:", e)

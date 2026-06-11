import duckdb

print("Starting DuckDB...")
conn = duckdb.connect(config={'allow_unsigned_extensions': 'true'})

print("Loading arrowtiles extension...")
try:
    conn.execute("LOAD 'target/release/arrowtiles.duckdb_extension'")
    print("SUCCESS! ArrowTiles extension loaded.")
except Exception as e:
    print("FAILED to load extension:", e)

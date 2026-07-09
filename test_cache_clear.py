import duckdb

print("Loading optimized DuckDB extension...")
con = duckdb.connect(config={'allow_unsigned_extensions': 'true'})
con.execute("LOAD 'D:/exploratory/duckdb-extension/duckdb-arrowtiles/target/release/arrowtiles.duckdb_extension'")

print("Testing arrowtiles_assign_tile...")
con.execute("""
    SELECT arrowtiles_assign_tile(0.5 + x*0.0000000001, 0.5, 14::UTINYINT, 100::UINTEGER) AS packed_id 
    FROM generate_series(1, 200) AS t(x)
""")
res1 = con.fetchall()
print(f"Generated 200 points. Unique assignments: {len(set(r[0] for r in res1 if r[0] is not None))}")
print(f"Nulls (capped): {sum(1 for r in res1 if r[0] is None)}")

print("Testing arrowtiles_clear_cache...")
con.execute("SELECT arrowtiles_clear_cache(1::TINYINT)")
res_clear = con.fetchall()
print(f"Items cleared from cache: {res_clear[0][0]}")

print("Testing assignment after clear...")
con.execute("""
    SELECT arrowtiles_assign_tile(0.5 + x*0.0000000001, 0.5, 14::UTINYINT, 100::UINTEGER) AS packed_id 
    FROM generate_series(1, 200) AS t(x)
""")
res2 = con.fetchall()
print(f"Generated 200 points. Unique assignments: {len(set(r[0] for r in res2 if r[0] is not None))}")
print(f"Nulls (capped): {sum(1 for r in res2 if r[0] is None)}")

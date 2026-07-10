import duckdb
res = duckdb.query("DESCRIBE SELECT * FROM read_parquet('D:/exploratory/duckdb-extension/deepgraph-arrowtiles-sandbox/s3_cache/batch_000.parquet')").fetchall()
for r in res:
    print(r)

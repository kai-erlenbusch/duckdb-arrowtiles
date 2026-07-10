import duckdb
con = duckdb.connect()
con.execute("CREATE TABLE t AS SELECT random() as x, random() as y, random() as s FROM range(1000000)")
con.execute("COPY t TO 'dummy.parquet' (FORMAT PARQUET)")

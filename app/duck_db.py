import duckdb
import pyarrow.parquet as pq


db_file = "github_data.db"
parquet_file = "issues.parquet"


with duckdb.connect(database=db_file) as con:
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    con.execute("DROP TABLE IF EXISTS raw.test;")

    con.execute(f"""
        CREATE TABLE raw.test AS 
        SELECT * FROM read_parquet('{parquet_file}');
    """)

    print(con.execute("SELECT * FROM raw.test LIMIT 5;").fetchdf())

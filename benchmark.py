import timeit
import pandas as pd
import sqlalchemy
import connectorx as cx
import unchecked_io
import os
import yaml # We need pyyaml for this
import time

# --- 1. Define Connection Strings and Query ---
# These must match your local Docker setup
DB_USER = "postgres"
DB_PASS = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5433" # <-- Your local Docker port
DB_NAME = "postgres"

# Global Configuration
BLAST_RADIUS = 125000 # Rows per parallel task (1M / 62500 = 16 partitions)

# SQLAlchemy connection string (for Pandas)
sqlalchemy_conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = sqlalchemy.create_engine(sqlalchemy_conn_str)

# ConnectorX connection string
connectorx_conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# The query for Pandas and ConnectorX (non-COPY)
sql_query = "SELECT * FROM benchmark_table"

# --- 2. Dynamically create the config.yaml for UncheckedIO ---
config_file = "config.yaml"
unchecked_io_query = "COPY (SELECT id::BIGINT, uuid::TEXT, username::TEXT, score::REAL, is_active::BOOLEAN, last_login::TIMESTAMP, notes::TEXT, course_id::INT, start_date::DATE, rating::FLOAT8 FROM benchmark_table) TO STDOUT (FORMAT binary)"

config_data = {
    'connection_string': connectorx_conn_str,
    'query': unchecked_io_query,
    'schema': [
        {'column_name': 'id', 'arrow_type': 'Int64'},
        {'column_name': 'uuid', 'arrow_type': 'Utf8'},
        {'column_name': 'username', 'arrow_type': 'Utf8'},
        {'column_name': 'score', 'arrow_type': 'Float32'},
        {'column_name': 'is_active', 'arrow_type': 'Boolean'},
        {'column_name': 'last_login', 'arrow_type': 'Timestamp(Nanosecond, None)'},
        {'column_name': 'notes', 'arrow_type': 'Utf8'},
        {'column_name': 'course_id', 'arrow_type': 'Int32'},
        {'column_name': 'start_date', 'arrow_type': 'Date32'},
        {'column_name': 'rating', 'arrow_type': 'Float64'}
    ]
}

with open(config_file, 'w') as f:
    yaml.dump(config_data, f)

print(f"UncheckedIO Config: {config_file} (dynamically created for local test)")

# --- 3. Define Benchmark Functions ---

def test_pandas():
    df = pd.read_sql(sql_query, engine)
    return df

def test_connectorx():
    df = cx.read_sql(connectorx_conn_str, sql_query, return_type="arrow")
    return df

def test_unchecked_io():
    # FIX: Pass the BLAST_RADIUS argument
    arrow_table = unchecked_io.load_data_from_config(config_file, BLAST_RADIUS)
    return arrow_table

# --- 4. Run Benchmarks ---
run_count = 3
print(f"Running benchmarks for 5,000,000 rows (average of {run_count} runs)...")
print(f"Blast Radius: {BLAST_RADIUS} rows per task")

# --- Pandas ---
print("\nRunning Pandas warmup...")
_ = test_pandas()
print("Timing pandas.read_sql...")
pandas_time = timeit.timeit(test_pandas, number=run_count) / run_count
print(f"Pandas Average Time: {pandas_time * 1000:.2f} ms")

# --- ConnectorX ---
print("\nRunning ConnectorX warmup...")
_ = test_connectorx()
print("Timing connectorx.read_sql...")
connectorx_time = timeit.timeit(test_connectorx, number=run_count) / run_count
print(f"ConnectorX Average Time: {connectorx_time * 1000:.2f} ms")

# --- UncheckedIO ---
print("\nRunning UncheckedIO warmup...")
_ = test_unchecked_io()
print("Timing unchecked_io.load_data_from_config...")
unchecked_io_time = timeit.timeit(test_unchecked_io, number=run_count) / run_count
print(f"UncheckedIO Average Time: {unchecked_io_time * 1000:.2f} ms")

# --- 5. Print Results ---
print("\n" + "---" * 10)
print("--- Benchmark Results (5,000,000 Rows) ---")
print(f"Pandas:       {pandas_time * 1000:>10.2f} ms")
print(f"ConnectorX:   {connectorx_time * 1000:>10.2f} ms")
print(f"UncheckedIO:  {unchecked_io_time * 1000:>10.2f} ms")
print("---" * 10)

print("\n--- Ratios ---")
if unchecked_io_time > 0:
    print(f"UncheckedIO is {pandas_time / unchecked_io_time:.2f}x faster than Pandas")
    print(f"UncheckedIO is {connectorx_time / unchecked_io_time:.2f}x faster than ConnectorX")
else:
    print("UncheckedIO was too fast to measure accurately!")
import sqlalchemy
import os
import time

# --- Configuration (Must match benchmark.py) ---
DB_USER = "postgres"
DB_PASS = "mysecretpassword"
DB_HOST = "localhost"
DB_PORT = "5433"
DB_NAME = "postgres"

# Build the SQLAlchemy connection string
sqlalchemy_conn_str = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# Define the path to your setup SQL file
# Adjust this path if you moved the setup_db.sql file
# NOTE: This path should be correct based on the file structure you uploaded:
sql_file_path = "billthemaker/unchecked-io/unchecked-io-f5624c1ce64a916629b9d01def2cfe6de0d08c63/setup_db.sql"


def run_sql_setup(engine, path):
    """Executes the SQL file content against the database."""
    print(f"Connecting to database at {DB_HOST}:{DB_PORT}...")
    try:
        # 1. Read the raw SQL content
        with open(path, 'r') as f:
            sql_content = f.read()

        # 2. Establish connection and execute
        with engine.connect() as connection:
            print(f"Executing SQL file: {path}")
            # Use begin/commit block for safety
            with connection.begin():
                connection.exec_driver_sql(sql_content)

            print("Successfully executed setup script!")
            print("Starting ANALYZE (may take a moment for 20M rows)...")

            # Execute ANALYZE separately for proper commit timing
            with connection.begin():
                connection.exec_driver_sql("ANALYZE benchmark_table")

            print("Database setup complete.")

    except Exception as e:
        print(f"FATAL ERROR during database setup: {e}")
        print("Please ensure your PostgreSQL server is running and accessible.")

if __name__ == "__main__":
    engine = sqlalchemy.create_engine(sqlalchemy_conn_str)
    start_time = time.time()
    run_sql_setup(engine, sql_file_path)
    end_time = time.time()
    print(f"Total time taken for setup: {end_time - start_time:.2f} seconds.")
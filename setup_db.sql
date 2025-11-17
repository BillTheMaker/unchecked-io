-- 1. Drop the old table if it exists
DROP TABLE IF EXISTS benchmark_table;

-- 2. Create the new 10-column table
CREATE TABLE benchmark_table (
                                 id BIGINT,
                                 uuid TEXT,
                                 username TEXT,
                                 score REAL,
                                 is_active BOOLEAN,
                                 last_login TIMESTAMP,
                                 notes TEXT,
                                 course_id INT,
                                 start_date DATE,
                                 rating FLOAT8
);

-- 3. Use generate_series to insert 1,000,000 rows of data
INSERT INTO benchmark_table (
    id,
    uuid,
    username,
    score,
    is_active,
    last_login,
    notes,
    course_id,
    start_date,
    rating
)
SELECT
    i AS id,
    md5(i::TEXT) AS uuid, -- Use md5 to generate a text uuid
    'user_' || i AS username,
    (random() * 100)::REAL AS score,
    (i % 2 = 0) AS is_active,
    NOW() - (random() * '365 days'::INTERVAL) AS last_login,
    CASE WHEN i % 10 = 0 THEN NULL ELSE 'Sample notes' END AS notes,
    (random() * 10 + 100)::INT AS course_id,
    (NOW() - (random() * '1000 days'::INTERVAL))::DATE AS start_date,
    (random() * 5)::FLOAT8 AS rating
FROM generate_series(1, 1000000) s(i);

-- 4. Analyze the table for better query planning (good practice)
ANALYZE benchmark_table;
```

### Action 2: Commit and Push the Change

You must commit and push this new `setup_db.sql` to your GitHub repo so Colab can pull it.

```bash
# In your local terminal
git add setup_db.sql
git commit -m "Create 1M row benchmark table"
git push
```

### Action 3: Update Your Colab Notebook

Now, in Colab, **replace your old Cell 2 and Cell 3** with these two new cells.

**New Cell 2 (Install & Setup):** This cell now does *all* setup in one go to fix the connection error.

```python
# 2. Install Rust, Tools, and Clone Repo
!curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
import os
os.environ['PATH'] += ":/root/.cargo/bin"
!pip install maturin wheel

# Go to root, clean up, and clone fresh
%cd /content/
!rm -rf unchecked-io
!git clone https://github.com/BillTheMaker/unchecked-io.git
%cd unchecked-io

# Build and install UncheckedIO
!pip install .
```

**New Cell 3 (Start & Populate DB):** This cell installs Postgres, robustly starts it, and runs your *new* 1M row script.

```python
# 3. Install, Run, and Populate PostgreSQL Server
print("Installing PostgreSQL...")
!apt-get -y -qq install postgresql postgresql-client > /dev/null

# Force the service to start and set password
!service postgresql start
!sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'mysecretpassword';"

print("Running 1M row setup_db.sql script (this may take a moment)...")
# Run our *new* 1M row SQL script
!psql "postgresql://postgres:mysecretpassword@localhost:5432/postgres" -f setup_db.sql
print("Database setup complete.")
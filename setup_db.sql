DROP TABLE IF EXISTS benchmark_table;

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
FROM generate_series(1, 20000000) s(i);

-- 4. Analyze the table for better query planning (good practice)
ANALYZE benchmark_table;


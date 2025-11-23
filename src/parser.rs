// --- External Crates ---
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{NoTls, CopyOutStream, Config as PgConfig};
// Required for the connection pool
use deadpool_postgres::{Pool, Manager, Runtime};
use anyhow::{Context, Result, anyhow};
use arrow::array::{
    ArrayBuilder, ArrayRef,
    Int64Builder, Float64Builder, Float32Builder, StringBuilder, BooleanBuilder,
    TimestampNanosecondBuilder, Date32Builder, Int32Builder
};
use arrow::datatypes::{
    DataType, Field, Schema,
};
use arrow::record_batch::RecordBatch;
use futures_util::stream::StreamExt;
use bytes::{Bytes, BytesMut, Buf};
use byteorder::{BigEndian, ReadBytesExt};
use std::io::{Cursor, Read};
use std::str;
use std::str::FromStr;
use chrono::{NaiveDateTime, NaiveDate};
use std::time::Instant;
use tokio::task::JoinSet;
use arrow::compute::concat_batches;
// NEW: For the Work Stealing Queue
use async_channel;
// NEW: Tracing macros for profiling - Only import if feature is enabled
#[cfg(feature = "profiling")]
use tracing::{span, Level};

// --- Internal Crates ---
use crate::config::{ConnectorConfig, load_and_validate_config};


// --- CONSTANTS ---
// Optimized calculation of epoch delta (2000-01-01 00:00:00 to 1970-01-01 00:00:00)
// 10957 days * 86400 seconds/day * 1,000,000 micros/second = 946684800000000 micros
const POSTGRES_EPOCH_MICROS_OFFSET: i64 = 946684800000000;


// --- 1. CORE DATABASE LOGIC (WORKER POOL PATTERN) ---
// This is the fully optimized function using Connection Pooling and Static Dispatch.
pub async fn run_db_logic(config: ConnectorConfig, blast_radius: i64) -> Result<RecordBatch> {

    // Start overall timer
    let start_total = Instant::now();
    let start_phase1 = Instant::now();

    // NEW: High-level span for the whole operation
    #[cfg(feature = "profiling")]
    let root_span = span!(Level::INFO, "UncheckedIO_Run");
    #[cfg(feature = "profiling")]
    let _root_guard = root_span.enter();

    // --- PHASE 1: SETUP CONNECTION POOL & STATS ---
    #[cfg(feature = "profiling")]
    let phase1_span = span!(Level::INFO, "Phase1_Setup");
    #[cfg(feature = "profiling")]
    let _p1_guard = phase1_span.enter();

    // 1. Calculate Worker Count (Fixed Parallelism)
    let num_workers = num_cpus::get();
    println!("UncheckedIO: Detected {} logical cores. Spawning {} worker threads.", num_workers, num_workers);

    // 2. Setup Connection Pool
    let pg_config: tokio_postgres::Config = PgConfig::from_str(&config.connection_string)
        .context("Invalid connection string in config")?;

    let manager = Manager::new(pg_config.clone(), NoTls);
    // FIX: Set pool size exactly to num_workers to prevent starvation or waiting
    let pool = Pool::builder(manager)
        .max_size(num_workers)
        .runtime(Runtime::Tokio1)
        .build()
        .context("Failed to build connection pool")?;

    // 3. Query Table Bounds
    // We grab a temporary connection just for this setup phase
    let client = pool.get().await.context("Failed to get pool connection for stats query")?;
    let partition_key = "id";

    let (base_query, _) = config.query.trim().split_once("TO STDOUT (FORMAT binary)")
        .context("Failed to parse base query from config")?;
    let base_query_inner = base_query.trim().trim_start_matches("COPY (").trim_end_matches(")");

    let stats_query = format!("SELECT MIN({}), MAX({}) FROM ({}) AS subquery", partition_key, partition_key, base_query_inner);

    let row = client.query_one(&stats_query, &[]).await?;
    let min_id: i64 = row.try_get(0).context("Failed to get MIN(id)")?;
    let max_id: i64 = row.try_get(1).context("Failed to get MAX(id)")?;
    drop(client); // Return connection to pool immediately

    println!("UncheckedIO: ID Range: {} to {}", min_id, max_id);

    // --- NEW: DYNAMIC PARTITION SIZING ---
    let total_rows = (max_id - min_id + 1).max(1);
    let calculated_blast_radius = if blast_radius <= 0 {
        // Auto-tuning: Aim for ~4 chunks per worker to balance load
        let target_chunks = (num_workers * 4) as i64;
        let dynamic_size = total_rows / target_chunks;
        // Ensure a sane minimum (e.g., don't make chunks of 1 row)
        let size = dynamic_size.max(10_000);
        println!("UncheckedIO: Auto-tuned partition size to {} rows (Targeting {} chunks).", size, target_chunks);
        size
    } else {
        println!("UncheckedIO: Using user-defined partition size: {} rows.", blast_radius);
        blast_radius
    };


    // 4. Create Work Queue
    // We use a tuple: (index, query, expected_rows) so we can re-sort later
    struct PartitionTask {
        index: usize,
        query: String,
        expected_rows: usize
    }

    // Create an unbounded channel. 
    // tx = transmitter (main thread), rx = receiver (workers)
    let (tx, rx) = async_channel::unbounded::<PartitionTask>();

    // 5. Populate the Queue (The "Blast Radius" Logic)
    let mut current_min = min_id;
    let mut idx = 0;
    let mut total_partitions = 0;

    while current_min <= max_id {
        let current_max = (current_min + calculated_blast_radius - 1).min(max_id);
        let new_query = format!(
            "COPY (SELECT * FROM ({}) AS sub WHERE {} BETWEEN {} AND {}) TO STDOUT (FORMAT binary)",
            base_query_inner, partition_key, current_min, current_max
        );
        let estimated_rows = (current_max - current_min + 1) as usize;

        // FIX: Removed 'partitions.push(...)' which caused the error.
        // We send directly to the channel now.
        let task = PartitionTask { index: idx, query: new_query, expected_rows: estimated_rows };

        // Send to queue (non-blocking since it's unbounded)
        tx.send(task).await.context("Failed to fill work queue")?;

        current_min += calculated_blast_radius;
        idx += 1;
        total_partitions += 1;
    }
    // Close the channel so workers know when to stop
    tx.close();

    println!("UncheckedIO: Queued {} partitions for processing.", total_partitions);

    #[cfg(feature = "profiling")]
    drop(_p1_guard); // End Phase 1 Span
    let duration_phase1 = start_phase1.elapsed();


    // --- PHASE 2: PARALLEL EXECUTION (DATA TRANSFER + PARSING) ---
    let start_phase2 = Instant::now();
    #[cfg(feature = "profiling")]
    let phase2_span = span!(Level::INFO, "Phase2_Execution");
    #[cfg(feature = "profiling")]
    let _p2_guard = phase2_span.enter();

    let arrow_schema = Arc::new(build_arrow_schema(&config)?);
    let mut join_set = JoinSet::new();

    // Spawn exactly 'num_workers' long-lived tasks
    for worker_id in 0..num_workers {
        let worker_rx = rx.clone();
        let worker_pool = pool.clone();
        let worker_schema = arrow_schema.clone();

        join_set.spawn(async move {
            // VISUALIZATION: Create a "track" for this worker in Tracy
            #[cfg(feature = "profiling")]
            let worker_span = span!(Level::INFO, "Worker_Thread", id = worker_id);
            #[cfg(feature = "profiling")]
            let _w_guard = worker_span.enter();

            let mut worker_batches: Vec<(usize, RecordBatch)> = Vec::new();

            // Worker Loop: Keep grabbing tasks until the queue is empty and closed
            while let Ok(task) = worker_rx.recv().await {

                // VISUALIZATION: Show exactly which partition is being processed
                #[cfg(feature = "profiling")]
                let task_span = span!(Level::INFO, "Processing_Task", partition_id = task.index);
                #[cfg(feature = "profiling")]
                let _t_guard = task_span.enter();

                // Process the task
                // We wrap this in an inner block to easily catch errors for Self-Healing
                let result = async {
                    let client = worker_pool.get().await.context("Pool exhausted")?;
                    let copy_stream = client.copy_out(task.query.as_str()).await?;
                    let pinned_stream: Pin<Box<CopyOutStream>> = Box::pin(copy_stream);

                    // Call Static Dispatch Parser
                    parse_data_with_schema(pinned_stream, worker_schema.clone()).await
                }.await;

                match result {
                    Ok((_rows, batch)) => {
                        worker_batches.push((task.index, batch));
                    }
                    Err(e) => {
                        // ERROR LOGGING
                        #[cfg(feature = "profiling")]
                        tracing::error!("Worker {}: Partition {} failed! Error: {}", worker_id, task.index, e);

                        // --- SELF-HEALING LOGIC ---
                        // If a partition fails (e.g. bad data), we log it and return NULLs
                        eprintln!("UncheckedIO Worker {}: Partition {} failed! Error: {}. Filling NULLs.", worker_id, task.index, e);
                        let null_batch = create_null_batch(worker_schema.clone(), task.expected_rows)?;
                        worker_batches.push((task.index, null_batch));
                    }
                }
            }

            // Return all batches processed by this worker
            Ok::<Vec<(usize, RecordBatch)>, anyhow::Error>(worker_batches)
        });
    }

    // --- PHASE 3: AGGREGATION ---
    let mut all_results: Vec<(usize, RecordBatch)> = Vec::with_capacity(total_partitions);

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(worker_result) => {
                match worker_result {
                    Ok(batches) => all_results.extend(batches),
                    Err(e) => return Err(anyhow!("Worker task failed internally: {}", e)),
                }
            }
            Err(e) => return Err(anyhow!("Worker task panic: {}", e)),
        }
    }

    #[cfg(feature = "profiling")]
    drop(_p2_guard); // End Phase 2 Span
    let duration_phase2 = start_phase2.elapsed();


    // --- PHASE 3: CONCATENATION AND FINALIZATION ---
    let start_phase3 = Instant::now();
    #[cfg(feature = "profiling")]
    let phase3_span = span!(Level::INFO, "Phase3_Concat");
    #[cfg(feature = "profiling")]
    let _p3_guard = phase3_span.enter();

    if all_results.is_empty() {
        println!("UncheckedIO: All workers returned empty batches.");
        let duration_total = start_total.elapsed();
        println!("--- UncheckedIO Internal Timing ---");
        println!("Phase 1 (Setup, Query): {:.2?}", duration_phase1);
        println!("Phase 2 (I/O, Parsing): {:.2?}", duration_phase2);
        println!("Phase 3 (Concatenation): {:.2?}", start_phase3.elapsed());
        println!("Total Wall Time: {:.2?}", duration_total);
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    // 1. Sort by index to restore original table order
    all_results.sort_by_key(|(index, _)| *index);

    // 2. Strip indices
    let batches: Vec<RecordBatch> = all_results.into_iter().map(|(_, batch)| batch).collect();

    // 3. Final Concatenation
    let final_batch = concat_batches(&arrow_schema, &batches)
        .context("Failed to stitch final batches")?;

    #[cfg(feature = "profiling")]
    drop(_p3_guard); // End Phase 3 Span
    let duration_phase3 = start_phase3.elapsed();
    let duration_total = start_total.elapsed();


    // --- FINAL REPORTING ---
    println!("--- UncheckedIO Internal Timing ---");
    println!("Phase 1 (Setup, Query): {:.2?}", duration_phase1);
    println!("Phase 2 (I/O, Parsing): {:.2?}", duration_phase2);
    println!("Phase 3 (Concatenation): {:.2?}", duration_phase3);
    println!("Total Wall Time: {:.2?}", duration_total);


    Ok(final_batch)
}

fn create_null_batch(schema: Arc<Schema>, num_rows: usize) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = schema.fields().iter().map(|field| {
        arrow::array::new_null_array(field.data_type(), num_rows)
    }).collect();
    RecordBatch::try_new(schema, columns).context("Failed to create null placeholder batch")
}

/// Helper function to build the Arrow Schema from the config
fn build_arrow_schema(config: &ConnectorConfig) -> Result<Schema> {
    let schema_fields: Vec<Field> = config.schema.iter().map(|col_cfg| {
        // FIX: Force all columns to be nullable for safety
        let nullable = true;

        let arrow_type = match col_cfg.arrow_type.as_str() {
            "Int64" => DataType::Int64,
            "Int32" => DataType::Int32,
            "Float64" => DataType::Float64,
            "Float32" => DataType::Float32,
            "Utf8" | "String" => DataType::Utf8,
            "Boolean" => DataType::Boolean,
            "Timestamp(Nanosecond, None)" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            "Date32" => DataType::Date32,
            _ => return Err(anyhow!("Unsupported type in config: {}", col_cfg.arrow_type)),
        };
        Ok(Field::new(&col_cfg.column_name, arrow_type, nullable))
    }).collect::<Result<Vec<Field>>>()?;

    Ok(Schema::new(schema_fields))
}


// --------------------------------------------------------------------------------
// --- 2. STATIC DISPATCH IMPLEMENTATION (The Fast Parser) ---
// --------------------------------------------------------------------------------

// Struct to hold the builders in a statically-known, fixed order (eliminates DynamicBuilder enum)
struct SchemaParser {
    id: Box<Int64Builder>,
    uuid: Box<StringBuilder>,
    username: Box<StringBuilder>,
    score: Box<Float32Builder>,
    is_active: Box<BooleanBuilder>,
    last_login: Box<TimestampNanosecondBuilder>,
    notes: Box<StringBuilder>,
    course_id: Box<Int32Builder>,
    start_date: Box<Date32Builder>,
    rating: Box<Float64Builder>,
}

// Helper to construct and parse data using the static SchemaParser
async fn parse_data_with_schema(
    stream: Pin<Box<CopyOutStream>>,
    arrow_schema: Arc<Schema>
) -> Result<(usize, RecordBatch)> {

    let mut parser = SchemaParser {
        id: Box::new(Int64Builder::new()),
        uuid: Box::new(StringBuilder::new()),
        username: Box::new(StringBuilder::new()),
        score: Box::new(Float32Builder::new()),
        is_active: Box::new(BooleanBuilder::new()),
        last_login: Box::new(TimestampNanosecondBuilder::new()),
        notes: Box::new(StringBuilder::new()),
        course_id: Box::new(Int32Builder::new()),
        start_date: Box::new(Date32Builder::new()),
        rating: Box::new(Float64Builder::new()),
    };

    let rows_processed = parse_binary_stream_static(stream, &mut parser).await?;

    // Collect all final arrays in the correct order (must match struct field order)
    let final_columns: Vec<ArrayRef> = vec![
        Arc::new(parser.id.finish()),
        Arc::new(parser.uuid.finish()),
        Arc::new(parser.username.finish()),
        Arc::new(parser.score.finish()),
        Arc::new(parser.is_active.finish()),
        Arc::new(parser.last_login.finish()),
        Arc::new(parser.notes.finish()),
        Arc::new(parser.course_id.finish()),
        Arc::new(parser.start_date.finish()),
        Arc::new(parser.rating.finish()),
    ];

    let record_batch = RecordBatch::try_new(
        arrow_schema,
        final_columns,
    ).context("Failed to create final Arrow RecordBatch")?;

    Ok((rows_processed, record_batch))
}

// The core streaming parser logic - INSTRUMENTED
async fn parse_binary_stream_static(
    mut stream: Pin<Box<CopyOutStream>>,
    parser: &mut SchemaParser,
) -> Result<usize> {

    let mut buffer = BytesMut::with_capacity(64 * 1024);
    let mut is_header_parsed: bool = false;
    let mut rows_processed: usize = 0;

    // NEW: Refactored loop to visualize Starvation vs Work
    'stream_loop: loop {

        // 1. MEASURE STARVATION (Waiting for Network)
        #[cfg(feature = "profiling")]
        let wait_span = span!(Level::ERROR, "IO_WAIT_STARVATION");
        #[cfg(feature = "profiling")]
        let guard = wait_span.enter();

        let next_item = stream.next().await;

        #[cfg(feature = "profiling")]
        drop(guard); // Important: Drop guard immediately when data arrives!

        match next_item {
            Some(segment_result) => {
                // 2. MEASURE WORK (CPU Parsing)
                #[cfg(feature = "profiling")]
                let work_span = span!(Level::INFO, "CPU_Parse_Chunk");
                #[cfg(feature = "profiling")]
                let _work_guard = work_span.enter();

                let segment: Bytes = segment_result.context("Error reading segment from CopyOutStream")?;
                buffer.extend_from_slice(&segment);

                if !is_header_parsed {
                    if buffer.len() < 19 { continue 'stream_loop; }
                    let mut header_cursor = Cursor::new(&buffer[..]);
                    parse_stream_header(&mut header_cursor).context("Failed to parse stream header")?;
                    buffer.advance(19);
                    is_header_parsed = true;
                }

                'parsing_loop: loop {
                    let mut cursor = Cursor::new(&buffer[..]);
                    let safe_position = cursor.position();

                    let col_count = match cursor.read_i16::<BigEndian>() {
                        Ok(count) => count,
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => { break 'parsing_loop; }
                        Err(e) => return Err(e.into()),
                    };

                    if col_count == -1 {
                        buffer.advance(2);
                        break 'stream_loop;
                    }

                    match parse_row_static(&mut cursor, parser, buffer.as_ref()) {
                        Ok(_) => {
                            rows_processed += 1;
                            let bytes_consumed = cursor.position();
                            buffer.advance(bytes_consumed as usize);
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            cursor.set_position(safe_position);
                            // Copy remaining bytes back to the buffer for the next chunk
                            let remaining_slice = &buffer.as_ref()[safe_position as usize..];
                            let mut leftover_buffer_vec = Vec::new();
                            leftover_buffer_vec.extend_from_slice(remaining_slice);
                            buffer.clear();
                            buffer.extend_from_slice(&leftover_buffer_vec);

                            break 'parsing_loop;
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }
            }
            None => break 'stream_loop, // End of stream
        }
    }

    if !buffer.is_empty() {
        return Err(anyhow!("Stream ended with leftover bytes ({}) but no trailer.", buffer.len()));
    }

    Ok(rows_processed)
}


fn parse_stream_header(cursor: &mut Cursor<&[u8]>) -> Result<()> {
    let mut magic_signature = [0u8; 11];
    cursor.read_exact(&mut magic_signature).context("Failed to read magic signature")?;
    if &magic_signature != b"PGCOPY\n\xff\r\n\0" {
        return Err(anyhow!("Invalid Postgres COPY binary signature."));
    }
    let _flags = cursor.read_u32::<BigEndian>().context("Failed to read flags")?;
    let _header_ext_len = cursor.read_u32::<BigEndian>().context("Failed to read header extension length")?;
    Ok(())
}

// --- STATIC DISPATCH ROW PARSER (The Key Speedup) ---
#[inline(always)]
fn parse_row_static(
    cursor: &mut Cursor<&[u8]>,
    p: &mut SchemaParser, // The concrete, statically-typed parser struct
    current_chunk: &[u8]
) -> Result<(), std::io::Error> {

    // Column 0: id (BIGINT)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.id.append_null() } else { p.id.append_value(cursor.read_i64::<BigEndian>()?) }

    // Column 1: uuid (TEXT)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.uuid.append_null() } else { read_string_field(cursor, p.uuid.as_mut(), current_chunk, len as usize)? }

    // Column 2: username (TEXT)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.username.append_null() } else { read_string_field(cursor, p.username.as_mut(), current_chunk, len as usize)? }

    // Column 3: score (REAL/Float32)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.score.append_null() } else { p.score.append_value(cursor.read_f32::<BigEndian>()?) }

    // Column 4: is_active (BOOLEAN)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.is_active.append_null() } else { p.is_active.append_value(cursor.read_u8()? != 0) }

    // Column 5: last_login (TIMESTAMP)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.last_login.append_null() } else {
        let pg_micros = cursor.read_i64::<BigEndian>()?;
        // Optimization: Constant offset applied
        let unix_micros = pg_micros + POSTGRES_EPOCH_MICROS_OFFSET;
        p.last_login.append_value(unix_micros * 1000);
    }

    // Column 6: notes (TEXT)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.notes.append_null() } else { read_string_field(cursor, p.notes.as_mut(), current_chunk, len as usize)? }

    // Column 7: course_id (INT)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.course_id.append_null() } else { p.course_id.append_value(cursor.read_i32::<BigEndian>()?) }

    // Column 8: start_date (DATE)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.start_date.append_null() } else {
        let pg_days = cursor.read_i32::<BigEndian>()?;
        // Optimization: 10957 days between 1970 and 2000
        p.start_date.append_value(pg_days + 10957);
    }

    // Column 9: rating (FLOAT8/Float64)
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.rating.append_null() } else { p.rating.append_value(cursor.read_f64::<BigEndian>()?) }

    Ok(())
}

// Helper function to consolidate zero-copy string reading and boundary checks
fn read_string_field(
    cursor: &mut Cursor<&[u8]>,
    builder: &mut StringBuilder,
    current_chunk: &[u8],
    field_len_usize: usize
) -> Result<(), std::io::Error> {

    if (cursor.position() as usize + field_len_usize) > current_chunk.len() {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Partial string field read"));
    }

    let start = cursor.position() as usize;
    let end = start + field_len_usize;
    let slice = &current_chunk[start..end];

    let val_str = str::from_utf8(slice)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    builder.append_value(val_str);
    cursor.set_position(end as u64); // Manually advance cursor

    Ok(())
}

// --------------------------------------------------------------------------------
// --- 3. PROFILER LOGIC (New Feature) ---
// --------------------------------------------------------------------------------

/// Maps a PostgreSQL internal type name to a standard Arrow Type string for config.yaml.
fn map_postgres_to_arrow_type(pg_type_name: &str) -> Option<&'static str> {
    match pg_type_name {
        "int8" | "bigint" | "serial8" => Some("Int64"),
        "int4" | "integer" | "serial" => Some("Int32"),
        "float8" | "double precision" => Some("Float64"),
        "float4" | "real" => Some("Float32"),
        "varchar" | "text" | "uuid" => Some("Utf8"),
        "bool" | "boolean" => Some("Boolean"),
        "timestamptz" | "timestamp" => Some("Timestamp(Nanosecond, None)"),
        "date" => Some("Date32"),
        _ => None, // Returns None for unsupported types (like JSON, arrays, etc.)
    }
}

pub async fn run_profiler_logic(config_path: &str) -> Result<String> {
    // Phase 1: Load config to get connection string and query
    let config: ConnectorConfig = load_and_validate_config(config_path)
        .context("Failed to load and validate config for profiling")?;

    // Use a non-COPY query to get metadata
    let (base_query, _) = config.query.trim().split_once("TO STDOUT (FORMAT binary)")
        .context("Query in config is malformed or not a COPY command")?;

    // We only need the base query for the metadata query
    let base_query_inner = base_query.trim().trim_start_matches("COPY (").trim_end_matches(")");

    // Construct the metadata query (limit 0 is fastest)
    let metadata_query = format!("SELECT * FROM ({}) AS subquery LIMIT 0", base_query_inner);

    // Phase 2: Connect and execute the query
    let pg_config: PgConfig = PgConfig::from_str(&config.connection_string)?;
    let (client, connection) = pg_config.connect(NoTls).await
        .context("Profiler: Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await { eprintln!("Profiler connection error: {}", e); }
    });

    let statement = client.prepare(&metadata_query).await
        .context("Profiler: Failed to prepare metadata query")?;

    let mut output = String::from("schema:\n");

    // Phase 3: Inspect the statement's columns for metadata
    for column in statement.columns() {
        let pg_type_name = column.type_().name().to_lowercase();
        let arrow_type = map_postgres_to_arrow_type(&pg_type_name)
            .unwrap_or("UNKNOWN (Review Manually)");

        let column_entry = format!(
            "- arrow_type: {}\n  column_name: {}\n",
            arrow_type,
            column.name()
        );
        output.push_str(&column_entry);
    }

    // Final instructions for the user
    output.push_str("\n# NOTE: Paste the 'schema' block above into your config.yaml\n");
    output.push_str(
        "# REVIEW any UNKNOWN types. PostgreSQL types: (int8, float8, text, bool, timestamp, date, etc.)\n"
    );

    Ok(output)
}
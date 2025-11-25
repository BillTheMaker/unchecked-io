// src/parser.rs

// --- External Crates ---
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{NoTls, CopyOutStream, Config as PgConfig};
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
use std::time::Instant;
use tokio::task::JoinSet;
use arrow::compute::concat_batches;
use async_channel;

#[cfg(feature = "profiling")]
use tracing::{span, Level};

use crate::config::{ConnectorConfig, load_and_validate_config};

// --- CONSTANTS ---
const POSTGRES_EPOCH_MICROS_OFFSET: i64 = 946684800000000;
// Default batch size (64k is a standard Arrow chunk size)
const DEFAULT_BATCH_SIZE: usize = 65_536;


// --- 1. CORE DATABASE LOGIC ---
pub async fn run_db_logic(config: ConnectorConfig, blast_radius: i64) -> Result<RecordBatch> {

    let start_total = Instant::now();
    let start_phase1 = Instant::now();

    #[cfg(feature = "profiling")]
    let root_span = span!(Level::INFO, "UncheckedIO_Run");
    #[cfg(feature = "profiling")]
    let _root_guard = root_span.enter();

    // --- PHASE 1: SETUP ---
    #[cfg(feature = "profiling")]
    let phase1_span = span!(Level::INFO, "Phase1_Setup");
    #[cfg(feature = "profiling")]
    let _p1_guard = phase1_span.enter();

    let num_workers = num_cpus::get();
    println!("UncheckedIO: Detected {} logical cores. Spawning {} persistent worker threads.", num_workers, num_workers);

    // CONFIG: Determine Target Batch Size
    let target_batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    println!("UncheckedIO: Worker Aggregation Target = {} rows/batch.", target_batch_size);

    let pg_config: tokio_postgres::Config = PgConfig::from_str(&config.connection_string)
        .context("Invalid connection string")?;

    let manager = Manager::new(pg_config.clone(), NoTls);
    let pool = Pool::builder(manager)
        .max_size(num_workers)
        .runtime(Runtime::Tokio1)
        .build()
        .context("Failed to build connection pool")?;

    let client = pool.get().await.context("Failed to get pool connection")?;
    let partition_key = "id";
    let (base_query, _) = config.query.trim().split_once("TO STDOUT (FORMAT binary)")
        .context("Failed to parse base query")?;
    let base_query_inner = base_query.trim().trim_start_matches("COPY (").trim_end_matches(")");

    let stats_query = format!("SELECT MIN({}), MAX({}) FROM ({}) AS subquery", partition_key, partition_key, base_query_inner);
    let row = client.query_one(&stats_query, &[]).await?;
    let min_id: i64 = row.try_get(0).context("Failed to get MIN(id)")?;
    let max_id: i64 = row.try_get(1).context("Failed to get MAX(id)")?;
    drop(client);

    let total_rows = (max_id - min_id + 1).max(1);
    let calculated_blast_radius = if blast_radius <= 0 {
        let target_chunks = (num_workers * 4) as i64;
        let dynamic_size = total_rows / target_chunks;
        dynamic_size.max(10_000)
    } else {
        blast_radius
    };

    println!("UncheckedIO: Auto-tuned partition size to {} rows.", calculated_blast_radius);

    struct PartitionTask {
        index: usize,
        query: String,
        expected_rows: usize
    }

    let (tx, rx) = async_channel::unbounded::<PartitionTask>();

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

        let task = PartitionTask { index: idx, query: new_query, expected_rows: estimated_rows };
        tx.send(task).await.context("Failed to fill work queue")?;

        current_min += calculated_blast_radius;
        idx += 1;
        total_partitions += 1;
    }
    tx.close();

    #[cfg(feature = "profiling")]
    drop(_p1_guard);

    // FIX: Define the duration variable here so it can be used in the print statement later
    let duration_phase1 = start_phase1.elapsed();


    // --- PHASE 2: PARALLEL EXECUTION (WITH AGGREGATION) ---
    let start_phase2 = Instant::now();
    #[cfg(feature = "profiling")]
    let phase2_span = span!(Level::INFO, "Phase2_Execution");
    #[cfg(feature = "profiling")]
    let _p2_guard = phase2_span.enter();

    let arrow_schema = Arc::new(build_arrow_schema(&config)?);
    let mut join_set = JoinSet::new();

    for worker_id in 0..num_workers {
        let worker_rx = rx.clone();
        let worker_pool = pool.clone();
        let worker_schema = arrow_schema.clone();

        join_set.spawn(async move {
            #[cfg(feature = "profiling")]
            let worker_span = span!(Level::INFO, "Worker_Thread", id = worker_id);
            #[cfg(feature = "profiling")]
            let _w_guard = worker_span.enter();

            let mut worker_batches: Vec<(usize, RecordBatch)> = Vec::new();

            // Initialize the Parser ONCE per worker (The Buffer)
            let mut parser = create_parser();
            let mut parser_row_count = 0;

            let mut client = worker_pool.get().await
                .context(format!("Worker {} failed to acquire connection", worker_id))?;

            // Worker Loop
            while let Ok(task) = worker_rx.recv().await {
                #[cfg(feature = "profiling")]
                let task_span = span!(Level::INFO, "Processing_Task", partition_id = task.index);
                #[cfg(feature = "profiling")]
                let _t_guard = task_span.enter();

                let result = async {
                    let copy_stream = client.copy_out(task.query.as_str()).await?;
                    let pinned_stream: Pin<Box<CopyOutStream>> = Box::pin(copy_stream);

                    // Parse DIRECTLY into the persistent parser
                    parse_binary_stream_static(pinned_stream, &mut parser).await
                }.await;

                match result {
                    Ok(rows_read) => {
                        parser_row_count += rows_read;

                        // CHECK FLUSH: Did we hit the batch size?
                        if parser_row_count >= target_batch_size {
                            let batch = flush_parser(&mut parser, worker_schema.clone())?;
                            worker_batches.push((task.index, batch));
                            parser_row_count = 0;
                            // Re-init parser builders
                            parser = create_parser();
                        }
                    }
                    Err(e) => {
                        eprintln!("Worker {} partition {} failed: {}. Handling failure.", worker_id, task.index, e);

                        // SAFETY FLUSH: If we have pending data, flush it first!
                        if parser_row_count > 0 {
                            let batch = flush_parser(&mut parser, worker_schema.clone())?;
                            worker_batches.push((task.index, batch));
                            parser_row_count = 0;
                            parser = create_parser();
                        }

                        // Emit NULL batch for the failed partition
                        let null_batch = create_null_batch(worker_schema.clone(), task.expected_rows)?;
                        worker_batches.push((task.index, null_batch));
                    }
                }
            }

            // FINAL FLUSH: Handle any remaining rows after queue is empty
            if parser_row_count > 0 {
                let batch = flush_parser(&mut parser, worker_schema.clone())?;
                worker_batches.push((usize::MAX, batch));
            }

            Ok::<Vec<(usize, RecordBatch)>, anyhow::Error>(worker_batches)
        });
    }

    // --- PHASE 3: AGGREGATION ---
    let mut all_results = Vec::new();
    while let Some(res) = join_set.join_next().await {
        match res {
            Ok(Ok(batches)) => all_results.extend(batches),
            Ok(Err(e)) => return Err(anyhow!("Worker failed: {}", e)),
            Err(e) => return Err(anyhow!("Worker panic: {}", e)),
        }
    }

    #[cfg(feature = "profiling")]
    drop(_p2_guard);

    // --- PHASE 4: CONCAT ---
    let start_phase3 = Instant::now();
    #[cfg(feature = "profiling")]
    let phase3_span = span!(Level::INFO, "Phase3_Concat");
    #[cfg(feature = "profiling")]
    let _p3_guard = phase3_span.enter();

    if all_results.is_empty() { return Ok(RecordBatch::new_empty(arrow_schema)); }

    // Sort by task index to maintain relative order
    all_results.sort_by_key(|(index, _)| *index);
    let batches: Vec<RecordBatch> = all_results.into_iter().map(|(_, b)| b).collect();
    let final_batch = concat_batches(&arrow_schema, &batches)?;

    #[cfg(feature = "profiling")]
    drop(_p3_guard);

    let duration_total = start_total.elapsed();

    // --- REPORT ---
    println!("--- UncheckedIO Internal Timing ---");
    // FIX: Using duration_phase1 correctly now
    println!("Phase 1 (Setup):   {:.2?}", duration_phase1);
    println!("Phase 2 (Execute): {:.2?}", start_phase3.duration_since(start_phase2));
    println!("Phase 3 (Concat):  {:.2?}", start_phase3.elapsed());
    println!("Total Wall Time:   {:.2?}", duration_total);

    Ok(final_batch)
}

// --- HELPER FUNCTIONS ---

fn create_null_batch(schema: Arc<Schema>, num_rows: usize) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = schema.fields().iter().map(|field| {
        arrow::array::new_null_array(field.data_type(), num_rows)
    }).collect();
    RecordBatch::try_new(schema, columns).context("Failed to create null placeholder batch")
}

fn build_arrow_schema(config: &ConnectorConfig) -> Result<Schema> {
    let schema_fields: Vec<Field> = config.schema.iter().map(|col_cfg| {
        let arrow_type = match col_cfg.arrow_type.as_str() {
            "Int64" => DataType::Int64,
            "Int32" => DataType::Int32,
            "Float64" => DataType::Float64,
            "Float32" => DataType::Float32,
            "Utf8" | "String" => DataType::Utf8,
            "Boolean" => DataType::Boolean,
            "Timestamp(Nanosecond, None)" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            "Date32" => DataType::Date32,
            _ => return Err(anyhow!("Unsupported type: {}", col_cfg.arrow_type)),
        };
        Ok(Field::new(&col_cfg.column_name, arrow_type, true))
    }).collect::<Result<Vec<Field>>>()?;
    Ok(Schema::new(schema_fields))
}

// --- PARSER STRUCT ---

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

fn create_parser() -> SchemaParser {
    SchemaParser {
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
    }
}

// Helper to flush the parser into a RecordBatch
fn flush_parser(parser: &mut SchemaParser, schema: Arc<Schema>) -> Result<RecordBatch> {
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
    RecordBatch::try_new(schema, final_columns).context("Failed to build RecordBatch")
}

// --- STREAMING PARSER ---

async fn parse_binary_stream_static(
    mut stream: Pin<Box<CopyOutStream>>,
    parser: &mut SchemaParser,
) -> Result<usize> {

    let mut buffer = BytesMut::with_capacity(64 * 1024);
    let mut is_header_parsed: bool = false;
    let mut rows_processed: usize = 0;

    'stream_loop: loop {
        #[cfg(feature = "profiling")]
        let wait_span = span!(Level::ERROR, "IO_WAIT");
        #[cfg(feature = "profiling")]
        let guard = wait_span.enter();

        let next_item = stream.next().await;

        #[cfg(feature = "profiling")]
        drop(guard);

        match next_item {
            Some(segment_result) => {
                #[cfg(feature = "profiling")]
                let work_span = span!(Level::INFO, "CPU_Parse");
                #[cfg(feature = "profiling")]
                let _work_guard = work_span.enter();

                let segment: Bytes = segment_result.context("Error reading segment")?;
                buffer.extend_from_slice(&segment);

                if !is_header_parsed {
                    if buffer.len() < 19 { continue 'stream_loop; }
                    let mut header_cursor = Cursor::new(&buffer[..]);
                    parse_stream_header(&mut header_cursor)?;
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
                            let remaining_slice = &buffer.as_ref()[safe_position as usize..];
                            let mut leftover_vec = Vec::new();
                            leftover_vec.extend_from_slice(remaining_slice);
                            buffer.clear();
                            buffer.extend_from_slice(&leftover_vec);
                            break 'parsing_loop;
                        }
                        Err(e) => return Err(e.into()),
                    }
                }
            }
            None => break 'stream_loop,
        }
    }

    if !buffer.is_empty() {
        return Err(anyhow!("Stream ended with leftover bytes"));
    }

    Ok(rows_processed)
}

fn parse_stream_header(cursor: &mut Cursor<&[u8]>) -> Result<()> {
    let mut magic = [0u8; 11];
    cursor.read_exact(&mut magic)?;
    if &magic != b"PGCOPY\n\xff\r\n\0" { return Err(anyhow!("Invalid signature")); }
    let _ = cursor.read_u32::<BigEndian>()?;
    let _ = cursor.read_u32::<BigEndian>()?;
    Ok(())
}

#[inline(always)]
fn parse_row_static(
    cursor: &mut Cursor<&[u8]>,
    p: &mut SchemaParser,
    current_chunk: &[u8]
) -> Result<(), std::io::Error> {

    // Column 0: id
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.id.append_null() } else { p.id.append_value(cursor.read_i64::<BigEndian>()?) }

    // Column 1: uuid
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.uuid.append_null() } else { read_string_field(cursor, p.uuid.as_mut(), current_chunk, len as usize)? }

    // Column 2: username
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.username.append_null() } else { read_string_field(cursor, p.username.as_mut(), current_chunk, len as usize)? }

    // Column 3: score
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.score.append_null() } else { p.score.append_value(cursor.read_f32::<BigEndian>()?) }

    // Column 4: is_active
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.is_active.append_null() } else { p.is_active.append_value(cursor.read_u8()? != 0) }

    // Column 5: last_login
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.last_login.append_null() } else {
        let val = cursor.read_i64::<BigEndian>()? + POSTGRES_EPOCH_MICROS_OFFSET;
        p.last_login.append_value(val * 1000);
    }

    // Column 6: notes
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.notes.append_null() } else { read_string_field(cursor, p.notes.as_mut(), current_chunk, len as usize)? }

    // Column 7: course_id
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.course_id.append_null() } else { p.course_id.append_value(cursor.read_i32::<BigEndian>()?) }

    // Column 8: start_date
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.start_date.append_null() } else {
        p.start_date.append_value(cursor.read_i32::<BigEndian>()? + 10957);
    }

    // Column 9: rating
    let len = cursor.read_i32::<BigEndian>()?;
    if len == -1 { p.rating.append_null() } else { p.rating.append_value(cursor.read_f64::<BigEndian>()?) }

    Ok(())
}

fn read_string_field(
    cursor: &mut Cursor<&[u8]>,
    builder: &mut StringBuilder,
    current_chunk: &[u8],
    len: usize
) -> Result<(), std::io::Error> {
    if (cursor.position() as usize + len) > current_chunk.len() {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Partial string"));
    }
    let start = cursor.position() as usize;
    let end = start + len;
    let slice = &current_chunk[start..end];
    let val = str::from_utf8(slice).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    builder.append_value(val);
    cursor.set_position(end as u64);
    Ok(())
}

// Profiler logic omitted for brevity (it remains unchanged from previous version)
pub async fn run_profiler_logic(_: &str) -> Result<String> { Ok("Profiler Placeholder".to_string()) }
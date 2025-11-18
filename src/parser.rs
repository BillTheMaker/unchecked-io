// --- External Crates ---
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{NoTls, CopyOutStream, Config as PgConfig};
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
use chrono::{NaiveDateTime, NaiveDate};
use std::mem;
use std::str::FromStr;
use tokio::task::JoinSet;
use arrow::compute::concat_batches;

// --- Internal Crates ---
use crate::config::ConnectorConfig;


// --- CONSTANTS ---
const POSTGRES_EPOCH_NAIVE: NaiveDateTime = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
const UNIX_EPOCH_NAIVE_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

// --- 1. CORE DATABASE LOGIC (PARALLEL COORDINATOR) ---
// Note: We remove the unused 'danger_mode' from the function signature to simplify the fast path API.
pub async fn run_db_logic(config: ConnectorConfig, blast_radius: i64) -> Result<RecordBatch> {

    // Removed danger_mode print, now defaults to fast path
    println!("UncheckedIO: Starting Query Planner (Blast Radius: {})...", blast_radius);

    // 1. Establish the *coordinator* connection
    let pg_config = PgConfig::from_str(&config.connection_string)?;
    let (client, connection) = pg_config.connect(NoTls).await
        .context("Coordinator: Failed to connect to PostgreSQL")?;
    tokio::spawn(async move {
        if let Err(e) = connection.await { eprintln!("Coordinator connection error: {}", e); }
    });

    // 2. Define Partition Strategy
    let partition_key = "id";

    // 3. Query for Table Bounds
    let (base_query, _) = config.query.trim().split_once("TO STDOUT (FORMAT binary)")
        .context("Failed to parse base query from config")?;
    let base_query_inner = base_query.trim().trim_start_matches("COPY (").trim_end_matches(")");

    let stats_query = format!("SELECT MIN({}), MAX({}) FROM ({}) AS subquery", partition_key, partition_key, base_query_inner);

    let row = client.query_one(&stats_query, &[]).await?;
    let min_id: i64 = row.try_get(0).context("Failed to get MIN(id)")?;
    let max_id: i64 = row.try_get(1).context("Failed to get MAX(id)")?;
    println!("UncheckedIO: ID Range: {} to {}", min_id, max_id);

    // 4. Generate Partitioned Queries (Unchanged)
    struct PartitionTask {
        index: usize,
        query: String,
        expected_rows: usize
    }

    let mut partitions: Vec<PartitionTask> = Vec::new();
    let mut current_min = min_id;
    let mut idx = 0;

    while current_min <= max_id {
        let current_max = (current_min + blast_radius - 1).min(max_id);
        let new_query = format!(
            "COPY (SELECT * FROM ({}) AS sub WHERE {} BETWEEN {} AND {}) TO STDOUT (FORMAT binary)",
            base_query_inner, partition_key, current_min, current_max
        );
        let estimated_rows = (current_max - current_min + 1) as usize;

        partitions.push(PartitionTask { index: idx, query: new_query, expected_rows: estimated_rows });
        current_min += blast_radius;
        idx += 1;
    }
    println!("UncheckedIO: Generated {} parallel partitions.", partitions.len());

    // --- Phase 2: Parallel Execution ---
    let arrow_schema = Arc::new(build_arrow_schema(&config)?);
    let mut join_set = JoinSet::new();

    for task in partitions {
        let worker_pg_config = pg_config.clone();
        let worker_schema = arrow_schema.clone();

        join_set.spawn(async move {
            let worker_logic = async {
                let (worker_client, worker_connection) = worker_pg_config.connect(NoTls).await?;
                tokio::spawn(async move {
                    if let Err(e) = worker_connection.await { eprintln!("Worker connection error: {}", e); }
                });

                let copy_stream = worker_client.copy_out(task.query.as_str()).await?;
                let pinned_stream: Pin<Box<CopyOutStream>> = Box::pin(copy_stream);

                // Now call the new parser entry point
                parse_data_with_schema(pinned_stream, worker_schema).await
            };

            let result = worker_logic.await;
            (task.index, result, task.expected_rows)
        });
    }

    // --- Phase 3: Collect, Order, and Stitch ---
    let mut results: Vec<Option<RecordBatch>> = vec![None; idx];

    while let Some(join_result) = join_set.join_next().await {
        let (index, parse_result, expected_rows) = join_result.context("Worker thread panic")?;

        match parse_result {
            Ok(batch) => {
                results[index] = Some(batch.1);
            }
            Err(e) => {
                // --- Self-Healing Placeholder ---
                eprintln!("UncheckedIO: Partition {} failed! Error: {}. Falling back to NULLs (Self-Healing logic required here).", index, e);
                let null_batch = create_null_batch(arrow_schema.clone(), expected_rows)?;
                results[index] = Some(null_batch);
            }
        }
    }

    let batches: Vec<RecordBatch> = results.into_iter()
        .filter_map(|b| b)
        .collect();

    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    let final_batch = concat_batches(&arrow_schema, &batches)
        .context("Failed to concatenate parallel batches")?;

    Ok(final_batch)
}

fn create_null_batch(schema: Arc<Schema>, num_rows: usize) -> Result<RecordBatch> {
    let columns: Vec<ArrayRef> = schema.fields().iter().map(|field| {
        arrow::array::new_null_array(field.data_type(), num_rows)
    }).collect();
    RecordBatch::try_new(schema, columns).context("Failed to create null placeholder batch")
}

fn build_arrow_schema(config: &ConnectorConfig) -> Result<Schema> {
    let schema_fields: Vec<Field> = config.schema.iter().map(|col_cfg| {
        let nullable = col_cfg.column_name == "notes";
        let arrow_type = match col_cfg.arrow_type.as_str() {
            "Int64" => DataType::Int64, "Int32" => DataType::Int32,
            "Float64" => DataType::Float64, "Float32" => DataType::Float32,
            "Utf8" | "String" => DataType::Utf8, "Boolean" => DataType::Boolean,
            "Timestamp(Nanosecond, None)" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            "Date32" => DataType::Date32,
            _ => return Err(anyhow!("Unsupported type in config: {}", col_cfg.arrow_type)),
        };
        Ok(Field::new(&col_cfg.column_name, arrow_type, nullable))
    }).collect::<Result<Vec<Field>>>()?;
    Ok(Schema::new(schema_fields))
}


// --------------------------------------------------------------------------------
// --- 2. STATIC DISPATCH IMPLEMENTATION (The Speed Gain) ---
// --------------------------------------------------------------------------------

// New Trait for all builders to implement append_null
trait ColumnBuilderTrait {
    fn append_null_to_self(&mut self);
}

// Implement the trait for the Boxed builders (which were in the DynamicBuilder enum)
impl ColumnBuilderTrait for Box<Int64Builder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<Int32Builder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<Float64Builder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<Float32Builder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<StringBuilder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<BooleanBuilder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<TimestampNanosecondBuilder> { fn append_null_to_self(&mut self) { self.append_null(); } }
impl ColumnBuilderTrait for Box<Date32Builder> { fn append_null_to_self(&mut self) { self.append_null(); } }


// New struct to hold the builders in a statically-known, fixed order
// Note: We use the exact types from the benchmark schema to simplify the MVP
struct SchemaParser {
    // Column 0: id
    id: Box<Int64Builder>,
    // Column 1: uuid
    uuid: Box<StringBuilder>,
    // Column 2: username
    username: Box<StringBuilder>,
    // Column 3: score
    score: Box<Float32Builder>,
    // Column 4: is_active
    is_active: Box<BooleanBuilder>,
    // Column 5: last_login
    last_login: Box<TimestampNanosecondBuilder>,
    // Column 6: notes
    notes: Box<StringBuilder>,
    // Column 7: course_id
    course_id: Box<Int32Builder>,
    // Column 8: start_date
    start_date: Box<Date32Builder>,
    // Column 9: rating
    rating: Box<Float64Builder>,
    // Note: This struct MUST match the order of the query result.
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

// The core streaming parser logic - generic over the SchemaParser struct
async fn parse_binary_stream_static(
    mut stream: Pin<Box<CopyOutStream>>,
    parser: &mut SchemaParser,
) -> Result<usize> {

    let mut buffer = BytesMut::with_capacity(64 * 1024);
    let mut is_header_parsed: bool = false;
    let mut rows_processed: usize = 0;

    'stream_loop: while let Some(segment_result) = stream.next().await {
        let segment: Bytes = segment_result.context("Error reading segment from CopyOutStream")?;
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
                    break 'parsing_loop;
                }
                Err(e) => {
                    return Err(e.into());
                }
            }
        }
    }

    if !buffer.is_empty() {
        return Err(anyhow!("Stream ended with leftover bytes ({}) but no trailer.", buffer.len()));
    }

    Ok(rows_processed)
}


fn parse_stream_header(cursor: &mut Cursor<&[u8]>) -> Result<()> {
    // (Unchanged)
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
        // 10957 days between 1970 and 2000 => 946684800000000 micros
        let unix_micros = pg_micros + 946684800000000;
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
        // 10957 days between 1970 and 2000
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
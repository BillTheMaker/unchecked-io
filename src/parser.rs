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
use std::str; // Ensure this is imported for zero-copy parsing
use chrono::{NaiveDateTime, NaiveDate};
use std::mem;
use std::str::FromStr;
use tokio::task::JoinSet;
use arrow::compute::concat_batches;

// --- Internal Crates ---
use crate::config::ConnectorConfig;


// --- 1. CORE DATABASE LOGIC (PARALLEL COORDINATOR) ---
// (This section is unchanged from your previous correct version)
pub async fn run_db_logic(config: ConnectorConfig, blast_radius: i64) -> Result<RecordBatch> {

    println!("UncheckedIO: Starting Query Planner (Blast Radius: {} rows)...", blast_radius);

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

    // 4. Generate Partitioned Queries
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
        // Estimate rows
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

                parse_binary_stream(pinned_stream, worker_schema).await
            };

            let result = worker_logic.await;
            (task.index, result, task.expected_rows)
        });
    }

    // --- Phase 3: Collect, Order, and Stitch ---
    let mut results: Vec<Option<RecordBatch>> = vec![None; idx];

    while let Some(join_result) = join_set.join_next().await {
        // NOTE: In production, handle JoinError better than .context() panic
        let (index, parse_result, expected_rows) = join_result.context("Worker thread panic")?;

        match parse_result {
            Ok(batch) => {
                results[index] = Some(batch.1);
            }
            Err(e) => {
                eprintln!("UncheckedIO: Partition {} failed! Error: {}. Filling with NULLs.", index, e);
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


// --- 2. INTERNAL PARSER IMPLEMENTATION ---

enum DynamicBuilder {
    Int64(Box<Int64Builder>),
    Int32(Box<Int32Builder>),
    Float64(Box<Float64Builder>),
    Float32(Box<Float32Builder>),
    String(Box<StringBuilder>),
    Boolean(Box<BooleanBuilder>),
    Timestamp(Box<TimestampNanosecondBuilder>),
    Date32(Box<Date32Builder>),
}

const POSTGRES_EPOCH_NAIVE: NaiveDateTime = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
const UNIX_EPOCH_NAIVE_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

async fn parse_binary_stream(
    mut stream: Pin<Box<CopyOutStream>>,
    arrow_schema: Arc<Schema>
) -> Result<(usize, RecordBatch)> {

    let mut builders: Vec<DynamicBuilder> = arrow_schema.fields().iter().map(|field| {
        match field.data_type() {
            DataType::Int64 => DynamicBuilder::Int64(Box::new(Int64Builder::new())),
            DataType::Int32 => DynamicBuilder::Int32(Box::new(Int32Builder::new())),
            DataType::Float64 => DynamicBuilder::Float64(Box::new(Float64Builder::new())),
            DataType::Float32 => DynamicBuilder::Float32(Box::new(Float32Builder::new())),
            DataType::Utf8 => DynamicBuilder::String(Box::new(StringBuilder::new())),
            DataType::Boolean => DynamicBuilder::Boolean(Box::new(BooleanBuilder::new())),
            DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None) => {
                DynamicBuilder::Timestamp(Box::new(TimestampNanosecondBuilder::new()))
            },
            DataType::Date32 => DynamicBuilder::Date32(Box::new(Date32Builder::new())),
            _ => panic!("Unsupported type in builder creation!"),
        }
    }).collect();

    let mut buffer = BytesMut::with_capacity(64 * 1024);
    let mut is_header_parsed: bool = false;
    let mut rows_processed: usize = 0;

    'stream_loop: while let Some(segment_result) = stream.next().await {
        let segment: Bytes = segment_result.context("Error reading segment from CopyOutStream")?;
        buffer.extend_from_slice(&segment);

        if !is_header_parsed {
            if buffer.len() < 19 {
                continue 'stream_loop;
            }
            let mut header_cursor = Cursor::new(&buffer[..]);
            parse_stream_header(&mut header_cursor)?;
            buffer.advance(19);
            is_header_parsed = true;
        }

        'parsing_loop: loop {
            // NOTE: We create a cursor over the *entire* remaining buffer slice
            let mut cursor = Cursor::new(&buffer[..]);

            let col_count = match cursor.read_i16::<BigEndian>() {
                Ok(count) => count,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break 'parsing_loop;
                }
                Err(e) => return Err(e.into()),
            };

            if col_count == -1 {
                buffer.advance(2);
                break 'stream_loop;
            }

            // Pass the slice `buffer.as_ref()` explicitly for zero-copy logic
            match parse_row(&mut cursor, &mut builders, buffer.as_ref()) {
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

    let final_columns: Vec<ArrayRef> = builders.into_iter().map(|builder| {
        match builder {
            DynamicBuilder::Int64(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::Int32(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::Float64(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::Float32(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::String(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::Boolean(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::Timestamp(mut b) => Arc::new(b.finish()) as ArrayRef,
            DynamicBuilder::Date32(mut b) => Arc::new(b.finish()) as ArrayRef,
        }
    }).collect();

    let record_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        final_columns,
    ).context("Failed to create final Arrow RecordBatch")?;

    Ok((rows_processed, record_batch))
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

fn parse_row(
    cursor: &mut Cursor<&[u8]>,
    builders: &mut [DynamicBuilder],
    current_chunk: &[u8]
) -> Result<(), std::io::Error> {

    for builder in builders.iter_mut() {
        let field_len_i32 = cursor.read_i32::<BigEndian>()?;

        if field_len_i32 == -1 {
            match builder {
                DynamicBuilder::Int64(b) => b.append_null(),
                DynamicBuilder::Int32(b) => b.append_null(),
                DynamicBuilder::Float64(b) => b.append_null(),
                DynamicBuilder::Float32(b) => b.append_null(),
                DynamicBuilder::String(b) => b.append_null(),
                DynamicBuilder::Boolean(b) => b.append_null(),
                DynamicBuilder::Timestamp(b) => b.append_null(),
                DynamicBuilder::Date32(b) => b.append_null(),
            }
            continue;
        }

        let field_len_usize = field_len_i32 as usize;

        if (cursor.position() as usize + field_len_usize) > current_chunk.len() {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Partial field read"));
        }

        match builder {
            DynamicBuilder::Int64(b) => {
                let val = cursor.read_i64::<BigEndian>()?;
                b.append_value(val);
            }
            DynamicBuilder::Int32(b) => {
                let val = cursor.read_i32::<BigEndian>()?;
                b.append_value(val);
            }
            DynamicBuilder::Float64(b) => {
                let val = cursor.read_f64::<BigEndian>()?;
                b.append_value(val);
            }
            DynamicBuilder::Float32(b) => {
                let val = cursor.read_f32::<BigEndian>()?;
                b.append_value(val);
            }
            // --- OPTIMIZATION START: ZERO-COPY STRING PARSING ---
            DynamicBuilder::String(b) => {
                // 1. Get current cursor position (start of string data)
                let start = cursor.position() as usize;
                let end = start + field_len_usize;

                // 2. Slice the bytes directly from current_chunk (Zero-Copy)
                // Safety: We already verified bounds check above.
                let slice = &current_chunk[start..end];

                // 3. Verify UTF-8 and append (still validates, but no allocation)
                let val_str = str::from_utf8(slice)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

                b.append_value(val_str);

                // 4. Manually advance cursor since we didn't use read_exact
                cursor.set_position(end as u64);
            }
            // --- OPTIMIZATION END ---
            DynamicBuilder::Boolean(b) => {
                let val_bool = cursor.read_u8()?;
                b.append_value(val_bool != 0);
            }
            DynamicBuilder::Timestamp(b) => {
                let pg_micros = cursor.read_i64::<BigEndian>()?;
                let unix_epoch = NaiveDateTime::from_timestamp_opt(0, 0).unwrap();
                let pg_epoch = POSTGRES_EPOCH_NAIVE;
                let epoch_delta_micros = (pg_epoch - unix_epoch).num_microseconds().unwrap();
                let unix_micros = epoch_delta_micros + pg_micros;
                let unix_nanos = unix_micros * 1000;
                b.append_value(unix_nanos);
            }
            DynamicBuilder::Date32(b) => {
                let pg_days = cursor.read_i32::<BigEndian>()?;
                let epoch_delta_days = (POSTGRES_EPOCH_NAIVE.date() - UNIX_EPOCH_NAIVE_DATE).num_days() as i32;
                let unix_days = epoch_delta_days + pg_days;
                b.append_value(unix_days);
            }
        }
    }
    Ok(())
}
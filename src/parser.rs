// --- External Crates ---
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{NoTls, CopyOutStream, Client, Config as PgConfig};
use anyhow::{Context, Result, anyhow};
use arrow::array::{
    ArrayBuilder, ArrayRef,
    Int64Builder, Float64Builder, Float32Builder, StringBuilder, BooleanBuilder,
    TimestampNanosecondBuilder, Date32Builder, Int32Builder
};
use arrow::datatypes::{
    DataType, Field, Schema,
    Float64Type, Float32Type, Int64Type, Int32Type, Utf8Type, BooleanType, TimestampNanosecondType,
    Date32Type
};
use arrow::record_batch::RecordBatch;
use futures_util::stream::StreamExt;
use bytes::Bytes;
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


// --- 1. CORE DATABASE LOGIC (PARALLEL COORDINATOR) ---
pub async fn run_db_logic(config: ConnectorConfig) -> Result<RecordBatch> {

    println!("UncheckedIO: Starting Query Planner...");

    // 1. Establish the *coordinator* connection
    let pg_config = PgConfig::from_str(&config.connection_string)?;

    let (client, connection) = pg_config.connect(NoTls).await
        .context("Coordinator: Failed to connect to PostgreSQL")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Coordinator connection error: {}", e);
        }
    });

    // 2. Define Partition Strategy
    let num_partitions = num_cpus::get().max(2);
    let partition_key = "id";

    // 3. Query for Table Bounds
    let (base_query, _) = config.query
        .trim()
        .split_once("TO STDOUT (FORMAT binary)")
        .context("Failed to parse base query from config")?;

    let base_query_inner = base_query.trim().trim_start_matches("COPY (").trim_end_matches(")");

    let stats_query = format!(
        "SELECT MIN({}), MAX({}), COUNT(*) FROM ({}) AS subquery",
        partition_key, partition_key, base_query_inner
    );

    println!("UncheckedIO: Running stats query: {}", stats_query);
    let row = client.query_one(&stats_query, &[]).await?;
    let min_id: i64 = row.try_get(0).context("Failed to get MIN(id)")?;
    let max_id: i64 = row.try_get(1).context("Failed to get MAX(id)")?;
    let count: i64 = row.try_get(2).context("Failed to get COUNT(*)")?;

    if count == 0 {
        println!("UncheckedIO: Table has no rows (COUNT=0). Returning empty batch.");
        let arrow_schema = build_arrow_schema(&config)?;
        // FIX 1: Wrap schema in Arc to satisfy SchemaRef
        return Ok(RecordBatch::new_empty(Arc::new(arrow_schema)));
    }

    let chunk_size = (count as f64 / num_partitions as f64).ceil() as i64;
    println!("UncheckedIO: Found {} rows. Creating {} partitions of ~{} rows each.", count, num_partitions, chunk_size);

    // 4. Generate Partitioned Queries
    let mut partition_queries: Vec<String> = Vec::new();
    for i in 0..num_partitions {
        let part_min = min_id + (i as i64 * chunk_size);
        let part_max = (part_min + chunk_size - 1).min(max_id);

        if part_min > max_id {
            break;
        }

        let new_query = format!(
            "COPY (SELECT * FROM ({}) AS sub WHERE {} BETWEEN {} AND {}) TO STDOUT (FORMAT binary)",
            base_query_inner,
            partition_key,
            part_min,
            part_max
        );
        partition_queries.push(new_query);
    }

    println!("UncheckedIO: Generated {} parallel queries.", partition_queries.len());

    // --- Phase 2: Parallel Execution ---

    let arrow_schema = Arc::new(build_arrow_schema(&config)?);
    let mut join_set = JoinSet::new();

    for query in partition_queries {
        let worker_pg_config = pg_config.clone();
        let worker_schema = arrow_schema.clone();

        join_set.spawn(async move {
            let (worker_client, worker_connection) = worker_pg_config.connect(NoTls).await?;

            tokio::spawn(async move {
                if let Err(e) = worker_connection.await {
                    eprintln!("Worker connection error: {}", e);
                }
            });

            let copy_stream = worker_client.copy_out(query.as_str()).await?;
            let pinned_stream: Pin<Box<CopyOutStream>> = Box::pin(copy_stream);

            let record_batch = parse_binary_stream(pinned_stream, worker_schema).await?;

            Ok::<_, anyhow::Error>(record_batch)
        });
    }

    // --- Phase 3: Collect and Concatenate ---
    let mut batches: Vec<RecordBatch> = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(batch_result) => {
                match batch_result {
                    Ok(batch) => {
                        if batch.num_rows() > 0 {
                            batches.push(batch);
                        }
                    },
                    Err(e) => return Err(anyhow!("A worker task failed: {}", e)),
                }
            },
            Err(e) => return Err(anyhow!("A tokio task failed to join: {}", e)),
        }
    }

    if batches.is_empty() {
        println!("UncheckedIO: All partitions returned empty. Returning empty batch.");
        return Ok(RecordBatch::new_empty(arrow_schema));
    }

    // FIX 2: Removed the '?' after concat_batches so .context() applies to the Result
    let final_batch = concat_batches(&batches[0].schema(), &batches)
        .context("Failed to concatenate parallel batches")?;

    Ok(final_batch)
}

/// Helper function to build the Arrow Schema from the config
fn build_arrow_schema(config: &ConnectorConfig) -> Result<Schema> {
    let schema_fields: Vec<Field> = config.schema.iter().map(|col_cfg| {
        let nullable = col_cfg.column_name == "notes"; // Hack for MVP

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
) -> Result<RecordBatch> {

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

    let mut leftover_buffer: Vec<u8> = Vec::new();
    let mut is_header_parsed: bool = false;
    let mut rows_processed: usize = 0;

    'stream_loop: while let Some(segment_result) = stream.next().await {
        let segment: Bytes = segment_result.context("Error reading segment from CopyOutStream")?;

        let mut current_chunk: Vec<u8> = mem::take(&mut leftover_buffer);
        current_chunk.extend_from_slice(&segment);

        let mut cursor = Cursor::new(&current_chunk[..]);

        if !is_header_parsed {
            if current_chunk.len() < 19 {
                leftover_buffer = current_chunk;
                continue 'stream_loop;
            }

            parse_stream_header(&mut cursor)?;
            is_header_parsed = true;
        }

        'parsing_loop: loop {
            let safe_position = cursor.position();

            let col_count = match cursor.read_i16::<BigEndian>() {
                Ok(count) => count,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    cursor.set_position(safe_position);
                    leftover_buffer.extend_from_slice(&current_chunk[safe_position as usize..]);
                    break 'parsing_loop;
                }
                Err(e) => return Err(e.into()),
            };

            if col_count == -1 {
                // println!("UncheckedIO: Worker reached end-of-stream trailer.");
                leftover_buffer.clear();
                break 'stream_loop;
            }

            match parse_row(&mut cursor, &mut builders, &current_chunk) {
                Ok(_) => {
                    rows_processed += 1;
                }
                // FIX 3: Error handling for std::io::Error
                // We check .kind() directly because 'e' is std::io::Error
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    cursor.set_position(safe_position);
                    leftover_buffer.extend_from_slice(&current_chunk[safe_position as usize..]);
                    break 'parsing_loop;
                }
                Err(e) => {
                    // Explicitly convert std::io::Error to anyhow::Error
                    return Err(e.into());
                }
            }
        }
    }

    if !leftover_buffer.is_empty() {
        return Err(anyhow!("Stream ended with leftover bytes ({}) but no trailer.", leftover_buffer.len()));
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

    Ok(record_batch)
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

    for (i, builder) in builders.iter_mut().enumerate() {
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
            DynamicBuilder::String(b) => {
                let mut str_buf = vec![0; field_len_usize];
                cursor.read_exact(&mut str_buf)?;
                let val_str = str::from_utf8(&str_buf)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                b.append_value(val_str);
            }
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
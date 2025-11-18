// --- External Crates ---
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{NoTls, CopyOutStream};
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
use std::mem; // <-- We need this for mem::take

// --- Internal Crates ---
use crate::config::ConnectorConfig;


// --- 1. CORE DATABASE LOGIC (Public API) ---
// This function remains unchanged.
pub async fn run_db_logic(config: ConnectorConfig) -> Result<RecordBatch> {
    // 1. Establish the connection
    println!("UncheckedIO: Attempting connection...");

    let (client, connection) = tokio_postgres::connect(&config.connection_string, NoTls).await
        .context("Failed to connect to PostgreSQL database")?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Postgres connection error: {}", e);
        }
    });

    // 2. Execute the COPY TO STDOUT command
    let copy_query = &config.query;
    println!("UncheckedIO: Executing user-defined query...");

    let copy_stream = client.copy_out(copy_query.as_str()).await
        .context("Failed to execute COPY TO STDOUT protocol. Check your query syntax and permissions.")?;

    // 3. Handle the Binary Stream
    let pinned_stream: Pin<Box<CopyOutStream>> = Box::pin(copy_stream);

    // Build the Arrow Schema from the config
    let schema_fields: Vec<Field> = config.schema.iter().map(|col_cfg| {
        let nullable = col_cfg.column_name == "notes";

        let arrow_type = match col_cfg.arrow_type.as_str() {
            "Int64" => DataType::Int64,
            "Int32" => DataType::Int32,
            "Float64" => DataType::Float64,
            "Float32" => DataType::Float32,
            "Utf8" | "String" => DataType::Utf8,
            "Boolean" => DataType::Boolean,
            "Timestamp(Nanosecond, None)" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            "Date32" => DataType::Date32,
            _ => panic!("Unsupported type in config: {}", col_cfg.arrow_type),
        };
        Field::new(&col_cfg.column_name, arrow_type, nullable)
    }).collect();
    let arrow_schema = Arc::new(Schema::new(schema_fields));

    // Call the parser
    // We pass the schema and get back the final RecordBatch
    let record_batch = parse_binary_stream(pinned_stream, arrow_schema.clone()).await?;

    println!("UncheckedIO: Successfully parsed {} rows via binary stream.", record_batch.num_rows());

    // 4. Final Output Confirmation
    println!("UncheckedIO: Built RecordBatch with {} rows and {} columns.",
             record_batch.num_rows(), record_batch.num_columns());

    println!("UncheckedIO: Data transfer complete. We lived.");

    Ok(record_batch)
}


// --- 2. INTERNAL PARSER IMPLEMENTATION ---
// All the complex logic is now contained in this private section.

// This enum will hold our different builder types
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

// Postgres Epoch for timestamps
const POSTGRES_EPOCH_NAIVE: NaiveDateTime = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
// Unix Epoch for dates
const UNIX_EPOCH_NAIVE_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

/// This is the refactored streaming state machine.
/// It reads the stream chunk by chunk and parses it.
async fn parse_binary_stream(
    mut stream: Pin<Box<CopyOutStream>>,
    arrow_schema: Arc<Schema>
) -> Result<RecordBatch> {

    // --- Phase 1: Task 1 (Initialize Builders) ---
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
            _ => panic!("Unsupported type in builder creation!"), // Will improve later
        }
    }).collect();

    // --- Phase 1: Task 2 (Initialize State) ---
    let mut leftover_buffer: Vec<u8> = Vec::new();
    let mut is_header_parsed: bool = false;
    let mut rows_processed: usize = 0;

    // --- Phase 2: Task 4 (Streaming Loop) ---
    'stream_loop: while let Some(segment_result) = stream.next().await {
        let segment: Bytes = segment_result.context("Error reading segment from CopyOutStream")?;

        // Combine leftover bytes from last chunk with the new chunk
        let mut current_chunk: Vec<u8> = mem::take(&mut leftover_buffer);
        current_chunk.extend_from_slice(&segment);

        // --- Phase 2: Task 5 (Create Cursor) ---
        let mut cursor = Cursor::new(&current_chunk[..]);

        // --- Phase 2: Task 6 (Handle Header) ---
        if !is_header_parsed {
            // Check if we have enough bytes for the header (11 + 4 + 4 = 19 bytes)
            if current_chunk.len() < 19 {
                // Not enough data. Move the chunk back and wait for more.
                leftover_buffer = current_chunk;
                continue 'stream_loop; // Get next segment
            }

            parse_stream_header(&mut cursor)?;
            is_header_parsed = true;
        }

        // --- Phase 3: Tasks 7-9 (Inner Parsing Loop) ---
        'parsing_loop: loop {
            // --- Task 8: Implement Safe Read (Row Level) ---
            // Save state before attempting to read a row's header
            let safe_position = cursor.position();

            // 1. Try to read the 2-byte row header (column count)
            let col_count = match cursor.read_i16::<BigEndian>() {
                Ok(count) => count,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // This is NOT an error. It's a partial read.
                    // We don't have enough data for a full row header.
                    // Rewind the cursor to the safe position...
                    cursor.set_position(safe_position);
                    // ... and save the remaining bytes for the next chunk.
                    leftover_buffer.extend_from_slice(&current_chunk[safe_position as usize..]);
                    // Break the *inner* loop to get the next network segment
                    break 'parsing_loop;
                }
                Err(e) => return Err(e.into()), // This is a real, unexpected error
            };

            // --- Task 10: Verify Stream Trailer ---
            if col_count == -1 {
                println!("UncheckedIO: Reached end-of-stream trailer.");
                leftover_buffer.clear(); // We are done, clear any remaining bytes
                break 'stream_loop; // Break the *outer* loop
            }

            // 3. Try to parse all fields for this row
            match parse_row(&mut cursor, &mut builders, &current_chunk) {
                Ok(_) => {
                    // Row was parsed successfully
                    rows_processed += 1;
                }
                // FIX: 'e' is already a std::io::Error. We just check its .kind() directly.
                // This resolves the E0599 (method not found) errors.
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // This was a partial row. The error is expected.
                    // Rewind to the start of the row.
                    cursor.set_position(safe_position);
                    leftover_buffer.extend_from_slice(&current_chunk[safe_position as usize..]);
                    break 'parsing_loop;
                }
                Err(e) => {
                    // This was a real, unexpected error.
                    // FIX: Convert the std::io::Error into an anyhow::Error.
                    // This resolves the E0308 (mismatched types) error.
                    return Err(e.into());
                }
            }
        } // End inner 'parsing_loop
    } // End outer 'stream_loop

    // --- Phase 1: Task 3 (Refactor Finalization) ---
    if !leftover_buffer.is_empty() {
        // We should have broken on the trailer. If we have leftovers, something is wrong.
        return Err(anyhow!("Stream ended with leftover bytes ({}) but no trailer. Data is corrupt.", leftover_buffer.len()));
    }

    // Finalize all the builders
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

    // Build the Final RecordBatch
    let record_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        final_columns,
    ).context("Failed to create final Arrow RecordBatch")?;

    Ok(record_batch)
}

/// Helper function to parse the 19-byte Postgres binary header.
/// This advances the cursor.
fn parse_stream_header(cursor: &mut Cursor<&[u8]>) -> Result<()> {
    let mut magic_signature = [0u8; 11];
    cursor.read_exact(&mut magic_signature).context("Failed to read magic signature")?;
    if &magic_signature != b"PGCOPY\n\xff\r\n\0" {
        return Err(anyhow!("Invalid Postgres COPY binary signature."));
    }
    let _flags = cursor.read_u32::<BigEndian>().context("Failed to read flags")?;
    let _header_ext_len = cursor.read_u32::<BigEndian>().context("Failed to read header extension length")?;

    println!("UncheckedIO: Postgres binary header validated.");
    Ok(())
}

/// Helper function to parse one full row of data from the cursor.
/// This function is designed to fail with an `UnexpectedEof` error if the row is partial,
/// allowing the outer loop to handle it.
fn parse_row(
    cursor: &mut Cursor<&[u8]>,
    builders: &mut [DynamicBuilder],
    current_chunk: &[u8] // Needed for partial string reads
) -> Result<(), std::io::Error> { // Returns a specific IO Error

    for (i, builder) in builders.iter_mut().enumerate() {
        // 1. Read field length (4 bytes)
        // This will propagate the UnexpectedEof error up if it fails
        let field_len_i32 = cursor.read_i32::<BigEndian>()?;

        if field_len_i32 == -1 {
            // Handle NULLs
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
            continue; // Go to the next field in this row
        }

        let field_len_usize = field_len_i32 as usize;

        // 2. Check if we have enough bytes in *this chunk* for the *entire field*
        // This is a "look-ahead" check *before* we consume bytes.
        if (cursor.position() as usize + field_len_usize) > current_chunk.len() {
            // Partial read: The field's data is split.
            // We return an EOF error to signal the outer loop.
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Partial field read"));
        }

        // 3. We have enough bytes. Parse it.
        // These reads will now succeed because we checked the length.
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
                // This str::from_utf8 is a potential panic! We should handle it.
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
    } // End field loop

    Ok(()) // Row was successfully parsed
}
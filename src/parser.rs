// --- External Crates ---
use std::pin::Pin;
use std::sync::Arc;
use tokio_postgres::{NoTls, CopyOutStream};
use anyhow::{Context, Result, anyhow};
// FIX: Import new builders
use arrow::array::{
    ArrayBuilder, ArrayRef,
    Int64Builder, Float64Builder, Float32Builder, StringBuilder, BooleanBuilder,
    TimestampNanosecondBuilder, Date32Builder, Int32Builder
};
// FIX: Import new types
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

// --- Internal Crates ---
use crate::config::ConnectorConfig;


// --- 1. CORE DATABASE LOGIC ---
pub async fn run_db_logic(config: ConnectorConfig) -> Result<()> {
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
    // FIX: We are now using the query from the config file!
    let copy_query = &config.query;
    println!("UncheckedIO: Executing user-defined query...");

    let copy_stream = client.copy_out(copy_query.as_str()).await
        .context("Failed to execute COPY TO STDOUT protocol. Check your query syntax and permissions.")?;

    // 3. Handle the Binary Stream
    let pinned_stream: Pin<Box<CopyOutStream>> = Box::pin(copy_stream);

    // Build the Arrow Schema from the config
    let schema_fields: Vec<Field> = config.schema.iter().map(|col_cfg| {
        // We now allow "nullable" to be controlled by the column name, a temporary "hack"
        let nullable = col_cfg.column_name == "notes";

        let arrow_type = match col_cfg.arrow_type.as_str() {
            "Int64" => DataType::Int64,
            "Int32" => DataType::Int32, // NEW
            "Float64" => DataType::Float64, // NEW
            "Float32" => DataType::Float32,
            "Utf8" | "String" => DataType::Utf8,
            "Boolean" => DataType::Boolean,
            "Timestamp(Nanosecond, None)" => DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
            "Date32" => DataType::Date32, // NEW
            _ => panic!("Unsupported type in config: {}", col_cfg.arrow_type),
        };
        Field::new(&col_cfg.column_name, arrow_type, nullable)
    }).collect();
    let arrow_schema = Arc::new(Schema::new(schema_fields));

    // Call the parser
    let (rows_processed, record_batch) = handle_binary_copy(pinned_stream, arrow_schema.clone()).await?;

    println!("UncheckedIO: Successfully parsed {} rows via binary stream.", rows_processed);

    // 4. Final Output Confirmation
    println!("UncheckedIO: Built RecordBatch with {} rows and {} columns.",
             record_batch.num_rows(), record_batch.num_columns());

    println!("UncheckedIO: Data transfer complete. We lived.");
    Ok(())
}


// --- 2. BINARY STREAM HANDLER (THE DYNAMIC PARSER) ---

// This enum will hold our different builder types
enum DynamicBuilder {
    Int64(Box<Int64Builder>),
    Int32(Box<Int32Builder>), // NEW
    Float64(Box<Float64Builder>), // NEW
    Float32(Box<Float32Builder>),
    String(Box<StringBuilder>),
    Boolean(Box<BooleanBuilder>),
    Timestamp(Box<TimestampNanosecondBuilder>),
    Date32(Box<Date32Builder>), // NEW
}

// Postgres Epoch for timestamps
const POSTGRES_EPOCH_NAIVE: NaiveDateTime = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
// Unix Epoch for dates
const UNIX_EPOCH_NAIVE_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

// This function is now private to this module (it's not 'pub')
async fn handle_binary_copy(
    mut stream: Pin<Box<CopyOutStream>>,
    arrow_schema: Arc<Schema>
) -> Result<(usize, RecordBatch)> {

    let mut binary_buffer: Vec<u8> = Vec::new();

    // 1. Aggregate all bytes from the stream into our buffer
    while let Some(segment_result) = stream.next().await {
        let segment: Bytes = segment_result.context("Error reading segment from CopyOutStream")?;
        binary_buffer.extend_from_slice(&segment);
    }

    let bytes_received = binary_buffer.len();
    if bytes_received == 0 {
        return Err(anyhow!("Received zero bytes from COPY stream. Check query and permissions."));
    }

    println!("UncheckedIO: Received {} total bytes from stream.", bytes_received);

    // --- PARSE POSTGRES BINARY HEADER ---
    let mut cursor = Cursor::new(&binary_buffer[..]);
    let mut magic_signature = [0u8; 11];
    cursor.read_exact(&mut magic_signature).context("Failed to read magic signature")?;
    if &magic_signature != b"PGCOPY\n\xff\r\n\0" {
        return Err(anyhow!("Invalid Postgres COPY binary signature."));
    }
    let _flags = cursor.read_u32::<BigEndian>().context("Failed to read flags")?;
    let _header_ext_len = cursor.read_u32::<BigEndian>().context("Failed to read header extension length")?;
    println!("UncheckedIO: Postgres binary header validated.");

    // --- DYNAMIC FIELD DESERIALIZATION ---

    // 1. Create a dynamic list of builders based on the schema
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

    let mut rows_processed = 0;

    // 2. Loop through the rest of the buffer until we hit the 2-byte trailer
    loop {
        let col_count = match cursor.read_i16::<BigEndian>() {
            Ok(count) => count,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                break; // Reached end of buffer *before* trailer, assume it's the end
            }
            Err(e) => return Err(e.into()),
        };

        // Check for the 2-byte trailer (-1) which signals the end of the data
        if col_count == -1 {
            println!("UncheckedIO: Reached end-of-stream trailer.");
            break; // End of stream
        }

        rows_processed += 1;

        // 3. Dynamic Row-Parsing Loop
        for (i, builder) in builders.iter_mut().enumerate() {
            let field_len_i32 = cursor.read_i32::<BigEndian>()
                .context(format!("Failed to read field length for col {}", i))?;

            if field_len_i32 == -1 {
                // Handle NULLs
                match builder {
                    DynamicBuilder::Int64(b) => b.append_null(),
                    DynamicBuilder::Float64(b) => b.append_null(),
                    DynamicBuilder::Int32(b) => b.append_null(),
                    DynamicBuilder::Float32(b) => b.append_null(),
                    DynamicBuilder::String(b) => b.append_null(),
                    DynamicBuilder::Boolean(b) => b.append_null(),
                    DynamicBuilder::Timestamp(b) => b.append_null(),
                    DynamicBuilder::Date32(b) => b.append_null(),
                }
                continue; // Go to the next field
            }

            let field_len_usize = field_len_i32 as usize;

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
                        .context(format!("Failed to parse UTF-8 string for col {}", i))?;
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
                    // Postgres binary DATE is i32 days since 2000-01-01
                    let pg_days = cursor.read_i32::<BigEndian>()?;
                    // Arrow Date32 is i32 days since 1970-01-01 (Unix Epoch)
                    let epoch_delta_days = (POSTGRES_EPOCH_NAIVE.date() - UNIX_EPOCH_NAIVE_DATE).num_days() as i32;
                    let unix_days = epoch_delta_days + pg_days;
                    b.append_value(unix_days);
                }
            }
        }
    }

    // 4. Finalize the Arrays
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

    // 5. Build the Final RecordBatch
    let record_batch = RecordBatch::try_new(
        arrow_schema.clone(),
        final_columns,
    ).context("Failed to create final Arrow RecordBatch")?;

    Ok((rows_processed, record_batch))
}
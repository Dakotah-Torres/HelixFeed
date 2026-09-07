pub mod book;
pub mod orders;
pub mod trades;

use arrow::array::{ArrayRef, Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde_json::Value;
use sqlx::postgres::PgPool;
use sqlx::Row;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

pub const PARQUET_ARCHIVE_DIR: &str = "parquet_archive";
const FETCH_BATCH_SIZE: i64 = 10_000;

#[derive(Debug, Clone)]
pub struct RawRecord {
    pub id: i32,
    pub received: DateTime<Utc>,
    pub data_provider: String,
    pub data_type: String,
    pub symbol: String,
    pub raw_json: Value,
}

pub async fn fetch_unprocessed(
    pool: &PgPool,
    data_type: &str,
    limit: i64,
) -> Result<Vec<RawRecord>, anyhow::Error> {
    let rows = sqlx::query(
        "SELECT id, received, data_provider, data_type, symbol, raw_json
         FROM raw_financial_data
         WHERE data_type = $1
         ORDER BY id
         LIMIT $2",
    )
    .bind(data_type)
    .bind(limit)
    .fetch_all(pool)
    .await?;

    let records = rows
        .into_iter()
        .map(|row| RawRecord {
            id: row.get("id"),
            received: row.get("received"),
            data_provider: row.get("data_provider"),
            data_type: row.get("data_type"),
            symbol: row.get("symbol"),
            raw_json: row.get("raw_json"),
        })
        .collect();

    Ok(records)
}

/// Deletes exactly the given row IDs — never a blind TRUNCATE. Raw rows that land in
/// the table *during* a normalization run simply aren't in this ID set, so they're
/// left untouched for the next run rather than silently lost.
pub async fn delete_processed(pool: &PgPool, ids: &[i32]) -> Result<(), anyhow::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    sqlx::query("DELETE FROM raw_financial_data WHERE id = ANY($1)")
        .bind(ids)
        .execute(pool)
        .await?;
    Ok(())
}

fn archive_path(data_type: &str) -> PathBuf {
    let now = Utc::now();
    let filename = format!("{}_{}.parquet", data_type, now.format("%Y%m%dT%H%M%SZ"));
    PathBuf::from(PARQUET_ARCHIVE_DIR).join(filename)
}

/// Archives the raw rows exactly as ingested (id, provider, data_type, symbol, raw JSON)
/// to a Parquet file. This is the cold-storage snapshot — independent of, and written
/// before, the normalized-table insert below.
fn write_parquet_archive(raw: &[RawRecord], data_type: &str) -> Result<(), anyhow::Error> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("received", DataType::Utf8, false),
        Field::new("data_provider", DataType::Utf8, false),
        Field::new("data_type", DataType::Utf8, false),
        Field::new("symbol", DataType::Utf8, false),
        Field::new("raw_json", DataType::Utf8, false),
    ]));

    let ids: Int32Array = raw.iter().map(|r| r.id).collect();
    let received: StringArray = raw.iter().map(|r| Some(r.received.to_rfc3339())).collect();
    let providers: StringArray = raw.iter().map(|r| Some(r.data_provider.as_str())).collect();
    let data_types: StringArray = raw.iter().map(|_| Some(data_type)).collect();
    let symbols: StringArray = raw.iter().map(|r| Some(r.symbol.as_str())).collect();
    let raw_jsons: StringArray = raw.iter().map(|r| Some(r.raw_json.to_string())).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ids) as ArrayRef,
            Arc::new(received) as ArrayRef,
            Arc::new(providers) as ArrayRef,
            Arc::new(data_types) as ArrayRef,
            Arc::new(symbols) as ArrayRef,
            Arc::new(raw_jsons) as ArrayRef,
        ],
    )?;

    let path = archive_path(data_type);
    let file = File::create(&path)?;
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(file, schema, Some(props))?;
    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}

/// One normalization pass for a single data_type: fetch unprocessed raw rows, archive
/// them to Parquet, transform + insert into the typed normalized table, then delete
/// exactly those raw rows. Returns how many raw rows were processed.
pub async fn run_for_data_type(pool: &PgPool, data_type: &str) -> Result<usize, anyhow::Error> {
    let raw = fetch_unprocessed(pool, data_type, FETCH_BATCH_SIZE).await?;
    if raw.is_empty() {
        return Ok(0);
    }

    let ids: Vec<i32> = raw.iter().map(|r| r.id).collect();

    write_parquet_archive(&raw, data_type)?;

    match data_type {
        "trades" => trades::insert(pool, &trades::parse_batch(&raw)?).await?,
        "book" => book::insert(pool, &book::parse_batch(&raw)?).await?,
        "orders" => orders::insert(pool, &orders::parse_batch(&raw)?).await?,
        other => anyhow::bail!("normalizer: unknown data_type '{}'", other),
    }

    delete_processed(pool, &ids).await?;

    Ok(raw.len())
}

pub async fn run_all(pool: &PgPool) -> Result<(), anyhow::Error> {
    std::fs::create_dir_all(PARQUET_ARCHIVE_DIR)?;

    for data_type in ["trades", "book", "orders"] {
        let processed = run_for_data_type(pool, data_type).await?;
        println!("normalizer: processed {} raw '{}' rows", processed, data_type);
    }

    Ok(())
}

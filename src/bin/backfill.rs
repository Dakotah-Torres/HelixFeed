use chrono::{DateTime, Utc};
use helix_feed::config::load_config;
use helix_feed::normalizer::{book, orders, trades, RawRecord, PARQUET_ARCHIVE_DIR};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::fs::File;
use std::path::Path;

/// How many raw rows to hold in memory at once before writing them out and clearing
/// the buffer. Keeps peak memory bounded regardless of how large the total archive is
/// or how big any single Parquet file happens to be — this is the fix for the incident
/// where the old orders-only version loaded every archived row into memory up front.
const STREAM_CHUNK_SIZE: usize = 50_000;

/// General-purpose recovery/backfill tool: re-reads archived Parquet files for a given
/// data type (trades, book, or orders) and re-inserts them into that type's normalized
/// table, streaming in bounded chunks rather than loading the whole archive at once.
///
/// This does NOT truncate or otherwise touch the target table — that's deliberate.
/// Prepare the table however you want first (e.g. the rename-old/create-fresh pattern),
/// then run this to (re)populate it. Usage:
///     backfill <trades|book|orders>
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    let data_type = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("usage: backfill <trades|book|orders>"))?;

    if !["trades", "book", "orders"].contains(&data_type.as_str()) {
        anyhow::bail!(
            "unknown data type '{}': must be trades, book, or orders",
            data_type
        );
    }

    let config = load_config("helix_config.yml")?;
    let pg = &config.database_conf.postgres_config;
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        pg.user, pg.password, pg.host, pg.port, pg.database
    );
    let pool = PgPoolOptions::new().max_connections(5).connect(&db_url).await?;

    let archive_dir = Path::new(PARQUET_ARCHIVE_DIR);
    let prefix = format!("{}_", data_type);

    let mut files: Vec<_> = std::fs::read_dir(archive_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let name = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            name.starts_with(&prefix) && name.ends_with(".parquet")
        })
        .collect();
    files.sort();

    println!("found {} archived '{}' files", files.len(), data_type);

    let mut buffer: Vec<RawRecord> = Vec::with_capacity(STREAM_CHUNK_SIZE);
    let mut total_written = 0usize;

    for path in &files {
        let filename = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        println!("reading {}", filename);

        let file = File::open(path)?;
        let reader = SerializedFileReader::new(file)?;

        for row_result in reader.get_row_iter(None)? {
            let row = row_result?;
            let id = row.get_int(0)?;
            let received_str = row.get_string(1)?;
            let data_provider = row.get_string(2)?.clone();
            let row_data_type = row.get_string(3)?.clone();
            let symbol = row.get_string(4)?.clone();
            let raw_json_str = row.get_string(5)?;

            let received: DateTime<Utc> = received_str.parse()?;
            let raw_json: serde_json::Value = serde_json::from_str(raw_json_str)?;

            buffer.push(RawRecord {
                id,
                received,
                data_provider,
                data_type: row_data_type,
                symbol,
                raw_json,
            });

            if buffer.len() >= STREAM_CHUNK_SIZE {
                total_written += flush_chunk(&pool, &data_type, &buffer).await?;
                buffer.clear();
            }
        }
    }

    if !buffer.is_empty() {
        total_written += flush_chunk(&pool, &data_type, &buffer).await?;
        buffer.clear();
    }

    println!(
        "backfill complete: {} '{}' rows written",
        total_written, data_type
    );

    Ok(())
}

async fn flush_chunk(
    pool: &PgPool,
    data_type: &str,
    chunk: &[RawRecord],
) -> Result<usize, anyhow::Error> {
    let count = match data_type {
        "trades" => {
            let rows = trades::parse_batch(chunk)?;
            let n = rows.len();
            trades::insert(pool, &rows).await?;
            n
        }
        "book" => {
            let rows = book::parse_batch(chunk)?;
            let n = rows.len();
            book::insert(pool, &rows).await?;
            n
        }
        "orders" => {
            let rows = orders::parse_batch(chunk)?;
            let n = rows.len();
            orders::insert(pool, &rows).await?;
            n
        }
        _ => unreachable!(),
    };
    println!(
        "  wrote a chunk of {} normalized '{}' rows",
        count, data_type
    );
    Ok(count)
}

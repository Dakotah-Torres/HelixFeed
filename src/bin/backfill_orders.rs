use chrono::{DateTime, Utc};
use helix_feed::config::load_config;
use helix_feed::normalizer::{orders, RawRecord, PARQUET_ARCHIVE_DIR};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use sqlx::postgres::PgPoolOptions;
use std::fs::File;
use std::path::Path;

/// One-off recovery: re-reads every archived orders_*.parquet file (which still has the
/// full, untouched raw JSON — including the `event` field the normalizer was dropping),
/// re-parses it with the fixed orders normalizer, and rebuilds orders_normalized from
/// scratch. Nothing needs to be re-collected from Kraken — the Parquet archive was
/// always the real source of truth for this.
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    let config = load_config("helix_config.yml")?;
    let pg = &config.database_conf.postgres_config;
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        pg.user, pg.password, pg.host, pg.port, pg.database
    );
    let pool = PgPoolOptions::new().max_connections(5).connect(&db_url).await?;

    let archive_dir = Path::new(PARQUET_ARCHIVE_DIR);
    let mut all_records: Vec<RawRecord> = Vec::new();

    for entry in std::fs::read_dir(archive_dir)? {
        let entry = entry?;
        let path = entry.path();
        let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("").to_string();
        if !filename.starts_with("orders_") || !filename.ends_with(".parquet") {
            continue;
        }

        println!("reading {}", filename);
        let file = File::open(&path)?;
        let reader = SerializedFileReader::new(file)?;

        for row_result in reader.get_row_iter(None)? {
            let row = row_result?;
            let id = row.get_int(0)?;
            let received_str = row.get_string(1)?;
            let data_provider = row.get_string(2)?.clone();
            let data_type = row.get_string(3)?.clone();
            let symbol = row.get_string(4)?.clone();
            let raw_json_str = row.get_string(5)?;

            let received: DateTime<Utc> = received_str.parse()?;
            let raw_json: serde_json::Value = serde_json::from_str(raw_json_str)?;

            all_records.push(RawRecord {
                id,
                received,
                data_provider,
                data_type,
                symbol,
                raw_json,
            });
        }
    }

    println!("read {} archived orders raw rows total", all_records.len());

    let normalized = orders::parse_batch(&all_records)?;
    println!("parsed into {} normalized order-book events", normalized.len());

    sqlx::query("TRUNCATE orders_normalized RESTART IDENTITY")
        .execute(&pool)
        .await?;
    orders::insert(&pool, &normalized).await?;

    println!(
        "backfill complete: {} rows written to orders_normalized",
        normalized.len()
    );

    Ok(())
}

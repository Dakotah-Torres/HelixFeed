use super::RawRecord;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPool;

/// A normalized L2 book snapshot. `bids`/`asks` stay as JSON arrays of {price, qty} —
/// that shape is itself common across exchanges, just not flat/scalar. `provider_meta`
/// is empty for Kraken today; kept for whatever a future provider adds that doesn't fit.
pub struct NormalizedBook {
    pub provider: String,
    pub symbol: String,
    pub event_time: DateTime<Utc>,
    pub received: DateTime<Utc>,
    pub checksum: i64,
    pub bids: Value,
    pub asks: Value,
    pub provider_meta: Value,
}

#[derive(Deserialize)]
struct RawBookEnvelope {
    data: Vec<RawBookItem>,
}

#[derive(Deserialize, Serialize)]
struct RawBookLevel {
    price: f64,
    qty: f64,
}

#[derive(Deserialize)]
struct RawBookItem {
    symbol: String,
    bids: Vec<RawBookLevel>,
    asks: Vec<RawBookLevel>,
    checksum: i64,
    timestamp: DateTime<Utc>,
}

pub fn parse_batch(raw: &[RawRecord]) -> Result<Vec<NormalizedBook>, anyhow::Error> {
    let mut out = Vec::new();
    for record in raw {
        let envelope: RawBookEnvelope = serde_json::from_value(record.raw_json.clone())?;
        for item in envelope.data {
            out.push(NormalizedBook {
                provider: record.data_provider.clone(),
                symbol: item.symbol,
                event_time: item.timestamp,
                received: record.received,
                checksum: item.checksum,
                bids: serde_json::to_value(&item.bids)?,
                asks: serde_json::to_value(&item.asks)?,
                provider_meta: serde_json::json!({}),
            });
        }
    }
    Ok(out)
}

pub async fn insert(pool: &PgPool, rows: &[NormalizedBook]) -> Result<(), anyhow::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let provider: Vec<&str> = rows.iter().map(|r| r.provider.as_str()).collect();
    let symbol: Vec<&str> = rows.iter().map(|r| r.symbol.as_str()).collect();
    let event_time: Vec<DateTime<Utc>> = rows.iter().map(|r| r.event_time).collect();
    let received: Vec<DateTime<Utc>> = rows.iter().map(|r| r.received).collect();
    let checksum: Vec<i64> = rows.iter().map(|r| r.checksum).collect();
    let bids: Vec<Value> = rows.iter().map(|r| r.bids.clone()).collect();
    let asks: Vec<Value> = rows.iter().map(|r| r.asks.clone()).collect();
    let provider_meta: Vec<Value> = rows.iter().map(|r| r.provider_meta.clone()).collect();

    sqlx::query(
        "INSERT INTO book_normalized
            (provider, symbol, event_time, received, checksum, bids, asks, provider_meta)
         SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::timestamptz[], $4::timestamptz[],
            $5::bigint[], $6::jsonb[], $7::jsonb[], $8::jsonb[]
         )",
    )
    .bind(&provider)
    .bind(&symbol)
    .bind(&event_time)
    .bind(&received)
    .bind(&checksum)
    .bind(&bids)
    .bind(&asks)
    .bind(&provider_meta)
    .execute(pool)
    .await?;

    Ok(())
}

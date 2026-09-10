use super::RawRecord;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::postgres::PgPool;

/// A normalized L3 order-book event. Same shape as book_normalized, but each bid/ask
/// level also carries its own order_id and per-level timestamp — that's what makes it
/// L3 rather than aggregated L2 depth.
pub struct NormalizedOrderBookEvent {
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
struct RawOrdersEnvelope {
    data: Vec<RawOrdersItem>,
}

#[derive(Deserialize, Serialize)]
struct RawOrderLevel {
    // Only present on "update" messages (add/modify/delete) — absent on the initial
    // "snapshot" message, since a snapshot is just "here are the orders", not an event.
    #[serde(default)]
    event: Option<String>,
    order_id: String,
    limit_price: f64,
    order_qty: f64,
    timestamp: DateTime<Utc>,
}

#[derive(Deserialize)]
struct RawOrdersItem {
    symbol: String,
    bids: Vec<RawOrderLevel>,
    asks: Vec<RawOrderLevel>,
    checksum: i64,
    timestamp: DateTime<Utc>,
}

pub fn parse_batch(raw: &[RawRecord]) -> Result<Vec<NormalizedOrderBookEvent>, anyhow::Error> {
    let mut out = Vec::new();
    for record in raw {
        let envelope: RawOrdersEnvelope = serde_json::from_value(record.raw_json.clone())?;
        for item in envelope.data {
            out.push(NormalizedOrderBookEvent {
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

pub async fn insert(pool: &PgPool, rows: &[NormalizedOrderBookEvent]) -> Result<(), anyhow::Error> {
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
        "INSERT INTO orders_normalized
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

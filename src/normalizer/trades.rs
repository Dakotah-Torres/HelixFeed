use super::RawRecord;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;
use sqlx::postgres::PgPool;

/// A normalized trade: fields common to any exchange's idea of "a trade" become real
/// columns; anything Kraken-specific that doesn't fit that shape goes in `provider_meta`.
pub struct NormalizedTrade {
    pub provider: String,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub trade_id: i64,
    pub event_time: DateTime<Utc>,
    pub received: DateTime<Utc>,
    pub provider_meta: Value,
}

#[derive(Deserialize)]
struct RawTradeEnvelope {
    data: Vec<RawTradeItem>,
}

#[derive(Deserialize)]
struct RawTradeItem {
    symbol: String,
    side: String,
    qty: f64,
    price: f64,
    ord_type: String,
    trade_id: i64,
    timestamp: DateTime<Utc>,
}

pub fn parse_batch(raw: &[RawRecord]) -> Result<Vec<NormalizedTrade>, anyhow::Error> {
    let mut out = Vec::new();
    for record in raw {
        let envelope: RawTradeEnvelope = serde_json::from_value(record.raw_json.clone())?;
        for item in envelope.data {
            out.push(NormalizedTrade {
                provider: record.data_provider.clone(),
                symbol: item.symbol,
                side: item.side,
                price: item.price,
                qty: item.qty,
                trade_id: item.trade_id,
                event_time: item.timestamp,
                received: record.received,
                provider_meta: serde_json::json!({ "ord_type": item.ord_type }),
            });
        }
    }
    Ok(out)
}

pub async fn insert(pool: &PgPool, rows: &[NormalizedTrade]) -> Result<(), anyhow::Error> {
    if rows.is_empty() {
        return Ok(());
    }

    let provider: Vec<&str> = rows.iter().map(|r| r.provider.as_str()).collect();
    let symbol: Vec<&str> = rows.iter().map(|r| r.symbol.as_str()).collect();
    let side: Vec<&str> = rows.iter().map(|r| r.side.as_str()).collect();
    let price: Vec<f64> = rows.iter().map(|r| r.price).collect();
    let qty: Vec<f64> = rows.iter().map(|r| r.qty).collect();
    let trade_id: Vec<i64> = rows.iter().map(|r| r.trade_id).collect();
    let event_time: Vec<DateTime<Utc>> = rows.iter().map(|r| r.event_time).collect();
    let received: Vec<DateTime<Utc>> = rows.iter().map(|r| r.received).collect();
    let provider_meta: Vec<Value> = rows.iter().map(|r| r.provider_meta.clone()).collect();

    sqlx::query(
        "INSERT INTO trades_normalized
            (provider, symbol, side, price, qty, trade_id, event_time, received, provider_meta)
         SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::float8[], $5::float8[],
            $6::bigint[], $7::timestamptz[], $8::timestamptz[], $9::jsonb[]
         )",
    )
    .bind(&provider)
    .bind(&symbol)
    .bind(&side)
    .bind(&price)
    .bind(&qty)
    .bind(&trade_id)
    .bind(&event_time)
    .bind(&received)
    .bind(&provider_meta)
    .execute(pool)
    .await?;

    Ok(())
}

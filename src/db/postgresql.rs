use crate::config::PostgresConfig; 
use sqlx::postgres::{PgPoolOptions, PgPool};
use serde_json; 
use chrono::{DateTime, Utc};
use crate::config::FeedType;
use crate::db::buffer::DataBuffer;


pub struct RawRow {
    pub received: DateTime<Utc>, 
    pub data_provider: String,
    pub data_type: FeedType, 
    pub symbol: String, 
    pub raw_json: serde_json::Value,
}

impl RawRow {
    pub fn new(received: DateTime<Utc>, data_provider: String, data_type: FeedType, symbol: String,  raw_json: serde_json::Value) -> Self {
        RawRow { received, data_provider, data_type, symbol , raw_json }
    }

    pub fn data_buff_to_rawrows(data_buffer: DataBuffer) -> Result<Vec<RawRow>, anyhow::Error> {
        let mut raw_rows: Vec<RawRow> = Vec::with_capacity(data_buffer.get_messages().len()); 
        let now: DateTime<Utc> = Utc::now();
        for msg in data_buffer.get_messages() {
            let json: serde_json::Value = serde_json::from_str(&msg)?;
            if let Some(data_type) = data_buffer.get_data_type(){
                if let Some(symbol) = data_buffer.get_symbol(){
                    if let Some(provider) = data_buffer.get_provider() {
                        let new_row = RawRow::new(now, provider, data_type , symbol, json);
                        raw_rows.push(new_row);
                    }
                }
            };
        }

        Ok(raw_rows)
    }
}

#[derive(Clone)]
pub struct PostgresDBRaw {
    pool: PgPool
}

impl PostgresDBRaw {
    pub async fn new(config: &PostgresConfig) -> Result<Self, anyhow::Error>{
        let db_url = format!("postgres://{}:{}@{}:{}/{}",
    config.user, config.password, config.host, config.port, config.database);
        
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&db_url)
            .await?;
        
        Ok(PostgresDBRaw { pool })
    }

    pub async fn new_with_migration(pg_config: &PostgresConfig) -> Result<Self, anyhow::Error> {
        let db = PostgresDBRaw::new(&pg_config).await?;
        sqlx::migrate!("./migrations")
            .run(&db.pool)
            .await?;
        Ok(db)
    }


    pub async fn insert_raw_data_batch(&self, rows: Vec<RawRow>) -> Result<(), anyhow::Error> {
        let received: Vec<DateTime<Utc>> = rows.iter().map(|r| r.received).collect(); 
        let data_provider: Vec<String> = rows.iter().map(|dp| dp.data_provider.clone()).collect();
        let data_type: Vec<String> = rows.iter().map(|dt| dt.data_type.as_str().to_string().clone()).collect();
        let symbol: Vec<String> = rows.iter().map(|sy| sy.symbol.clone()).collect();
        let json: Vec<serde_json::Value> = rows.iter().map(|j| j.raw_json.clone()).collect();

        let query = "
            INSERT INTO raw_financial_data ( received, data_provider, data_type, symbol, raw_json)
            SELECT * FROM UNNEST($1::timestamptz[],  $2::text[], $3::text[], $4::text[], $5::jsonb[])
        ";

        sqlx::query(query )
            .bind(&received)
            .bind(&data_provider)
            .bind(&data_type)
            .bind(&symbol)
            .bind(&json)
            .execute(&self.pool)
            .await?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use dotenvy; 

    #[tokio::test]
    async fn test_postgres_connection() -> Result<(), anyhow::Error> {
        dotenvy::dotenv().ok();

        let db_user = std::env::var("DB_USER").unwrap();
        let db_pass = std::env::var("DB_PASS").unwrap(); 
        let postgres_config: PostgresConfig =  PostgresConfig {
                    host: "localhost".to_string(),
                    port: "5432".to_string(),
                    database: "helixfeed_dev".to_string(),
                    user: db_user,
                    password: db_pass
                };
                
        let pool = PostgresDBRaw::new(&postgres_config).await;

        assert!(pool.is_ok(),"expected connection to succeed, got: {:?}", pool.err());
        Ok(())

    }

    #[tokio::test]
    async fn test_postgres_with_migrations() -> Result<(), anyhow::Error> {
        dotenvy::dotenv().ok();

        let db_user = std::env::var("DB_USER").unwrap();
        let db_pass = std::env::var("DB_PASS").unwrap(); 
        let postgres_config: PostgresConfig =  PostgresConfig {
                    host: "localhost".to_string(),
                    port: "5432".to_string(),
                    database: "helixfeed_dev".to_string(),
                    user: db_user,
                    password: db_pass
                };
        
        let pool = PostgresDBRaw::new(&postgres_config).await;

        assert!(pool.is_ok(), "expected connection to succeed, got: {:?}", pool.err());
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_raw_data_batch() -> Result<(), anyhow::Error> {
        dotenvy::dotenv().ok();
        let db_user = std::env::var("DB_USER").unwrap();
        let db_pass = std::env::var("DB_PASS").unwrap();
        let postgres_config = PostgresConfig {
            host: "localhost".to_string(),
            port: "5432".to_string(),
            database: "helixfeed_dev".to_string(),
            user: db_user,
            password: db_pass,
        };

        let db = PostgresDBRaw::new_with_migration(&postgres_config).await?;

        let test_symbol = "TEST/UNIT";
        let rows = vec![
            RawRow {
                received: Utc::now(),
                data_provider: "kraken".to_string(),
                data_type: FeedType::Trades,
                symbol: test_symbol.to_string(),
                raw_json: serde_json::json!({ "price": 100.0 }),
            },
            RawRow {
                received: Utc::now(),
                data_provider: "databento".to_string(),
                data_type: FeedType::Book,
                symbol: test_symbol.to_string(),
                raw_json: serde_json::json!({ "bids": [1, 2, 3] }),
            },
            RawRow {
                received: Utc::now(),
                data_provider: "kraken".to_string(),
                data_type: FeedType::Ticks,
                symbol: test_symbol.to_string(),
                raw_json: serde_json::json!({ "tick": 42 }),
            },
        ];
        
        db.insert_raw_data_batch(rows).await?;

        // verify all 3 landed
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM raw_financial_data WHERE symbol = $1")
            .bind(test_symbol)
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(count.0, 3, "expected 3 rows inserted");

        // verify a specific row's fields actually match — proves column alignment, not just row count
        let row: (String, String) = sqlx::query_as(
            "SELECT data_provider, data_type FROM raw_financial_data WHERE symbol = $1 AND data_type = $2"
        )
            .bind(test_symbol)
            .bind("book")
            .fetch_one(&db.pool)
            .await?;
        assert_eq!(row.0, "databento", "expected the 'book' row's provider to be databento, not swapped with another row");

        // cleanup
        sqlx::query("DELETE FROM raw_financial_data WHERE symbol = $1")
            .bind(test_symbol)
            .execute(&db.pool)
            .await?;

        Ok(())

    }




}


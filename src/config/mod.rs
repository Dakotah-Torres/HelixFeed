use serde::{Deserialize};
use serde_yaml;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum Market {
    Crypto, 
    Futures,
    Forex, 
    Equities
}




#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FeedType {
    Trades, 
    Book,
    Candles, 
    Orders,
    Ticks
}

impl FeedType {
    pub fn as_str(&self) -> &'static str {
        match self {
            FeedType::Trades => "trades",
            FeedType::Book => "book",
            FeedType::Candles => "candles",
            FeedType::Orders => "orders",
            FeedType::Ticks => "ticks",
        }
    }
}





#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub host: String, 
    pub port: String,
    pub database: String,
    pub user: String, 
    pub password: String
}

#[derive(Debug, Clone, Deserialize)]
pub struct R2Config {
    pub bucket: String,
    pub upload_schedule: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LogConfig {
    pub feed_log_location: String,
    pub system_log_location: String
}

#[derive(Debug, Clone, Deserialize)]
pub struct DBConfig {
    pub postgres_config: PostgresConfig,
    pub duckdb_location: String,
    #[serde(default)]
    pub r2: Option<R2Config>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SymbolConfig {
    pub markets: Market,
    pub symbol: String, 
    pub feed_type: FeedType,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ProviderConfig {
    pub provider: String, 
    pub reconnect_delay_secs: u32,
    pub max_reconnect_attempts: u32,
    pub symbol_feeds: Vec<SymbolConfig>,
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub providers: Vec<ProviderConfig>,
    pub database_conf: DBConfig,
    pub log_config: LogConfig, 
    pub buffer_capacity: usize,
    pub buffer_swap_trigger: f32, 
}

pub fn load_config(path: &str) -> Result<Config, anyhow::Error> {
    let mut file = File::open(path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    let config: Config = serde_yaml::from_str(&content)?;
    Ok(config)
}

pub fn validate_config(config: &Config) -> Result<(), anyhow::Error> {
    let known_providers = vec!["kraken", "databento"];
    let valid_upload_schedule = vec!["daily", "weekly", "monthly"];

    // --- Global, one-time checks: postgres, r2, logs all live once now, not per-provider ---

    let pg = &config.database_conf.postgres_config;

    if pg.host.is_empty() {
        anyhow::bail!("No Database store Host Provided");
    }
    if pg.port.is_empty() {
        anyhow::bail!("Database Port not Provided");
    }
    if pg.port.parse::<u16>().is_err() {
        anyhow::bail!("Database port '{}' is not a valid port number", pg.port);
    }
    if pg.user.is_empty() || pg.password.is_empty() {
        anyhow::bail!("Username or Password for Database Missing");
    }

    // r2 is optional — only validate its contents if it was actually provided.
    if let Some(r2) = &config.database_conf.r2 {
        if r2.bucket.is_empty() {
            anyhow::bail!("r2 configured but bucket is empty");
        }
        if !valid_upload_schedule.contains(&r2.upload_schedule.as_str()) {
            anyhow::bail!("Invalid r2 upload_schedule '{}'. Must be daily, weekly, or monthly", r2.upload_schedule);
        }
    }

    if config.log_config.feed_log_location.is_empty() {
        anyhow::bail!("feed_log_location must not be empty");
    }
    if config.log_config.system_log_location.is_empty() {
        anyhow::bail!("system_log_location must not be empty");
    }

    if config.buffer_capacity == 0 {
            anyhow::bail!("buffer_capacity must be greater than 0");
        }

        if config.buffer_swap_trigger < 0.0 || config.buffer_swap_trigger > 1.0 {
            anyhow::bail!("buffer_swap_trigger must be 0.0–1.0, got {}", config.buffer_swap_trigger);
        }

    // --- Per-provider checks ---

    for provider in &config.providers {
        if !known_providers.contains(&provider.provider.as_str()) {
            anyhow::bail!("Unknown provider '{}'. Known: kraken, databento", provider.provider);
        }

        if provider.max_reconnect_attempts == 0 {
            anyhow::bail!("max_reconnect_attempts must be greater than 0");
        }

        for symbol_feed  in &provider.symbol_feeds {
            if symbol_feed.symbol.is_empty() {
                anyhow::bail!("Provider '{}' has a feed with no symbols configured", provider.provider);
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use dotenvy;

    fn valid_config() -> Config {
        dotenvy::dotenv().ok();
        let db_user = std::env::var("DB_USER").unwrap();
        let db_pass = std::env::var("DB_PASS").unwrap();

        Config {
            providers: vec![ProviderConfig {
                provider: "kraken".to_string(),
                reconnect_delay_secs: 5,
                max_reconnect_attempts: 5,
                symbol_feeds: vec![SymbolConfig {
                    markets: Market::Crypto,
                    symbol: "BTC/USD".to_string(),
                    feed_type: FeedType::Trades,
                }],
            }],
            database_conf: DBConfig {
                postgres_config: PostgresConfig {
                    host: "localhost".to_string(),
                    port: "5432".to_string(),
                    database: "helixfeed_dev".to_string(),
                    user: db_user,
                    password: db_pass,
                },
                duckdb_location: "/db/kraken.duckdb".to_string(),
                r2: Some(R2Config {
                    bucket: "helixfeed-archive".to_string(),
                    upload_schedule: "weekly".to_string(),
                }),
            },
            log_config: LogConfig {
                feed_log_location: "logs/kraken.log".to_string(),
                system_log_location: "logs/system.log".to_string(),
            },
            buffer_capacity: 1000,
            buffer_swap_trigger: 0.8,
        }
    }

    #[test]
    fn test_valid_config() {
        assert!(validate_config(&valid_config()).is_ok());
    }

    #[test]
    fn test_valid_config_no_r2() {
        let mut config = valid_config();
        config.database_conf.r2 = None;
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn test_unknown_provider() {
        let mut config = valid_config();
        config.providers[0].provider = "binance".to_string();
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("Unknown provider"));
    }

    #[test]
    fn test_buffer_capacity_zero() {
        let mut config = valid_config();
        config.buffer_capacity = 0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("buffer_capacity must be greater than 0"));
    }

    #[test]
    fn test_buffer_swap_trigger() {
        let mut config = valid_config();
        config.buffer_swap_trigger = 2.0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("buffer_swap_trigger must be 0.0–1.0"));

        config.buffer_swap_trigger = -1.0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("buffer_swap_trigger must be 0.0–1.0"));
    }

    #[test]
    fn test_max_reconnect_attempts_zero() {
        let mut config = valid_config();
        config.providers[0].max_reconnect_attempts = 0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("feed_log_location must not be empty"));
    }
}
use serde::{Deserialize};
use serde_yaml;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Market {
    Crypto, 
    Futures,
    Forex, 
    Equities
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    Normalized,
    Raw   
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FeedType {
    Trades, 
    Book,
    Candles, 
    Orders,
    Ticks
}

#[derive(Debug, Deserialize)]
pub struct DataFeedConfig {
    pub feed_type: FeedType,
    pub mode: Mode
}
#[derive(Debug, Deserialize)]
pub struct SymbolConfig {
    pub symbol: String, 
    pub data: Vec<DataFeedConfig>
}

#[derive(Debug, Deserialize)]
pub struct Feeds{
    pub provider: String, 
    pub markets: Vec<Market>,
    pub log_location: String, 
    pub db_location: String,
    pub reconnect_delay_secs: u32,
    pub max_reconnect_attempts: u32,
    pub symbols: Vec<SymbolConfig>,
    pub buffer_capacity: usize,
    pub buffer_swap_trigger: f32, 
    pub r2_bucket: String, 
    pub r2_upload_schedule: String 
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub feeds: Vec<Feeds>
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

    for feed in &config.feeds {
        if !known_providers.contains(&feed.provider.as_str()) {
            anyhow::bail!("Unknown provider '{}'. Known: kraken, databento", feed.provider);
        }
        

        if feed.buffer_capacity == 0 {
            anyhow::bail!("buffer_capacity must be greater than 0");
        }

        if feed.buffer_swap_trigger < 0.0 || feed.buffer_swap_trigger > 1.0 {
            anyhow::bail!("buffer_swap_trigger must be 0.0–1.0, got {}", feed.buffer_swap_trigger);
        }

        if feed.max_reconnect_attempts == 0 {
            anyhow::bail!("max_reconnect_attempts must be greater than 0");
        }

        if !valid_upload_schedule.contains(&feed.r2_upload_schedule.as_str()) {
            anyhow::bail!("Invalid r2_upload_schedule '{}'. Must be daily, weekly, or monthly", feed.r2_upload_schedule);
        }

        if feed.symbols.is_empty() {
            anyhow::bail!("Provider '{}' has no symbols configured", feed.provider)
        }

        for symbol in &feed.symbols {
            if symbol.data.is_empty() {
                anyhow::bail!("Symbol '{}' under '{}' has no data feeds configured", symbol.symbol, feed.provider);
            }
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config {
            feeds: vec![Feeds {
                provider: "kraken".to_string(),
                markets: vec![Market::Crypto],
                log_location: "logs/kraken.log".to_string(),
                db_location: "/db/kraken.duckdb".to_string(),
                reconnect_delay_secs: 5,
                max_reconnect_attempts: 5,
                buffer_capacity: 1000,
                buffer_swap_trigger: 0.8,
                r2_bucket: "helixfeed-archive".to_string(),
                r2_upload_schedule: "weekly".to_string(),
                symbols: vec![SymbolConfig {
                    symbol: "BTC/USD".to_string(),
                    data: vec![DataFeedConfig {
                        feed_type: FeedType::Trades,
                        mode: Mode::Normalized,
                    }],
                }],
            }],
        }
    }

    #[test]
    fn test_valid_config() {
        assert!(validate_config(&valid_config()).is_ok());
    }

    #[test]
    fn test_unknown_provider() {
        let mut config = valid_config();
        config.feeds[0].provider = "binance".to_string();
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("Unknown provider"));
    }

    #[test]
    fn test_buffer_capacity_zero() {
        let mut config = valid_config();
        config.feeds[0].buffer_capacity = 0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("buffer_capacity must be greater than 0"));
    }
    
    #[test]
    fn test_buffer_swap_triger() {
        let mut config = valid_config();
        config.feeds[0].buffer_swap_trigger = 2.0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("buffer_swap_trigger must be 0.0–1.0"));
        config.feeds[0].buffer_swap_trigger = -1.0;
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("buffer_swap_trigger must be 0.0–1.0"));
    }

    #[test]
    fn test_r2_schedule() {
        let mut config = valid_config();
        config.feeds[0].r2_upload_schedule = "Monday".to_string();
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("Invalid r2_upload_schedule"));
    }
    #[test]
    fn test_no_data_feeds() {
        let mut config = valid_config();
        for symbol in &mut config.feeds[0].symbols {
            symbol.data.clear();
        }
        
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("has no data feeds configured"));
    }

    #[test]
    fn test_no_symbols() {
        let mut config = valid_config();
        config.feeds[0].symbols.clear();
        
        let err = validate_config(&config).unwrap_err();
        assert!(err.to_string().contains("no symbols configured"));
    }


}
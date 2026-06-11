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
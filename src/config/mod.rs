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
pub enum SupportedFeeds {
    Trades, 
    Book,
    Candles, 
    Orders,
    Ticks
}


#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFeeds {
    Trades(Mode), 
    Book(Mode),
    Candles(Mode), 
    Orders(Mode),
    Ticks(Mode)
}

#[derive(Debug, Deserialize)]
pub struct SymbolConfig {
    pub symbol: String, 
    pub data: Vec<DataFeeds>
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
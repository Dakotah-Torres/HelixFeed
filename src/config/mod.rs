use serde::{Deserialize};
use serde_yaml;
use std::fs::File;
use std::io::Read;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Markets {
    Crypto, 
    Futures,
    Forex, 
    Equities
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DataFeeds {
    Trades, 
    Book,
    Candles, 
    Orders,
    Ticks
}

#[derive(Debug, Deserialize)]
pub struct Feeds{
    pub provider: String, 
    pub markets: Vec<Markets>,
    pub symbols: Vec<String>,
    pub data_types: Vec<DataFeeds>
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
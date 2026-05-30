use futures_util::StreamExt;
use serde::{Serialize, Deserialize};

use tokio_tungstenite::tungstenite::protocol::Message;




use super::{KRAKEN_PUB_URL, CHANNEL_TICKER_L1, kraken_trade_connect};


use crate::db::buffer;


#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenTickerReqInner {
    pub channel: String, 
    pub symbol: Vec<String>,
    pub snapshot: bool
}

#[derive(Serialize, Deserialize, Debug)]
pub struct  KrakenTickerReqOuter {
    pub method: String,
    pub params: KrakenTickerReqInner,
    pub req_id: u64, 
    
}
#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenTickerResInner<'a> {
    ask: f64,
    ask_qty: f64,
    bid: f64,
    bid_qyt: f64,
    change: f64,
    change_pct:f64,
    high: f64,
    last: f64,
    low:f64,
    symbol: &'a str,
    timestamp: &'a str,
    volume: f64,
    vwap: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenTickerResOuter<'a> {
    pub channel: &'a str,
    #[serde(rename = "type")]
    pub res_type: &'a str,
    pub data: Vec<KrakenTickerResInner<'a>>
    

} 






pub async fn kraken_ticker_data_feed(){
    println!(" ------ Ticker Engine Starting ------ ");
    let inner = KrakenTickerReqInner {
        channel: CHANNEL_TICKER_L1.to_string(),
        symbol: vec!["BTC/USD".to_string()],
        snapshot: false,
    };
    let outer = KrakenTickerReqOuter {
        method: "subscribe".to_string(),
        params: inner,
        req_id: 231,
    };

    let mut stream = kraken_trade_connect(outer, KRAKEN_PUB_URL)
            .await;

    while let Some(message) = stream.next().await {
        if let Ok(Message::Text(msg)) = message {
            println!("{}", msg);
        }
    }
}



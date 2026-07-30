use futures_util::StreamExt;
use serde::{Serialize, Deserialize};

use tokio_tungstenite::tungstenite::protocol::Message;
use tokio::sync::mpsc;

use crate::data_feeds::kraken::connection::connector::{KRAKEN_PUB_URL, CHANNEL_TICKER_L1, kraken_connect};
use std::sync::{Arc, Mutex};
use crate::logging::feed_logger::FeedLogger; 
use crate::logging::feed_logger::LoggerContext; 



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


pub async fn kraken_ticker_data_feed(symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext){
        {
            let mut log = logger.lock().unwrap();
            log.log_started(&log_ctx);
            log.log_info(format!("Ticker Engine Starting: {}", symbols.join(", ")), &log_ctx);
        }
        
        let inner = KrakenTickerReqInner {
            channel: CHANNEL_TICKER_L1.to_string(),
            symbol: symbols, //this will be all the symboles that are set in the config file
            snapshot: false,
        };
        
        let outer = KrakenTickerReqOuter {
            method: "subscribe".to_string(),
            params: inner,
            req_id: 231,
        };

        let mut stream = kraken_connect(outer, KRAKEN_PUB_URL)
                .await;

        while let Some(message) = stream.next().await {
            if let Ok(Message::Text(msg)) = message {
                if tx.send(msg).await.is_err() {
                    let mut log = logger.lock().unwrap();
                    log.log_error("Ticker: receiver dropped, shutting down".to_string(), &log_ctx);
                    break;
                }
            }
        }
    }






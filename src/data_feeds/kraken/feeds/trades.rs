use crate::data_feeds::kraken::connection::connector::{KRAKEN_PUB_URL, CHANNEL_TRADES, kraken_connect};
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio::sync::mpsc;
use std::sync::{Arc, Mutex};
use crate::logging::feed_logger::{FeedLogger, LoggerContext};


#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenTradeInnerReq {
    pub channel: String, 
    pub symbol: Vec<String>,
    pub snapshot: bool
}
#[derive(Serialize, Deserialize, Debug)]
pub struct  KrakenTradeReqOuter {
    pub method: String,
    pub params: KrakenTradeInnerReq,
    pub req_id: u64, 
    
}
#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenTradeInnerRes<'a> {
    pub symbol: &'a str, 
    pub side: &'a str, 
    pub qty: f64,
    pub price: f64,
    pub ord_type: &'a str, 
    pub trade_id: i64, 
    pub timestamp: &'a str, 
}
#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenTradeOuterRes<'a> {
    pub channel: &'a str,
    #[serde(rename = "type")]
    pub trd_type: &'a str,
    pub data:Vec<KrakenTradeInnerRes<'a>>,
}

pub async fn kraken_trade_data_feed(symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext){
        {
            let mut log = logger.lock().unwrap();
            log.log_started(&log_ctx);
            log.log_info(format!("Trade Engine Starting: {}", symbols.join(", ")), &log_ctx);
        }
        let inner = KrakenTradeInnerReq {
            channel: CHANNEL_TRADES.to_string(),
            symbol: symbols, //this will be all the symboles that are set in the config file
            snapshot: false,
        };
        let outer = KrakenTradeReqOuter {
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
                    log.log_error("Trades: receiver dropped, shutting down".to_string(), &log_ctx);
                    break;
                }
            }
        }
    }
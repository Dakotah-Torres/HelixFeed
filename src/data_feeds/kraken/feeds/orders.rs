use std::env;
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use tokio_tungstenite::tungstenite::protocol::Message;
use crate::data_feeds::kraken::connection::connector::{KRAKEN_AUTH_URL, CHANNEL_ORDERS_L3, kraken_connect};
use std::sync::{Arc, Mutex};
use crate::logging::feed_logger::{FeedLogger, LoggerContext};
use crate::logging::LogType;
use sha2::{Sha256, Digest};
use hex; 


use tokio::sync::mpsc; 

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(into ="u32")]
pub enum OrderDepth {
    Ten = 10,
    OneHundred = 100,
    OneThousand = 1000,
}

fn hash_string(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    // Efficiently encodes directly to a String
    hex::encode(result) 
}

impl From<OrderDepth> for u32 {
    fn from(depth: OrderDepth) -> u32 {
        match depth {
            OrderDepth::Ten => 10,
            OrderDepth::OneHundred => 100,
            OrderDepth::OneThousand => 1000,
        }
    }
}



#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenOrdersReqInnerParams {
    channel: String,
    symbol: Vec<String>, 
    depth: OrderDepth,
    snapshot: bool,
    token: String
}

#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenOrdersReqOuter {
    method: String,
    params: KrakenOrdersReqInnerParams,
    req_id: i32,

}


#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenOrderBidAsk<'a> {
    order_id: &'a str,
    limit_price: f64,
    order_qty: f64,
    timestamp: &'a str
}



#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenOrderResObject<'a> {
    symbol: &'a str,
    bids: Vec<KrakenOrderBidAsk<'a>>,
    asks: Vec<KrakenOrderBidAsk<'a>>,
    checksum:  i64,
    timestamp: &'a str

}


pub async fn kraken_order_data_feed(symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext){
    let api_key = env::var("KRAKEN_WEB_SOCKET_KEY")
        .expect("KRAKEN_API_SECRET not set in .env");
    {
        let mut log = logger.lock().unwrap();
        log.feed_log(LogType::Info, "started", &log_ctx);
        log.feed_log(LogType::Info, &format!("Order Engine Starting: {}", symbols.join(", ")), &log_ctx);
        let api_hash = hash_string(&api_key);
        log.feed_log(LogType::Info, &format!("API CONNECTED | CONFIRMATION KEY: {}", api_hash), &log_ctx);
    }
    
    
    let params = KrakenOrdersReqInnerParams {
    channel: CHANNEL_ORDERS_L3.to_string(),
    symbol: symbols,
    depth: OrderDepth::OneHundred,
    snapshot: false,
    token: api_key, 
    
    };

    let order_request = KrakenOrdersReqOuter {
        method: "subscribe".to_string(),
        params: params,
        req_id: 1234
    };

    let mut stream  = kraken_connect(order_request, KRAKEN_AUTH_URL)
        .await;

    while let Some(message) = stream.next().await {
        if let Ok(Message::Text(msg)) = message {
            if tx.send(msg).await.is_err() {
                let mut log = logger.lock().unwrap();
                log.feed_log(LogType::Error, "Orders: receiver dropped, shutting down", &log_ctx);
                break;
            }
        }
    }
}





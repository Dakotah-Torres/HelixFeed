use futures_util::StreamExt;
use serde::{Serialize, Deserialize};

use tokio_tungstenite::tungstenite::protocol::Message;
use crate::data_feeds::kraken::connection::connector::{KRAKEN_PUB_URL, CHANNEL_BOOK_L2, kraken_connect};
use std::sync::{Arc, Mutex};
use crate::logging::feed_logger::{FeedLogger, LoggerContext};
use crate::logging::LogType;

use tokio::sync::mpsc; 

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(into ="u32")]
pub enum BookDepth {
    Ten = 10,
    TwentyFive = 25,
    OneHundred = 100,
    FiveHundred = 500,
    OneThousand = 1000,
}

impl From<BookDepth> for u32 {
    fn from(depth: BookDepth) -> u32 {
        match depth {
            BookDepth::Ten => 10,
            BookDepth::TwentyFive => 25,
            BookDepth::OneHundred => 100,
            BookDepth::FiveHundred => 500,
            BookDepth::OneThousand => 1000,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KrakenBookReqInner {
    channel: String,
    symbol: Vec<String>,
    depth: BookDepth,
    snapshot: bool,
} 
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KrakenBookReqOuter {
    method: String,
    params: KrakenBookReqInner,
    req_id: u64
    
}
#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenBookBidAsk {
    price: f64,
    qty: f64,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenBookObject<'a> {
    asks: Vec<KrakenBookBidAsk>,
    bids: Vec<KrakenBookBidAsk>,
    checksum: i64,
    symbol: &'a str,
    timestamp: &'a str 
}
#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenBookResOuter <'a>{
    channel: &'a str, 
    #[serde(rename="type")]
    res_type: &'a str,
    data: Vec<KrakenBookObject<'a>>
}

pub async fn kraken_book_data_feed(symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext , reconnect_delay_secs:  u32, max_reconnect_attempts: u32) {

    {
        let mut log = logger.lock().unwrap();
        log.feed_log(LogType::Info, &format!("Book Engine Starting: {}", symbols.join(", ")), &log_ctx);
    }
    let inner = KrakenBookReqInner {
        channel: CHANNEL_BOOK_L2.to_string(),
        symbol: symbols,
        depth: BookDepth::OneHundred,
        snapshot:false
    };
    let outer = KrakenBookReqOuter {
        method: "subscribe".to_string(),
        params: inner,
        req_id: 1234
    };

    let mut attempts = 0; 
    loop {
        let mut stream = match kraken_connect(outer.clone(), KRAKEN_PUB_URL).await{
            Ok(stream) => {
                attempts = 0;
                stream
            }

            Err(e) => {
                attempts +=1; 
                {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Error, &format!("Kraken Book Connection Failed - Below Error:\n {} \n Attempting to reconnect {} out of {} attempts", e, attempts, max_reconnect_attempts), &log_ctx);
                    if attempts >= max_reconnect_attempts{
                        log.feed_log(LogType::Error, &format!("Kraken Book Connection Failed - Below Error:\n {} \n Max Attemtps reached Ending Connection", e), &log_ctx);
                        return
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay_secs as u64)).await; 
                continue;
                
            }
        };

    
        while let Some(message) = stream.next().await {
            if let Ok(Message::Text(msg)) = message {
                if tx.send(msg).await.is_ok(){
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Error, "Book: receiver dropped, shutting down", &log_ctx);
                
                } 
            }

        }
    };
}

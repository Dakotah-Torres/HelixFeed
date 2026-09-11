use futures_util::StreamExt;
use serde::{Serialize, Deserialize};

use tokio_tungstenite::tungstenite::protocol::Message;
use crate::data_feeds::kraken::connection::connector::{KRAKEN_PUB_URL, CHANNEL_BOOK_L2, kraken_connect, STALE_CONNECTION_TIMEOUT_SECS};
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
                {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Info, "Connected", &log_ctx);
                }
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

        // See docs/logging.md - timeout wraps every read so a silently-dead connection
        // (socket never errors, never closes, Kraken just stops sending) still produces a
        // log line and forces a reconnect instead of blocking forever.
        loop {
            let next = tokio::time::timeout(
                std::time::Duration::from_secs(STALE_CONNECTION_TIMEOUT_SECS),
                stream.next(),
            ).await;

            let message = match next {
                Ok(Some(m)) => m,
                Ok(None) => {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Warn, "Stream ended (remote closed the connection) - reconnecting", &log_ctx);
                    break;
                }
                Err(_elapsed) => {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Warn, &format!("No messages received in {}s - connection appears stale, forcing reconnect", STALE_CONNECTION_TIMEOUT_SECS), &log_ctx);
                    break;
                }
            };

            match message {
                Ok(Message::Text(msg)) => {
                    let is_book = serde_json::from_str::<serde_json::Value>(&msg)
                        .ok()
                        .and_then(|v| v.get("channel").and_then(|c| c.as_str().map(|s| s.to_string())))
                        .map(|channel| channel == "book")
                        .unwrap_or(false);

                    if !is_book {
                        continue;
                    }

                    if tx.send(msg).await.is_err(){
                        let mut log = logger.lock().unwrap();
                        log.feed_log(LogType::Error, "Book: receiver dropped, shutting down", &log_ctx);
                        break;
                    }
                }
                Ok(Message::Close(frame)) => {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Warn, &format!("Received Close frame from Kraken: {:?}", frame), &log_ctx);
                    break;
                }
                Ok(_) => {
                    // Ping/Pong/Binary/raw Frame - proves the connection is alive, nothing to
                    // forward, no log needed.
                }
                Err(e) => {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Error, &format!("WebSocket read error: {} - reconnecting", e), &log_ctx);
                    break;
                }
            }
        }
    };
}

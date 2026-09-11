use crate::data_feeds::kraken::connection::connector::{KRAKEN_PUB_URL, CHANNEL_TRADES, kraken_connect, STALE_CONNECTION_TIMEOUT_SECS};
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use tokio_tungstenite::tungstenite::protocol::Message;
use tokio::sync::mpsc;
use std::sync::{Arc, Mutex};
use crate::logging::feed_logger::{FeedLogger, LoggerContext};
use crate::logging::LogType;


#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KrakenTradeInnerReq {
    pub channel: String, 
    pub symbol: Vec<String>,
    pub snapshot: bool
}
#[derive(Serialize, Deserialize, Debug, Clone)]
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

pub async fn kraken_trade_data_feed(symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext, reconnect_delay_secs:  u32, max_reconnect_attempts: u32){
        {
            let mut log = logger.lock().unwrap();
            log.feed_log(LogType::Info, "started", &log_ctx);
            log.feed_log(LogType::Info, &format!("Trade Engine Starting: {}", symbols.join(", ")), &log_ctx);
        }
        
        

        let mut attempts = 0;

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
                    attempts += 1;
                    {
                        let mut log = logger.lock().unwrap();
                        log.feed_log(LogType::Error, &format!("Kraken Trade Connection Failed - Below Error:\n {} \n Attempting to reconnect {} out of {} attempts", e, attempts, max_reconnect_attempts), &log_ctx);
                        if attempts >= max_reconnect_attempts{
                            log.feed_log(LogType::Error, &format!("Kraken Trade Connection Failed - Below Error:\n {} \n Max Attemtps reached Ending Connection", e), &log_ctx);
                            return
                        }

                    }

                    tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay_secs as u64)).await;
                    continue;

                }
            };

            // See docs/logging.md - timeout wraps every read so a silently-dead connection
            // (socket never errors, never closes, Kraken just stops sending) still produces
            // a log line and forces a reconnect instead of blocking forever.
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
                        let is_trade = serde_json::from_str::<serde_json::Value>(&msg)
                            .ok()
                            .and_then(|v| v.get("channel").and_then(|c| c.as_str().map(|s| s.to_string())))
                            .map(|channel| channel == "trade")
                            .unwrap_or(false);

                        if !is_trade {
                            continue;
                        }

                        if tx.send(msg).await.is_err() {
                            let mut log = logger.lock().unwrap();
                            log.feed_log(LogType::Error, "Trades: receiver dropped, shutting down", &log_ctx);
                            break;
                        }
                    }
                    Ok(Message::Close(frame)) => {
                        let mut log = logger.lock().unwrap();
                        log.feed_log(LogType::Warn, &format!("Received Close frame from Kraken: {:?}", frame), &log_ctx);
                        break;
                    }
                    Ok(_) => {
                        // Ping/Pong/Binary/raw Frame - proves the connection is alive, nothing
                        // to forward, no log needed.
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
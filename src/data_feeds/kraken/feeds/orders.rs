
use futures_util::StreamExt;
use serde::{Serialize, Deserialize};
use tokio_tungstenite::tungstenite::protocol::Message;
use crate::data_feeds::kraken::connection::connector::{KRAKEN_AUTH_URL, CHANNEL_ORDERS_L3, kraken_connect, get_kraken_ws_token, STALE_CONNECTION_TIMEOUT_SECS};
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



#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct KrakenOrdersReqInnerParams {
    channel: String,
    symbol: Vec<String>, 
    depth: OrderDepth,
    snapshot: bool,
    token: String
}

#[derive(Serialize, Deserialize, Debug, Clone)]
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


pub async fn kraken_order_data_feed(symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext, reconnect_delay_secs: u32, max_reconnect_attempts: u32){
    let mut attempts = 0;
    
    {
        let mut log = logger.lock().unwrap();
        log.feed_log(LogType::Info, "started", &log_ctx);
        log.feed_log(LogType::Info, &format!("Order Engine Starting: {}", symbols.join(", ")), &log_ctx);
        
    }

    let api_key = match get_kraken_ws_token().await {
        Ok(token) => token,
        Err(e) => {
            let mut log = logger.lock().unwrap();
            log.feed_log(LogType::Error, &format!("API Key Unable to be retrieved: {} - Ending process", e), &log_ctx);
            return
        }
    };
    
    {
        let mut log = logger.lock().unwrap();
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
    loop {
        let mut stream  = match kraken_connect(order_request.clone(), KRAKEN_AUTH_URL).await{
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
                    log.feed_log(LogType::Error, &format!("Kraken Order Connection Failed - Below Error:\n {} \n Attempting to reconnect {} out of {} attempts", e, attempts, max_reconnect_attempts), &log_ctx);
                    if attempts >= max_reconnect_attempts{
                        log.feed_log(LogType::Error, &format!("Kraken Order Connection Failed - Below Error:\n {} \n Max Attemtps reached Ending Connection", e), &log_ctx);
                        return
                    }
                }

                tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay_secs as u64)).await;
                continue;

            }
        };

        // Wrapped in a timeout rather than a bare `while let Some(...) = stream.next().await`
        // so silence itself is detectable. A bare read can't tell "nothing has happened in
        // 6 hours because the market is quiet" apart from "the socket is dead but never
        // errored or closed" — this is exactly what took BTC/USD Orders down on 2026-09-10:
        // it went quiet at 21:19:16 and stream.next() just sat there forever with no log
        // output at all, no error, nothing. See docs/logging.md for the full breakdown.
        loop {
            let next = tokio::time::timeout(
                std::time::Duration::from_secs(STALE_CONNECTION_TIMEOUT_SECS),
                stream.next(),
            ).await;

            let message = match next {
                Ok(Some(m)) => m,
                Ok(None) => {
                    // Remote closed the TCP stream cleanly. Not an error — just the end of
                    // this connection — but worth its own line so a reconnect shows up in
                    // the log as "stream ended" rather than looking identical to a fresh
                    // "Order Engine Starting" with no explanation for why we're here again.
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Warn, "Stream ended (remote closed the connection) - reconnecting", &log_ctx);
                    break;
                }
                Err(_elapsed) => {
                    // No frame of ANY kind — not data, not a ping/pong, not a close — for
                    // STALE_CONNECTION_TIMEOUT_SECS. Treat the socket as a zombie and force
                    // a reconnect rather than waiting indefinitely.
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Warn, &format!("No messages received in {}s - connection appears stale, forcing reconnect", STALE_CONNECTION_TIMEOUT_SECS), &log_ctx);
                    break;
                }
            };

            match message {
                Ok(Message::Text(msg)) => {
                    let is_orders = serde_json::from_str::<serde_json::Value>(&msg)
                        .ok()
                        .and_then(|v| v.get("channel").and_then(|c| c.as_str().map(|s| s.to_string())))
                        .map(|channel| channel == CHANNEL_ORDERS_L3)
                        .unwrap_or(false);

                    if !is_orders {
                        continue;
                    }

                    if tx.send(msg).await.is_err() {
                        let mut log = logger.lock().unwrap();
                        log.feed_log(LogType::Error, "Orders: receiver dropped, shutting down", &log_ctx);
                        break;
                    }
                }
                Ok(Message::Close(frame)) => {
                    // Kraken telling us it's hanging up, rather than us finding out the hard
                    // way — usually means something on Kraken's end (maintenance, rate limit,
                    // auth/token expiry), so it's worth distinguishing from a generic drop.
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Warn, &format!("Received Close frame from Kraken: {:?}", frame), &log_ctx);
                    break;
                }
                Ok(_) => {
                    // Ping/Pong/Binary/raw Frame - not data, nothing to forward, but it does
                    // prove the connection is alive, so no log needed here.
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





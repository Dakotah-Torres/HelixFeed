
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
use std::future::Future;

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
    run_order_feed(KRAKEN_AUTH_URL, get_kraken_ws_token, symbols, tx, logger, log_ctx, reconnect_delay_secs, max_reconnect_attempts).await
}

/// The actual feed loop. The endpoint and the token fetcher are parameters so tests can run it
/// against a local fake Kraken server with a fake token source.
pub(crate) async fn run_order_feed<F, Fut>(url: &str, fetch_token: F, symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext, reconnect_delay_secs: u32, max_reconnect_attempts: u32)
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<String, anyhow::Error>>,
{
    let mut attempts = 0;
    
    {
        let mut log = logger.lock().unwrap();
        log.feed_log(LogType::Info, "started", &log_ctx);
        log.feed_log(LogType::Info, &format!("Order Engine Starting: {}", symbols.join(", ")), &log_ctx);
        
    }

    loop {
        // A fresh token on EVERY connect, not once up front. Kraken WS tokens have to be used
        // within 15 minutes of being issued - the old code fetched one before this loop, so any
        // reconnect after that subscribed with an expired token. Kraken's error reply isn't a
        // level3 message so it got filtered out, the stale timeout fired 60s later, and the feed
        // reconnected with the same dead token forever, silently.
        let token = match fetch_token().await {
            Ok(token) => {
                let mut log = logger.lock().unwrap();
                log.feed_log(LogType::Info, &format!("API CONNECTED | CONFIRMATION KEY: {}", hash_string(&token)), &log_ctx);
                token
            }
            Err(e) => {
                attempts += 1;
                {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Error, &format!("WS token could not be retrieved: {} - attempt {} out of {}", e, attempts, max_reconnect_attempts), &log_ctx);
                    if attempts >= max_reconnect_attempts {
                        log.feed_log(LogType::Error, "WS token could not be retrieved - Max Attempts reached Ending Connection", &log_ctx);
                        return
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay_secs as u64)).await;
                continue;
            }
        };

        let order_request = KrakenOrdersReqOuter {
            method: "subscribe".to_string(),
            params: KrakenOrdersReqInnerParams {
                channel: CHANNEL_ORDERS_L3.to_string(),
                symbol: symbols.clone(),
                depth: OrderDepth::OneHundred,
                snapshot: false,
                token,
            },
            req_id: 1234
        };

        let mut stream  = match kraken_connect(order_request, url).await{
            Ok(stream) => {
                attempts = 0;
                {
                    let mut log = logger.lock().unwrap();
                    log.feed_log(LogType::Info, "Connected", &log_ctx);
                }
                // TODO(v1.1.0 metrics): FEED_UP{provider,symbol,feed_type}.set(1) here
                stream
            }

            Err(e) => {
                attempts +=1;
                // TODO(v1.1.0 metrics): RECONNECT_ATTEMPTS_TOTAL{provider,symbol,feed_type}.inc() here
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
        // TODO(v1.1.0 metrics): FEED_UP{provider,symbol,feed_type}.set(0) on every `break`
        // in this inner loop (stale timeout, stream end, close frame, read error, receiver dropped).
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

                    // TODO(v1.1.0 metrics): TOTAL_MESSAGES{provider,symbol,feed_type}.inc() here
                    if tx.send(msg).await.is_err() {
                        let mut log = logger.lock().unwrap();
                        log.feed_log(LogType::Error, "Orders: DB inserter is gone (receiver dropped) - stopping this feed instead of reconnecting", &log_ctx);
                        // `return`, not `break`: a break only leaves the read loop, and the outer loop
                        // would reconnect to Kraken, fail to send again, and repeat forever.
                        return;
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





#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FeedType;
    use crate::data_feeds::kraken::test_support::{eventually, fake_kraken, test_logger};
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    /// Regression test for the stale-token bug: every (re)connect must subscribe with a
    /// freshly fetched token, never the one from the first connection.
    #[tokio::test]
    async fn every_reconnect_uses_a_fresh_token() {
        // The server hangs up right after each subscribe, forcing a reconnect every time.
        let server = fake_kraken(vec![], true).await;
        let (logger, ctx) = test_logger(FeedType::Orders);
        let (tx, _rx) = mpsc::channel(8);

        let issued = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&issued);
        let fetch_token = move || {
            let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
            async move { Ok(format!("token-{}", n)) }
        };

        let url = server.url.clone();
        let feed = tokio::spawn(async move {
            run_order_feed(&url, fetch_token, vec!["BTC/USD".to_string()], tx, logger, ctx, 0, 5).await
        });

        assert!(eventually(|| server.connection_count() >= 3).await, "feed should keep reconnecting");
        feed.abort();

        let subscribes = server.subscribes.lock().unwrap();
        let tokens: Vec<&str> = subscribes.iter().take(3).map(|s| s["params"]["token"].as_str().unwrap()).collect();
        assert_eq!(tokens, vec!["token-1", "token-2", "token-3"]);
    }

    #[tokio::test]
    async fn gives_up_after_max_token_failures() {
        let server = fake_kraken(vec![], false).await;
        let (logger, ctx) = test_logger(FeedType::Orders);
        let (tx, _rx) = mpsc::channel(8);

        let calls = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&calls);
        let fetch_token = move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Err(anyhow::anyhow!("Kraken REST API unavailable")) }
        };

        let feed = run_order_feed(&server.url, fetch_token, vec!["BTC/USD".to_string()], tx, logger, ctx, 0, 3);
        tokio::time::timeout(Duration::from_secs(5), feed).await.expect("feed should give up");

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(server.connection_count(), 0, "never connect without a token");
    }
}

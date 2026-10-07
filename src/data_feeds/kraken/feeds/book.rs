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
// TODO(v1.1.0): `checksum` is deserialized but never validated - roadmap M5 calls this out as
// pending. Once book checksum validation exists, add a helix_book_checksum_failures_total{symbol}
// counter alongside it so a dropped/out-of-order message is a visible metric, not silent
// book corruption.
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
    run_book_feed(KRAKEN_PUB_URL, symbols, tx, logger, log_ctx, reconnect_delay_secs, max_reconnect_attempts).await
}

/// The actual feed loop, with the endpoint as a parameter so tests can point it at a local
/// fake Kraken server instead of the real one.
pub(crate) async fn run_book_feed(url: &str, symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext , reconnect_delay_secs:  u32, max_reconnect_attempts: u32) {

    {
        let mut log = logger.lock().unwrap();
        log.feed_log(LogType::Info, &format!("Book Engine Starting: {}", symbols.join(", ")), &log_ctx);
    }
    let inner = KrakenBookReqInner {
        channel: CHANNEL_BOOK_L2.to_string(),
        symbol: symbols,
        depth: BookDepth::OneHundred,
        snapshot:true
    };
    let outer = KrakenBookReqOuter {
        method: "subscribe".to_string(),
        params: inner,
        req_id: 1234
    };

    let mut attempts = 0;
    loop {
        let mut stream = match kraken_connect(outer.clone(), url).await{
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

                    // TODO(v1.1.0 metrics): TOTAL_MESSAGES{provider,symbol,feed_type}.inc() here
                    if tx.send(msg).await.is_err(){
                        let mut log = logger.lock().unwrap();
                        log.feed_log(LogType::Error, "Book: DB inserter is gone (receiver dropped) - stopping this feed instead of reconnecting", &log_ctx);
                        // `return`, not `break`: a break only leaves the read loop, and the outer loop
                        // would reconnect to Kraken, fail to send again, and repeat forever.
                        return;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FeedType;
    use crate::data_feeds::kraken::test_support::{fake_kraken, test_logger};
    use std::time::Duration;

    /// Same regression as the trades feed: a dropped receiver must stop the feed, not
    /// start a reconnect loop.
    #[tokio::test]
    async fn feed_stops_instead_of_reconnecting_when_receiver_is_dropped() {
        let book = r#"{"channel":"book","type":"update","data":[]}"#.to_string();
        let server = fake_kraken(vec![book], false).await;
        let (logger, ctx) = test_logger(FeedType::Book);

        let (tx, rx) = mpsc::channel(8);
        drop(rx);

        let feed = run_book_feed(&server.url, vec!["BTC/USD".to_string()], tx, logger, ctx, 0, 5);
        tokio::time::timeout(Duration::from_secs(5), feed)
            .await
            .expect("feed should return on its own, not reconnect forever");

        assert_eq!(server.connection_count(), 1);
    }
}

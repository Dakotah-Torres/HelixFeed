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
    run_trade_feed(KRAKEN_PUB_URL, symbols, tx, logger, log_ctx, reconnect_delay_secs, max_reconnect_attempts).await
}

/// The actual feed loop, with the endpoint as a parameter so tests can point it at a local
/// fake Kraken server instead of the real one.
pub(crate) async fn run_trade_feed(url: &str, symbols: Vec<String>, tx: mpsc::Sender<String>, logger: Arc<Mutex<FeedLogger>>, log_ctx: LoggerContext, reconnect_delay_secs:  u32, max_reconnect_attempts: u32){
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
                    attempts += 1;
                    // TODO(v1.1.0 metrics): RECONNECT_ATTEMPTS_TOTAL{provider,symbol,feed_type}.inc() here
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
                        let is_trade = serde_json::from_str::<serde_json::Value>(&msg)
                            .ok()
                            .and_then(|v| v.get("channel").and_then(|c| c.as_str().map(|s| s.to_string())))
                            .map(|channel| channel == "trade")
                            .unwrap_or(false);

                        if !is_trade {
                            continue;
                        }

                        // TODO(v1.1.0 metrics): TOTAL_MESSAGES{provider,symbol,feed_type}.inc() here
                        if tx.send(msg).await.is_err() {
                            let mut log = logger.lock().unwrap();
                            log.feed_log(LogType::Error, "Trades: DB inserter is gone (receiver dropped) - stopping this feed instead of reconnecting", &log_ctx);
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
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::FeedType;
    use crate::data_feeds::kraken::test_support::{fake_kraken, test_logger};
    use std::time::Duration;

    /// Regression test: when the DB inserter is gone, the feed used to `break` out of the read
    /// loop only, reconnect, fail to send again, and loop against Kraken forever. It must now
    /// stop after the first failed send, having connected exactly once.
    #[tokio::test]
    async fn feed_stops_instead_of_reconnecting_when_receiver_is_dropped() {
        let trade = r#"{"channel":"trade","type":"update","data":[]}"#.to_string();
        let server = fake_kraken(vec![trade], false).await;
        let (logger, ctx) = test_logger(FeedType::Trades);

        let (tx, rx) = mpsc::channel(8);
        drop(rx); // the DB inserter has died

        let feed = run_trade_feed(&server.url, vec!["BTC/USD".to_string()], tx, logger, ctx, 0, 5);
        tokio::time::timeout(Duration::from_secs(5), feed)
            .await
            .expect("feed should return on its own, not reconnect forever");

        assert_eq!(server.connection_count(), 1);
    }

    #[tokio::test]
    async fn feed_forwards_trade_messages_and_skips_others() {
        let heartbeat = r#"{"channel":"heartbeat"}"#.to_string();
        let trade = r#"{"channel":"trade","type":"update","data":[]}"#.to_string();
        let server = fake_kraken(vec![heartbeat, trade.clone()], false).await;
        let (logger, ctx) = test_logger(FeedType::Trades);

        let (tx, mut rx) = mpsc::channel(8);
        let url = server.url.clone();
        let feed = tokio::spawn(async move {
            run_trade_feed(&url, vec!["BTC/USD".to_string()], tx, logger, ctx, 0, 5).await
        });

        let received = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap();
        assert_eq!(received, Some(trade));
        assert_eq!(server.subscribes.lock().unwrap()[0]["params"]["channel"], "trade");
        feed.abort();
    }
}

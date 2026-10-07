//! A local stand-in for Kraken's WebSocket endpoint, so the feed loops can be tested
//! end-to-end (connect -> subscribe -> receive -> reconnect) without touching the network.

use std::sync::{Arc, Mutex};

use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::protocol::Message;

use crate::config::FeedType;
use crate::db::inserter::tests::temp_log_path;
use crate::logging::feed_logger::{FeedLogger, LoggerContext};

pub(crate) struct FakeKraken {
    pub url: String,
    /// The subscribe request each accepted connection sent, in connection order.
    pub subscribes: Arc<Mutex<Vec<serde_json::Value>>>,
}

impl FakeKraken {
    pub fn connection_count(&self) -> usize {
        self.subscribes.lock().unwrap().len()
    }
}

/// Starts a server on a random local port. For every connection it records the subscribe
/// request, sends `replies`, then either closes the socket (`close_after`) or keeps it open
/// until the client goes away.
pub(crate) async fn fake_kraken(replies: Vec<String>, close_after: bool) -> FakeKraken {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let subscribes = Arc::new(Mutex::new(Vec::new()));

    let seen = Arc::clone(&subscribes);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let seen = Arc::clone(&seen);
            let replies = replies.clone();
            tokio::spawn(async move {
                let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await else { return };
                if let Some(Ok(Message::Text(req))) = ws.next().await {
                    seen.lock().unwrap().push(serde_json::from_str(&req).unwrap());
                }
                for reply in replies {
                    let _ = ws.send(Message::Text(reply)).await;
                }
                if close_after {
                    let _ = ws.close(None).await;
                    return;
                }
                while let Some(Ok(_)) = ws.next().await {}
            });
        }
    });

    FakeKraken { url, subscribes }
}

pub(crate) fn test_logger(feed_type: FeedType) -> (Arc<Mutex<FeedLogger>>, LoggerContext) {
    let logger = FeedLogger::new(temp_log_path("feed"), "kraken".to_string()).unwrap();
    (Arc::new(Mutex::new(logger)), LoggerContext::new("BTC/USD".to_string(), feed_type))
}

/// Polls `cond` for up to two seconds.
pub(crate) async fn eventually(cond: impl Fn() -> bool) -> bool {
    for _ in 0..200 {
        if cond() {
            return true;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    cond()
}

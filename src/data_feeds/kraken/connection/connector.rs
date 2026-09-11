pub use tokio_tungstenite::tungstenite::protocol::Message;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio::net::TcpStream;
use url::Url;
use futures_util:: {SinkExt, StreamExt};
use futures_util::stream::SplitStream;
use serde::Serialize;
use std::time:: {SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use sha2::{Sha256, Digest, Sha512};
use hmac:: { Hmac, Mac, KeyInit};
use base64::{Engine as _, engine::general_purpose};
use lazy_static::lazy_static;
use tokio::sync::Mutex as AsyncMutex;


use crate::data_feeds::traits::DataProvider;
use crate::data_feeds::traits::ReplayCapability;
use crate::data_feeds::traits::ReplayLevel;
use crate::data_feeds::traits::Resolution;
use crate::config::FeedType;
use crate::config::Market;

type HmacSha512 = Hmac<Sha512>;

pub type KrakenStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type KrakenReadStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub const KRAKEN_PUB_URL: &str = "wss://ws.kraken.com/v2";
pub const KRAKEN_AUTH_URL: &str = "wss://ws-l3.kraken.com/v2";
pub const CHANNEL_BOOK_L2: &str = "book";
pub const CHANNEL_ORDERS_L3: &str = "level3";
pub const CHANNEL_TRADES: &str = "trade";

/// If a feed goes this long without receiving ANY frame at all — not data, not a ping/pong,
/// not even a close — the connection is treated as stale and force-reconnected. This is what
/// catches a zombie TCP connection: one where the socket never errors and never closes,
/// Kraken just stops sending, and `stream.next().await` would otherwise block forever with
/// no log output at all. This is exactly what took the BTC/USD Orders feed down on
/// 2026-09-10 — it went quiet at 21:19:16 and never reconnected because nothing was
/// watching for silence itself. See docs/logging.md for the full breakdown of where this
/// gets checked and logged.
pub const STALE_CONNECTION_TIMEOUT_SECS: u64 = 60;


pub async fn kraken_connect<T: Serialize>(connection_request: T, _url:&str) -> Result<KrakenReadStream, anyhow::Error> {

    let url = Url::parse(_url)?;
    
    let(ws_stream, _) = connect_async(url.to_string())
        .await?;

    let (mut write, read) = ws_stream.split();
    let conn_req_json = serde_json::to_string(&connection_request)?;

    write.send(Message::Text(conn_req_json)).await?;

    //returning the read stream
    Ok(read)

}

lazy_static! {
    // Shared across every call to get_kraken_ws_token(), from every feed task, so
    // concurrent authenticated requests can never generate colliding or out-of-order
    // nonces — Kraken requires each nonce to be strictly greater than the last one it
    // saw for a given API key, across ALL uses of that key, not just per-caller.
    static ref LAST_NONCE: AtomicU64 = AtomicU64::new(0);

    // Generating an increasing nonce isn't enough on its own — Kraken cares about the
    // order requests *arrive* at its server, not the order their nonces were generated.
    // Two concurrent requests can still arrive out of order over the network. This
    // mutex forces every token fetch (nonce generation through the HTTP round-trip)
    // to fully complete, one at a time, so requests always reach Kraken in the same
    // order their nonces were handed out.
    static ref TOKEN_FETCH_LOCK: AsyncMutex<()> = AsyncMutex::new(());
}

/// Returns a nonce that is always strictly greater than the previous one this process
/// generated, even if called many times concurrently within the same millisecond.
fn next_nonce() -> u64 {
    let now_millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis() as u64;

    let mut current = LAST_NONCE.load(Ordering::SeqCst);
    loop {
        let candidate = std::cmp::max(now_millis, current + 1);
        match LAST_NONCE.compare_exchange(current, candidate, Ordering::SeqCst, Ordering::SeqCst) {
            Ok(_) => return candidate,
            Err(actual) => current = actual,
        }
    }
}

pub async fn get_kraken_ws_token() -> Result<String, anyhow::Error> {
    // Held for the whole function — see TOKEN_FETCH_LOCK's doc comment above.
    let _guard = TOKEN_FETCH_LOCK.lock().await;

    // generate nonce
    let nonce = next_nonce().to_string();

    // build post body
    let mut params = HashMap::new();
    params.insert("nonce", nonce.as_str());
    let post_data = serde_urlencoded::to_string(&params)?;

    // read credentials from .env
    let api_key    = std::env::var("KRAKEN_API_KEY")?;
    let api_secret = std::env::var("KRAKEN_API_PRIVATE_KEY")?;

    // step 1 — SHA256 hash of nonce + post body
    let encoded     = format!("{}{}", nonce, post_data);
    let sha256_hash = Sha256::digest(encoded.as_bytes());

    // step 2 — prepend URL path to the hash
    let url_path      = "/0/private/GetWebSocketsToken";
    let secret_bytes = general_purpose::STANDARD.decode(&api_secret)?;
    let mut mac       = HmacSha512::new_from_slice(&secret_bytes)?;
    mac.update(url_path.as_bytes());
    mac.update(&sha256_hash);
    let signature = general_purpose::STANDARD.encode(mac.finalize().into_bytes());
    // step 3 — POST to Kraken REST API
    let client   = reqwest::Client::new();
    let response = client
        .post(format!("https://api.kraken.com{}", url_path))
        .header("API-Key", &api_key)
        .header("API-Sign", &signature)
        .form(&params)
        .send()
        .await?;

    // step 4 — parse the token from the response
    let body: serde_json::Value = response.json().await?;

    if let Some(errors) = body["error"].as_array() {
        if !errors.is_empty() {
            anyhow::bail!("Kraken API error: {:?}", errors);
        }
    }

    let token = body["result"]["token"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("Token not found in response"))?
        .to_string();

    Ok(token)
}


pub struct KrakenConnector; 

impl DataProvider for KrakenConnector {

    fn provider_name(&self) -> &str {
        "Kraken"
    }

    fn supported_feed_types(&self) -> Vec<crate::config::FeedType> {
        let feeds = vec![
            FeedType::Trades,
            FeedType::Book,
            FeedType::Orders,
        ];
        feeds
    }

    fn supported_markets(&self) -> Vec<Market> {
        let markets = vec![
            Market::Crypto
        ];
        markets
    }

    fn replay_capability(&self) -> ReplayCapability {
        ReplayCapability {
            level: ReplayLevel::None, 
            resolution: Resolution::Second,
        }
        
    }

}




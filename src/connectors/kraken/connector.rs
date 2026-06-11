use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};
use tokio::net::TcpStream;
use url::Url;
use futures_util:: {SinkExt, StreamExt};
use futures_util::stream::SplitStream;
use serde::Serialize;
use std::time:: {SystemTime, UNIX_EPOCH};
use std::collections::HashMap;
use sha2::{Sha256, Digest, Sha512};
use hmac:: { Hmac, Mac, KeyInit};
use base64::{Engine as _, engine::general_purpose};
pub use tokio_tungstenite::tungstenite::protocol::Message;



use crate::connectors::traits::DataProvider;
use crate::connectors::traits::ReplayCapability;
use crate::connectors::traits::ReplayLevel;
use crate::connectors::traits::Resolution;
use crate::config::FeedType;
use crate::config::Market;



type HmacSha512 = Hmac<Sha512>;

pub type KrakenStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub type KrakenReadStream = SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>;

pub const KRAKEN_PUB_URL: &str = "wss://ws.kraken.com/v2";
pub const KRAKEN_AUTH_URL: &str = "wss://ws-l3.kraken.com/v2";
pub const CHANNEL_BOOK_L2: &str = "book";
pub const CHANNEL_TICKER_L1: &str = "ticker";
pub const CHANNEL_ORDERS_L3: &str = "level3";
pub const CHANNEL_TRADES: &str = "trade";


pub async fn kraken_trade_connect<T: Serialize>(connection_request: T, _url:&str) -> KrakenReadStream {

    let url = Url::parse(_url).expect("Invalid URL");
    
    let(ws_stream, _) = connect_async(url.to_string())
        .await
        .expect("Failed to Connect");

    let (mut write, read) = ws_stream.split();
    let conn_req_json = serde_json::to_string(&connection_request)
        .expect("Failed to serialize request");

    write.send(Message::Text(conn_req_json))
        .await
        .expect("Unable to Connect");

    //returning the read stream
    read

}

pub async fn get_kraken_ws_token() -> Result<String, anyhow::Error> {
    // generate nonce
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
        .to_string();

    // build post body
    let mut params = HashMap::new();
    params.insert("nonce", nonce.as_str());
    let post_data = serde_urlencoded::to_string(&params)?;

    // read credentials from .env
    let api_key    = std::env::var("KRAKEN_API_KEY")?;
    let api_secret = std::env::var("KRAKEN_API_SECRET")?;

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
            FeedType::Ticks,
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


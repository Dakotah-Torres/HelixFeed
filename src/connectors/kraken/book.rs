use futures_util::StreamExt;
use serde::{Serialize, Deserialize};

use tokio_tungstenite::tungstenite::protocol::Message;
use super::{KRAKEN_PUB_URL, CHANNEL_BOOK_L2, kraken_trade_connect};

use crate::db::buffer::DoubleBuffer; 






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

#[derive(Serialize, Deserialize, Debug)]
pub struct KrakenBookReqInner {
    channel: String,
    symbol: Vec<String>,
    depth: BookDepth,
    snapshot: bool,
} 
#[derive(Serialize, Deserialize, Debug)]
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

pub async fn kraken_book_data_feed(){
    println!(" ------ Book Engine Starting ------ ");
    let inner = KrakenBookReqInner {
        channel: CHANNEL_BOOK_L2.to_string(),
        symbol: vec!["BTC/USD".to_string()],
        depth: BookDepth::OneHundred,
        snapshot:false
    };
    let outer = KrakenBookReqOuter {
        method: "subscribe".to_string(),
        params: inner,
        req_id: 1234
    };

    let mut stream = kraken_trade_connect(outer, KRAKEN_PUB_URL)
        .await;

    let buffer = DoubleBuffer::new(1000, 0.8);
    while let Some(message) = stream.next().await {
        if let Ok(Message::Text(msg)) = message {
            buffer.push_swap_and_save(msg,  "book", "BTCUSD")
        }
    }
}


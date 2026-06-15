use crate::config::Market;
use crate::config::FeedType;


use tokio::sync::mpsc;
use std::future::Future;

use qasm_core::core_types::trade::TradeData;
use qasm_core::core_types::book::BookData;
use qasm_core::core_types::candle::Candle;


pub enum ReplayLevel {
    None, 
    CandlesOnly, 
    TickOnly,
    TickAndBook, 
    TickBookAndTrade,
    TickBookTradeAndOrder,
}


pub enum Resolution {
    Second, 
    Millisecond, 
    Microsecond,
}
pub enum NormalizedMessage {
    Trade(TradeData),
    Book(BookData),
    Candle(Candle)
}

pub struct ReplayCapability {
    pub level: ReplayLevel,
    pub resolution: Resolution,
}


pub trait DataProvider {

    fn provider_name(&self) -> &str;
    fn supported_feed_types(&self) -> Vec<FeedType>;
    fn supported_markets(&self) -> Vec<Market>; 
    fn replay_capability(&self) -> ReplayCapability;
}

pub trait TickFeed {
    
    fn tick_raw_feed(
        &self,
        symbols: Vec<String>,
        tx: mpsc::Sender<String>
    ) -> impl Future<Output = ()>; 

    fn tick_normalized_feed(&self, raw: String) -> NormalizedMessage;
}

pub trait TradeFeed {
    
    fn trade_raw_feed(&self,
        symbols: Vec<String>,
        tx: mpsc::Sender<String>
    ) -> impl Future<Output = ()>;

    fn trade_normalized_feed(&self, raw: String) -> NormalizedMessage;
}
pub trait BookFeed {
    
    fn book_raw_feed(&self,
        symbols: Vec<String>,
        tx: mpsc::Sender<String>
    ) -> impl Future<Output = ()>;

    fn book_normalized_feed(&self, raw: String) -> NormalizedMessage;
}
pub trait OrdersFeed {
    fn orders_raw_feed(&self,
        symbols: Vec<String>,
        tx: mpsc::Sender<String>
    ) -> impl Future<Output = ()>;

    fn orders_normalized_feed(&self, raw: String) -> NormalizedMessage;
}

pub trait CandleFeed {
    
    fn candle_raw_feed(&self,
        symbols: Vec<String>,
        tx: mpsc::Sender<String>
    ) -> impl Future<Output = ()>;

    fn candle_normalized_feed(&self, raw: String) -> NormalizedMessage;
}
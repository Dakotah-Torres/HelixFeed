use crate::config::DataFeeds; 
use crate::config::Markets;
use qasm_core::trade::TradeData;
use qasm_core::book::BookData;
use qasm_core::candle::Candle;

pub enum NormalizedMessage {
    Trade(TradeData),
    Book(BookData),
    Candle(Candle)
}
pub trait DataProvider {

    fn provider_name(&self) -> &str;
    fn supported_data_feeds(&self) -> Vec<DataFeeds>;
    fn supported_markets(&self) -> Vec<Markets>; 
}

pub trait TickFeed {
    type RawMessage<'a>;
    fn tick_raw_feed(&self) -> Self::RawMessage<'a> ; 
    fn tick_normalized_feed(&self) -> NormalizedMessage;
}

pub trait TradeFeed {
    type RawMessage<'a>;
    fn trade_raw_feed(&self) -> Self::RawMessage<'a> ; 
    fn trade_normalized_feed(&self) -> NormalizedMessage;
}
pub trait BookFeed {
    type RawMessage<'a>;
    fn book_raw_feed(&self) -> Self::RawMessage<'a> ; 
    fn book_normalized_feed(&self) -> NormalizedMessage;
}
pub trait OrdersFeed {
    type RawMessage<'a>;
    fn orders_raw_feed(&self) -> Self::RawMessage<'a> ; 
    fn orders_normalized_feed(&self) -> NormalizedMessage;
}
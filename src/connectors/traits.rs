use crate::config::DataFeeds; 
use crate::config::Market;
use crate::config::SupportedFeeds;
use qasm_core::core_types::trade::TradeData;
use qasm_core::core_types::book::BookData;
use qasm_core::core_types::candle::Candle;


pub enum NormalizedMessage {
    Trade(TradeData),
    Book(BookData),
    Candle(Candle)
}
pub trait DataProvider {

    fn provider_name(&self) -> &str;
    fn supported_data_feeds(&self) -> Vec<SupportedFeeds>;
    fn supported_markets(&self) -> Vec<Market>; 
}

pub trait TickFeed {
    type RawMessage<'a>;
    fn tick_raw_feed<'a>(&self) -> Self::RawMessage<'a> ; 
    fn tick_normalized_feed<'a>(&self, raw: Self::RawMessage<'a>) -> NormalizedMessage;
}

pub trait TradeFeed {
    type RawMessage<'a>;
    fn trade_raw_feed<'a>(&self) -> Self::RawMessage<'a> ; 
    fn trade_normalized_feed<'a>(&self, raw: Self::RawMessage<'a>) -> NormalizedMessage;
}
pub trait BookFeed {
    type RawMessage<'a>;
    fn book_raw_feed<'a>(&self) -> Self::RawMessage<'a> ; 
    fn book_normalized_feed<'a>(&self, raw: Self::RawMessage<'a>) -> NormalizedMessage;
}
pub trait OrdersFeed {
    type RawMessage<'a>;
    fn orders_raw_feed<'a>(&self) -> Self::RawMessage<'a> ; 
    fn orders_normalized_feed<'a>(&self, raw: Self::RawMessage<'a>) -> NormalizedMessage;
}
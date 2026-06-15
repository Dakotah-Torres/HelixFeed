use crate::data_feeds::kraken::feeds::book::kraken_book_data_feed; 
use crate::data_feeds::kraken::feeds::ticker::kraken_ticker_data_feed;
use crate::data_feeds::kraken::feeds::trades::kraken_trade_data_feed;

use crate::logging::logger::Logger;
use crate::logging::logger::LoggerContext;

use crate::config::FeedType;
use crate::config::Feeds; 

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc; 

use crate::db::buffer::DoubleBuffer;

pub fn kraken_feed_aggrigatotor(feed: Feeds) -> Result<(), anyhow::Error>{
    for symbol in feed.symbols {
        for data_feed in &symbol.data {
            
            let (tx, mut rx) = mpsc::channel::<String>(feed.buffer_capacity);
            let symbols = vec![symbol.symbol.clone()];
            let db_location = feed.db_location.clone();
            let provider = feed.provider.clone();
            let symbol_name = symbol.symbol.clone();
            let feed_type_str = format!("{:?}", data_feed.feed_type).to_lowercase();
            
            let log = Arc::new(Mutex::new(Logger::new(feed.log_location.clone(), feed.provider.clone())?));
            let log_ctx = LoggerContext::new(symbol_name.clone(), data_feed.feed_type, data_feed.mode);
            
            match data_feed.feed_type {
                FeedType::Trades => {
                    let log = Arc::clone(&log);
                    let log_ctx = log_ctx.clone();
                    tokio::spawn(async move {
                        kraken_trade_data_feed(symbols, tx, log, log_ctx).await;
                    }); 
                }
                FeedType::Book => {
                    let log = Arc::clone(&log);
                    let log_ctx = log_ctx.clone();
                    tokio::spawn(async move {
                        kraken_book_data_feed(symbols, tx, log, log_ctx).await;
                    }); 
                }
                FeedType::Ticks => {
                    let log = Arc::clone(&log);
                    let log_ctx = log_ctx.clone();
                    tokio::spawn(async move {
                        kraken_ticker_data_feed(symbols, tx, log, log_ctx).await;
                    }); 
                }
                
                _ => {}
            } 
            let log = Arc::clone(&log);
            tokio::spawn(async move {
                let buffer = DoubleBuffer::new(1000, 0.8);
                while let Some(msg) = rx.recv().await {
                    let mut log = log.lock().unwrap();
                    buffer.push_swap_and_save(msg, &feed_type_str, &symbol_name, &provider, &db_location, &mut log, &log_ctx);
                }
            });
        }
    }
    Ok(())
}

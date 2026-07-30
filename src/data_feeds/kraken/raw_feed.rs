use crate::data_feeds::kraken::feeds::book::kraken_book_data_feed; 
use crate::data_feeds::kraken::feeds::ticker::kraken_ticker_data_feed;
use crate::data_feeds::kraken::feeds::trades::kraken_trade_data_feed;

use crate::logging::feed_logger::FeedLogger;
use crate::logging::feed_logger::LoggerContext;

use crate::config::FeedType;
use crate::config::ProviderConfig; 
use crate::config::DBLogsConfig;

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc; 
use tokio::sync::mpsc::Sender;

use crate::db::buffer::DoubleBuffer;
use crate::db::buffer::DataBuffer;

pub struct FeedPayload {
    pub buffer: DataBuffer, 
    pub config: ProviderConfig
}

//This function is meant to allow you to call one function it then initiates a mpsc channel. This will be used to pass the tx to the the provided feed type
// once it is determined what feed type is being created then this spans a new double buffer instance 
pub fn kraken_raw_feed_channel(feed: ProviderConfig, DBLogs: DBLogsConfig,  provider_channel_tx: Sender<DataBuffer> ) -> Result<(), anyhow::Error>{
    for symbol in feed.symbols {
        for data_feed in &symbol.data {
            
            let (tx_feed, mut rx_feed) = mpsc::channel::<String>(feed.buffer_capacity);
            let symbols = vec![symbol.symbol.clone()];
            
            let symbol_name = symbol.symbol.clone();
            
            let log = Arc::new(Mutex::new(FeedLogger::new(DBLogs.log_location.clone(), feed.provider.clone())?));
            let log_ctx = LoggerContext::new(symbol_name.clone(), data_feed.feed_type, data_feed.mode);
            
            match data_feed.feed_type {
                FeedType::Trades => {
                    let log = Arc::clone(&log);
                    let log_ctx = log_ctx.clone();
                    tokio::spawn(async move {
                        kraken_trade_data_feed(symbols, tx_feed, log, log_ctx).await;
                    }); 
                }
                FeedType::Book => {
                    let log = Arc::clone(&log);
                    let log_ctx = log_ctx.clone();
                    tokio::spawn(async move {
                        kraken_book_data_feed(symbols, tx_feed, log, log_ctx).await;
                    }); 
                }
                FeedType::Ticks => {
                    let log = Arc::clone(&log);
                    let log_ctx = log_ctx.clone();
                    tokio::spawn(async move {
                        kraken_ticker_data_feed(symbols, tx_feed, log, log_ctx).await;
                    }); 
                }
                
                _ => {}
            } 
            let log = Arc::clone(&log);
        
            tokio::spawn(async move {
                let dub_buffer = DoubleBuffer::new(1000, 0.8);
                while let Some(msg) = rx_feed.recv().await {
                    let mut log = log.lock().unwrap();
                    if let Ok(Some(buffer)) = dub_buffer.buffer_push_and_swap(msg, log, &log_ctx) {
                        let payload = FeedPayload {
                            buffer,
                            config
                        };
                        
                        if provider_channel_tx.send(payload).is_err() {
                            log.log_error("Buffer was unable to send".to_string(), &log_ctx);
                            break;
                        }
                    }
                }
            });
        }
    }
    Ok(())
}

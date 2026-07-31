use crate::data_feeds::kraken::feeds::book::kraken_book_data_feed; 
use crate::data_feeds::kraken::feeds::ticker::kraken_ticker_data_feed;
use crate::data_feeds::kraken::feeds::trades::kraken_trade_data_feed;

use crate::logging::feed_logger::FeedLogger;
use crate::logging::feed_logger::LoggerContext;

use crate::config::FeedType;
use crate::config::ProviderConfig; 
use crate::config::LogConfig;

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc; 
use tokio::sync::mpsc::Sender;

use crate::db::buffer::DoubleBuffer;
use crate::db::buffer::DataBuffer;


//This function is meant to allow you to call one function it then initiates a mpsc channel. This will be used to pass the tx to the the provided feed type
// once it is determined what feed type is being created then this spans a new double buffer instance 
pub fn kraken_raw_feed_channel(provider_conf: ProviderConfig, log_conf: LogConfig,  provider_channel_tx: Sender<DataBuffer>, buffer_capacity: usize, buffer_trigger: f32 ) -> Result<(), anyhow::Error>{
    
    for symbol in provider_conf.symbol_feeds {   
        let (tx_feed, mut rx_feed) = mpsc::channel::<String>(buffer_capacity);
        let symbols = vec![symbol.symbol.clone()];
        
        let symbol_name = symbol.symbol.clone();
        let provider_name = provider_conf.provider.clone();
        let log = Arc::new(Mutex::new(FeedLogger::new(log_conf.feed_log_location.clone(), provider_name.clone())?));
        let log_ctx = LoggerContext::new(symbol_name.clone(), symbol.feed_type);
        

        let symbol_provider_channel_tx = provider_channel_tx.clone(); 

        match symbol.feed_type {
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

        tokio::spawn(async move {
                let dub_buffer: DoubleBuffer = DoubleBuffer::new(buffer_capacity, buffer_trigger);
                while let Some(msg) = rx_feed.recv().await {
                    let swap_result = {
                        let mut log = log.lock().unwrap();
                        dub_buffer.buffer_push_and_swap(msg, provider_name.clone() ,symbol.clone(), &mut log, &log_ctx)
                    };

                    if let Ok(Some(buff)) = swap_result {
                        if symbol_provider_channel_tx.send(buff).await.is_err() {
                            let mut log = log.lock().unwrap();
                            log.log_error("Buffer was unable to send".to_string(), &log_ctx);
                            break;
                        }
                    }        
                }
            });
    }
    Ok(())
}

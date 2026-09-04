use crate::data_feeds::kraken::feeds::book::kraken_book_data_feed;
use crate::data_feeds::kraken::feeds::orders::kraken_order_data_feed;
use crate::data_feeds::kraken::feeds::trades::kraken_trade_data_feed;

use crate::logging::feed_logger::FeedLogger;
use crate::logging::feed_logger::LoggerContext;
use crate::logging::sys_logger::SysLogger;
use crate::logging::LogType;

use crate::config::FeedType;
use crate::config::ProviderConfig; 
use crate::config::LogConfig;

use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

use crate::db::buffer::DoubleBuffer;
use crate::db::postgresql::{PostgresDBRaw, RawRow};


//This function is meant to allow you to call one function it then initiates a mpsc channel. This will be used to pass the tx to the the provided feed type
// once it is determined what feed type is being created then this spans a new double buffer instance 
pub fn kraken_raw_feed_channel(provider_conf: ProviderConfig, log_conf: LogConfig,  db: PostgresDBRaw, buffer_capacity: usize, buffer_trigger: f32 ) -> Result<(), anyhow::Error>{
    
    for symbol in provider_conf.symbol_feeds {   
        let (tx_feed, mut rx_feed) = mpsc::channel::<String>(buffer_capacity);
        let symbols = vec![symbol.symbol.clone()];
        
        let symbol_name = symbol.symbol.clone();
        let provider_name = provider_conf.provider.clone();
        let log = Arc::new(Mutex::new(FeedLogger::new(log_conf.feed_log_location.clone(), provider_name.clone())?));
        let mut sys_log = SysLogger::new(log_conf.system_log_location.clone(), "Kraken Raw Aggregator".to_string())?;
        let log_ctx = LoggerContext::new(symbol_name.clone(), symbol.feed_type);
        

        let sym_db = db.clone(); 

        match symbol.feed_type {
            FeedType::Trades => {
                let log = Arc::clone(&log);
                let log_ctx = log_ctx.clone();
                tokio::spawn(async move {
                    kraken_trade_data_feed(symbols, tx_feed, log, log_ctx, provider_conf.reconnect_delay_secs, provider_conf.max_reconnect_attempts).await;
                }); 
            }

            FeedType::Book => {
                let log = Arc::clone(&log);
                let log_ctx = log_ctx.clone();
                tokio::spawn(async move {
                    kraken_book_data_feed(symbols, tx_feed, log, log_ctx, provider_conf.reconnect_delay_secs, provider_conf.max_reconnect_attempts).await;
                }); 
            }
            FeedType::Orders => {
                let log = Arc::clone(&log);
                let log_ctx = log_ctx.clone();
                tokio::spawn(async move {
                    kraken_order_data_feed(symbols, tx_feed, log, log_ctx, provider_conf.reconnect_delay_secs, provider_conf.max_reconnect_attempts).await;
                });
            }

        }

        tokio::spawn(async move {
                let dub_buffer: DoubleBuffer = DoubleBuffer::new(buffer_capacity, buffer_trigger);
                while let Some(msg) = rx_feed.recv().await {
                    let swap_result = {
                        let mut log = log.lock().unwrap();
                        dub_buffer.buffer_push_and_swap(msg, provider_name.clone() ,symbol.clone(), &mut log, &log_ctx)
                    };

                    if let Ok(Some(buff)) = swap_result {
                        let raw_data = match RawRow::data_buff_to_rawrows(buff) {
                            Ok(raw_data) => raw_data,
                            Err(e) => {
                                sys_log.sys_log(LogType::Error, &format!("Kraken Raw Feed Aggregator failed to convert raw date to row: {}", e));
                                break
                            }
                        };
                        match sym_db.insert_raw_data_batch(raw_data).await{
                            Ok(()) => (),
                            Err(e)=> {
                                sys_log.sys_log(LogType::Error, &format!("Kraken Raw Feed Aggregator failed to insert to DB: {}", e));
                                break
                            }
                        };
                    }        
                }
            });
    }
    Ok(())
}

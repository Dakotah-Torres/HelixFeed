use crate::data_feeds::kraken::feeds::book::kraken_book_data_feed;
use crate::data_feeds::kraken::feeds::orders::kraken_order_data_feed;
use crate::data_feeds::kraken::feeds::trades::kraken_trade_data_feed;

use crate::logging::feed_logger::FeedLogger;
use crate::logging::feed_logger::LoggerContext;
use crate::logging::sys_logger::SysLogger;

use crate::config::FeedType;
use crate::config::ProviderConfig; 
use crate::config::LogConfig;

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::db::buffer::DoubleBuffer;
use crate::db::inserter::{Inserter, RetryPolicy};
use crate::db::postgresql::PostgresDBRaw;

/// How often a partially-filled buffer gets written even if it never hits the swap trigger.
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);


//This function is meant to allow you to call one function it then initiates a mpsc channel. This will be used to pass the tx to the the provided feed type
// once it is determined what feed type is being created then this spans a new double buffer instance 
//
// Returns the JoinHandle of every DB inserter task it spawned, so feed_runner can tell them
// to flush on shutdown and wait for them to finish before the process exits.
pub fn kraken_raw_feed_channel(provider_conf: ProviderConfig, log_conf: LogConfig,  db: PostgresDBRaw, buffer_capacity: usize, buffer_trigger: f32, shutdown: watch::Receiver<bool>) -> Result<Vec<JoinHandle<()>>, anyhow::Error>{
    let mut inserters = Vec::new();

    for symbol in provider_conf.symbol_feeds {
        let (tx_feed, rx_feed) = mpsc::channel::<String>(buffer_capacity);
        let symbols = vec![symbol.symbol.clone()];
        
        let symbol_name = symbol.symbol.clone();
        let provider_name = provider_conf.provider.clone();
        let log = Arc::new(Mutex::new(FeedLogger::new(log_conf.feed_log_location.clone(), provider_name.clone())?));
        let sys_log = SysLogger::new(log_conf.system_log_location.clone(), "Kraken Raw Aggregator".to_string())?;
        let log_ctx = LoggerContext::new(symbol_name.clone(), symbol.feed_type);
        

        let sym_db = db.clone();

        // TODO(v1.1.0 metrics): FEEDS_RUNNING.inc() when each of the tokio::spawn calls below
        // fires, .dec() when that task's loop returns for good (max reconnect attempts hit,
        // receiver dropped, etc.) - no single spot today, needs wrapping each spawned future.
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

        let inserter = Inserter {
            sink: sym_db,
            buffer: DoubleBuffer::new(buffer_capacity, buffer_trigger),
            provider_name,
            symbol,
            feed_log: log,
            log_ctx,
            sys_log,
            flush_interval: FLUSH_INTERVAL,
            retry: RetryPolicy::default(),
        };
        inserters.push(tokio::spawn(inserter.run(rx_feed, shutdown.clone())));
    }
    Ok(inserters)
}

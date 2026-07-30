use crate::metrics::prometheus::{register_metrics, start_metrics_server};
use crate::config::load_config;
use crate::config::validate_config;
use crate::data_feeds::kraken::raw_feed::kraken_raw_feed_channel;
use crate::db::postgresql::{RawRow, PostgresDBRaw};
use crate::db::buffer::DataBuffer;
use crate::data_feeds::kraken::raw_feed::FeedPayload;
use tokio::sync::mpsc; 

// at the top of run_feed()

pub async fn feed_runner( config: &str) -> Result<(), anyhow::Error> {
    register_metrics();
    tokio::spawn(start_metrics_server());


    let config = load_config(config)?;
    validate_config(&config)?;
    println!("Config valid — starting feeds");

    //Run database migrations
    let db = PostgresDBRaw::new(&config.database_and_logs_conf.postgres_config);
    let (tx_provider_channel, mut rx_provider_channel) = mpsc::channel::<FeedPayload>;
    let dblogs = config.database_and_logs_conf; 

    for feed in config.feeds {
        tokio::spawn( async move {
            match feed.provider.as_str() {
                "kraken" => {
                    kraken_raw_feed_channel(feed, dblogs, tx_provider_channel);
                }
                _ => {
                    print!("Unknown provider: {}", feed.provider);
                }
            }
        });
        
    };

    while let Some(buff) = rx_provider_channel.recv().await{
        let converted_buff = RawRow::databuff_to_rawrows(buff, p);
        db::insert_raw_data_batch(converted_buff).await?;
    }; 
    tokio::signal::ctrl_c().await?;
    Ok(())
}


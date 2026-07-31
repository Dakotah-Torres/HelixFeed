use crate::metrics::prometheus::{register_metrics, start_metrics_server};
use crate::config::load_config;
use crate::config::validate_config;
use crate::data_feeds::kraken::raw_feed::kraken_raw_feed_channel;
use crate::db::postgresql::{RawRow, PostgresDBRaw};
use crate::db::buffer::DataBuffer;
use tokio::sync::mpsc; 

// at the top of run_feed()

pub async fn feed_runner(config: &str) -> Result<(), anyhow::Error> {
    register_metrics();
    tokio::spawn(start_metrics_server());

    let config = load_config(config)?;
    validate_config(&config)?;
    println!("Config valid — starting feeds");

    //Run database migrations
    let conn = PostgresDBRaw::new(&config.database_conf.postgres_config).await?;
    let (tx_provider_channel, mut rx_provider_channel) = mpsc::channel::<DataBuffer>(config.buffer_capacity);

    for provider in config.providers {
        
        let log_config = config.log_config.clone();
        let buffer_cap = config.buffer_capacity.clone(); 
        let buffer_trig = config.buffer_swap_trigger.clone();
        let provider_channel = tx_provider_channel.clone();

        tokio::spawn( async move {
            match provider.provider.as_str() {
                "kraken" => {
                    if let Err(e) = kraken_raw_feed_channel(provider, log_config, provider_channel, buffer_cap, buffer_trig) {
                        eprint!("Kraken feed setup failed: {e}");
                    }
                }
                _ => {
                    print!("Unknown provider: {}", provider.provider);
                }
            }
        });
    };

    tokio::spawn( async move {
        while let Some(data_buff) = rx_provider_channel.recv().await{
            if let Ok(converted_buff) = RawRow::data_buff_to_rawrows(data_buff){
                if let Err(e) = conn.insert_raw_data_batch(converted_buff).await {
                    eprintln!("Insert failed {e}")
                }
            };
            
        };
    });

    

    
    tokio::signal::ctrl_c().await?;
    Ok(())
}


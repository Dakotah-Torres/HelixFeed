use crate::metrics::prometheus::{register_metrics, start_metrics_server};
use crate::config::load_config;
use crate::config::validate_config;
use crate::data_feeds::kraken::feed::kraken_feed_aggrigatotor;
// at the top of run_feed()

pub async fn run_feed(config: &str) -> Result<(), anyhow::Error> {
    register_metrics();
    tokio::spawn(start_metrics_server());

    let config = load_config(config)?;
    validate_config(&config)?;
    println!("Config valid — starting feeds");

    for feed in config.feeds {
        match feed.provider.as_str() {
            "kraken" => {
                kraken_feed_aggrigatotor(feed);
            }
            _ => {
                print!("Unknown provider: {}", feed.provider);
            }
        }
    }
    tokio::signal::ctrl_c().await?;
    Ok(())
}
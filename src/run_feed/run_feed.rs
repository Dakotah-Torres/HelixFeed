use crate::logging::logger::Logger; 

pub async fn run_feed(config: &str) -> Result<(), anyhow::Error> {
    let config = crate::config::load_config(config)?;
    for feed in config.feeds {
        match feed.provider.as_str() {
            "kraken" => {
                for symbol in &feed.symbols {
                    for data_feed in &symbol.data {
                        let mut log = Logger::new(feed.log_location.clone(), feed.provider.clone(), symbol.symbol.clone(), data_feed.feed_type, data_feed.mode)?;
                        log.log_started(); 
                        println!(
                            "Kraken | {} | {:?}",
                            symbol.symbol,
                            data_feed
                        ); 
                    }
                }
            }
            _ => {
                print!("Unknown provider: {}", feed.provider);
            }
        }
    }

    Ok(())
}
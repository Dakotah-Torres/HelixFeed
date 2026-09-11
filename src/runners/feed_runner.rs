use crate::metrics::prometheus::{register_metrics, start_metrics_server};
use crate::config::load_config;
use crate::config::validate_config;
use crate::data_feeds::kraken::raw_feed::kraken_raw_feed_channel;
use crate::db::postgresql::PostgresDBRaw;
use crate::logging::sys_logger::SysLogger;
use crate::logging::LogType;

// at the top of run_feed()

pub async fn feed_runner(config: &str) -> Result<(), anyhow::Error> {
    register_metrics();
    tokio::spawn(start_metrics_server());

    let config = load_config(config)?;
    validate_config(&config)?;
    println!("Config valid — starting feeds");

    //Run database migrations
    let conn = PostgresDBRaw::new(&config.database_conf.postgres_config).await?;
    

    for provider in config.providers {
        
        let log_config = config.log_config.clone();
        let buffer_cap = config.buffer_capacity.clone(); 
        let buffer_trig = config.buffer_swap_trigger.clone();
        let provider_db_connection = conn.clone();

        tokio::spawn( async move {
            match provider.provider.as_str() {
                "kraken" => {
                    if let Err(e) = kraken_raw_feed_channel(provider, log_config.clone(), provider_db_connection, buffer_cap, buffer_trig) {
                        // This is provider setup failing before a single feed task even
                        // spawns (e.g. FeedLogger::new() couldn't open its log file) - it
                        // previously only went to eprint!, which systemd captures to the
                        // journal but nobody actually checks there. Routed into system.log
                        // so it shows up next to every other startup/system-level error,
                        // with a stderr fallback in case SysLogger::new() itself can't open
                        // the file (so the signal is never fully lost either way).
                        match SysLogger::new(log_config.system_log_location.clone(), "Kraken Provider Setup".to_string()) {
                            Ok(mut sys_log) => sys_log.sys_log(LogType::Error, &format!("Kraken feed setup failed: {e}")),
                            Err(log_err) => eprintln!("Kraken feed setup failed: {e} (and could not open system log: {log_err})"),
                        }
                    }
                }
                _ => {
                    eprintln!("Unknown provider configured: {} - no feed started for it", provider.provider);
                }
            }
        });
    };

 
    
    tokio::signal::ctrl_c().await?;
    Ok(())
}


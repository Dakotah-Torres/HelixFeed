use crate::metrics::prometheus::{register_metrics, start_metrics_server};
use crate::config::load_config;
use crate::config::validate_config;
use crate::data_feeds::kraken::raw_feed::kraken_raw_feed_channel;
use crate::db::postgresql::PostgresDBRaw;
use crate::logging::sys_logger::SysLogger;
use crate::logging::LogType;

use std::time::Duration;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::watch;

/// How long shutdown waits for every inserter to write out its remaining buffer. Kept well
/// under systemd's default TimeoutStopSec (90s) so we finish before systemd sends SIGKILL.
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(30);

pub async fn feed_runner(config: &str) -> Result<(), anyhow::Error> {
    register_metrics();
    tokio::spawn(start_metrics_server());

    let config = load_config(config)?;
    validate_config(&config)?;
    println!("Config valid — starting feeds");

    let conn = PostgresDBRaw::new(&config.database_conf.postgres_config).await?;

    // Flipped to true once on SIGTERM/SIGINT - every inserter watches it and flushes.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut inserters = Vec::new();

    for provider in config.providers {
        let log_config = config.log_config.clone();

        match provider.provider.as_str() {
            "kraken" => {
                match kraken_raw_feed_channel(provider, log_config.clone(), conn.clone(), config.buffer_capacity, config.buffer_swap_trigger, shutdown_rx.clone()) {
                    Ok(handles) => inserters.extend(handles),
                    Err(e) => {
                        // This is provider setup failing before a single feed task even
                        // spawns (e.g. FeedLogger::new() couldn't open its log file) - routed
                        // into system.log so it shows up next to every other startup/system-level
                        // error, with a stderr fallback in case SysLogger::new() itself can't open
                        // the file (so the signal is never fully lost either way).
                        match SysLogger::new(log_config.system_log_location.clone(), "Kraken Provider Setup".to_string()) {
                            Ok(mut sys_log) => sys_log.sys_log(LogType::Error, &format!("Kraken feed setup failed: {e}")),
                            Err(log_err) => eprintln!("Kraken feed setup failed: {e} (and could not open system log: {log_err})"),
                        }
                    }
                }
            }
            _ => {
                eprintln!("Unknown provider configured: {} - no feed started for it", provider.provider);
            }
        }
    }

    wait_for_shutdown_signal().await?;

    let mut sys_log = SysLogger::new(config.log_config.system_log_location.clone(), "Feed Runner".to_string()).ok();
    if let Some(log) = sys_log.as_mut() {
        log.sys_log(LogType::Info, &format!("Shutdown signal received - flushing {} DB inserters", inserters.len()));
    }

    let _ = shutdown_tx.send(true);
    let drained = tokio::time::timeout(SHUTDOWN_FLUSH_TIMEOUT, futures_util::future::join_all(inserters)).await;

    if let Some(log) = sys_log.as_mut() {
        match drained {
            Ok(_) => log.sys_log(LogType::Info, "All DB inserters flushed - exiting"),
            Err(_) => log.sys_log(LogType::Error, &format!("DB inserters did not finish flushing within {:?} - exiting anyway, unflushed rows lost", SHUTDOWN_FLUSH_TIMEOUT)),
        }
    }
    Ok(())
}

/// systemd stops services with SIGTERM, not SIGINT - `tokio::signal::ctrl_c()` alone never
/// sees it, so the process used to be killed with buffers still full. Wait for either.
async fn wait_for_shutdown_signal() -> Result<(), anyhow::Error> {
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        result = tokio::signal::ctrl_c() => result?,
        _ = sigterm.recv() => {}
    }
    Ok(())
}

use crate::archive::status::{archive_status_path, ArchiveRunStatus};
use crate::config::{FeedType, LogConfig, ProviderConfig};
use crate::db::postgresql::PostgresDBRaw;
use crate::logging::sys_logger::SysLogger;
use crate::logging::LogType;
use crate::metrics::prometheus::{
    ARCHIVE_LAST_RUN_FILES, ARCHIVE_LAST_RUN_SUCCESS, ARCHIVE_LAST_RUN_TIMESTAMP,
    FEED_LAST_RECEIVED_TIMESTAMP, FEED_UP, SYSTEMD_UNIT_ACTIVE,
};

use chrono::{DateTime, Utc};
use std::path::PathBuf;
use std::time::Duration;

/// Same threshold as the manual "check 2" in docs/production.md - the query that finally
/// caught the 2026-10-09 outage.
pub const STALE_THRESHOLD_SECS: i64 = 300;
pub const HEALTH_POLL_INTERVAL: Duration = Duration::from_secs(30);
pub const MONITORED_UNITS: [&str; 2] = ["helixfeed-normalize.timer", "helixfeed-archive.timer"];

#[derive(Debug, Clone, PartialEq)]
pub struct PipelineKey {
    pub provider: String,
    pub symbol: String,
    pub feed_type: FeedType,
}

/// One entry per configured (provider, symbol, feed_type). feed_runner has to call this
/// before its provider loop, which moves `config.providers`.
pub fn pipelines_from_config(providers: &[ProviderConfig]) -> Vec<PipelineKey> {
    providers
        .iter()
        .flat_map(|p| {
            p.symbol_feeds.iter().map(move |sf| PipelineKey {
                provider: p.provider.clone(),
                symbol: sf.symbol.clone(),
                feed_type: sf.feed_type,
            })
        })
        .collect()
}

/// Checks from the outside whether data is actually landing, rather than trusting the feed
/// tasks' own view of themselves - on 2026-10-09 the process stayed `active` for hours with
/// every feed dead. Publishes the results on /metrics and logs to system.log only when
/// something changes state (up -> down, down -> up), never once per tick.
pub struct HealthPoller {
    db: PostgresDBRaw,
    feeds: Vec<(PipelineKey, Option<bool>)>,
    units: Vec<(&'static str, Option<bool>)>,
    archive_status_path: PathBuf,
    archive_last_success: Option<bool>,
    /// So a Postgres outage logs once when it starts and once when it ends, not every 30s.
    query_failing: bool,
    sys_log: SysLogger,
}

impl HealthPoller {
    pub fn new(db: PostgresDBRaw, pipelines: Vec<PipelineKey>, log_config: &LogConfig) -> Result<Self, anyhow::Error> {
        Ok(HealthPoller {
            db,
            feeds: pipelines.into_iter().map(|p| (p, None)).collect(),
            units: MONITORED_UNITS.iter().map(|u| (*u, None)).collect(),
            archive_status_path: archive_status_path(&log_config.system_log_location),
            archive_last_success: None,
            query_failing: false,
            sys_log: SysLogger::new(log_config.system_log_location.clone(), "Health Poller".to_string())?,
        })
    }

    pub async fn run(mut self) {
        loop {
            // Sleep first: right after a restart the feeds haven't connected or flushed yet,
            // and polling immediately would log every pipeline as down on every deploy.
            tokio::time::sleep(HEALTH_POLL_INTERVAL).await;
            self.poll_feeds().await;
            self.poll_archive();
            self.poll_units().await;
        }
    }

    async fn poll_feeds(&mut self) {
        let rows = match self.db.last_received_per_pipeline().await {
            Ok(rows) => {
                if self.query_failing {
                    self.query_failing = false;
                    self.sys_log.sys_log(LogType::Info, "Feed freshness query is working again");
                }
                rows
            }
            Err(e) => {
                if !self.query_failing {
                    self.query_failing = true;
                    self.sys_log.sys_log(LogType::Error, &format!("Feed freshness query failed: {e} - feed gauges keep their last value until it recovers"));
                }
                return;
            }
        };

        let now = Utc::now();
        for (key, last_up) in self.feeds.iter_mut() {
            let latest = rows
                .iter()
                .find(|(provider, symbol, data_type, _)| *provider == key.provider && *symbol == key.symbol && data_type == key.feed_type.as_str())
                .map(|(_, _, _, received)| *received);

            let labels = [key.provider.as_str(), key.symbol.as_str(), key.feed_type.as_str()];
            if let Some(received) = latest {
                FEED_LAST_RECEIVED_TIMESTAMP.with_label_values(&labels).set(received.timestamp() as f64);
            }

            let up = is_fresh(latest, now);
            FEED_UP.with_label_values(&labels).set(if up { 1.0 } else { 0.0 });

            if *last_up != Some(up) {
                let pipeline = format!("{} | {} | {}", key.provider, key.symbol, key.feed_type.as_str());
                match (up, latest) {
                    (true, _) => self.sys_log.sys_log(LogType::Info, &format!("{pipeline} is UP")),
                    (false, Some(received)) => self.sys_log.sys_log(LogType::Error, &format!("{pipeline} is DOWN - newest row is from {received}, over {STALE_THRESHOLD_SECS}s ago")),
                    (false, None) => self.sys_log.sys_log(LogType::Error, &format!("{pipeline} is DOWN - no recent rows at all")),
                }
                *last_up = Some(up);
            }
        }
    }

    fn poll_archive(&mut self) {
        // No file yet just means the archiver hasn't run since this was deployed.
        let Ok(status) = ArchiveRunStatus::read_from(&self.archive_status_path) else {
            return;
        };

        ARCHIVE_LAST_RUN_TIMESTAMP.set(status.finished_at.timestamp() as f64);
        ARCHIVE_LAST_RUN_SUCCESS.set(if status.success { 1.0 } else { 0.0 });
        ARCHIVE_LAST_RUN_FILES.with_label_values(&["uploaded"]).set(status.summary.uploaded as f64);
        ARCHIVE_LAST_RUN_FILES.with_label_values(&["failed"]).set(status.summary.failed as f64);
        ARCHIVE_LAST_RUN_FILES.with_label_values(&["corrupt"]).set(status.summary.corrupt as f64);

        if self.archive_last_success != Some(status.success) {
            let s = status.summary;
            if status.success {
                self.sys_log.sys_log(LogType::Info, &format!("Archiver OK - run at {} uploaded {} files", status.finished_at, s.uploaded));
            } else {
                let error = status.error.as_deref().unwrap_or("see R2 Archiver lines above");
                self.sys_log.sys_log(LogType::Error, &format!("Archiver FAILING - run at {}: {} uploaded, {} failed, {} moved to corrupt ({})", status.finished_at, s.uploaded, s.failed, s.corrupt, error));
            }
            self.archive_last_success = Some(status.success);
        }
    }

    async fn poll_units(&mut self) {
        for (unit, last_active) in self.units.iter_mut() {
            let active = unit_is_active(unit).await;
            SYSTEMD_UNIT_ACTIVE.with_label_values(&[*unit]).set(if active { 1.0 } else { 0.0 });

            if *last_active != Some(active) {
                if active {
                    self.sys_log.sys_log(LogType::Info, &format!("systemd unit {unit} is active"));
                } else {
                    self.sys_log.sys_log(LogType::Warn, &format!("systemd unit {unit} is NOT active - its job is not being scheduled"));
                }
                *last_active = Some(active);
            }
        }
    }
}

fn is_fresh(last_received: Option<DateTime<Utc>>, now: DateTime<Utc>) -> bool {
    last_received.is_some_and(|received| (now - received).num_seconds() <= STALE_THRESHOLD_SECS)
}

/// Read-only query, needs no root. A unit that isn't installed reports "inactive", and a
/// machine without systemctl fails to spawn it - both read as not active, never a panic.
async fn unit_is_active(unit: &str) -> bool {
    let output = tokio::process::Command::new("systemctl")
        .args(["show", unit, "--property=ActiveState", "--value"])
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => parse_active_state(&String::from_utf8_lossy(&out.stdout)),
        _ => false,
    }
}

fn parse_active_state(output: &str) -> bool {
    output.trim() == "active"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Market, SymbolConfig};

    #[test]
    fn parse_active_state_only_accepts_active() {
        assert!(parse_active_state("active\n"));
        assert!(!parse_active_state("inactive\n"));
        assert!(!parse_active_state("activating\n"));
        assert!(!parse_active_state(""));
    }

    #[test]
    fn freshness_uses_the_five_minute_threshold() {
        let now = Utc::now();
        assert!(is_fresh(Some(now - chrono::Duration::seconds(30)), now));
        assert!(is_fresh(Some(now - chrono::Duration::seconds(STALE_THRESHOLD_SECS)), now));
        assert!(!is_fresh(Some(now - chrono::Duration::seconds(STALE_THRESHOLD_SECS + 1)), now));
        assert!(!is_fresh(None, now), "no rows at all must read as down, not up");
    }

    #[test]
    fn pipelines_are_flattened_per_symbol_and_feed_type() {
        let feed = |symbol: &str, feed_type| SymbolConfig { markets: Market::Crypto, symbol: symbol.to_string(), feed_type };
        let providers = vec![ProviderConfig {
            provider: "kraken".to_string(),
            reconnect_delay_secs: 5,
            max_reconnect_attempts: 5,
            symbol_feeds: vec![feed("BTC/USD", FeedType::Trades), feed("BTC/USD", FeedType::Book), feed("ETC/USD", FeedType::Orders)],
        }];

        let pipelines = pipelines_from_config(&providers);

        assert_eq!(pipelines.len(), 3);
        assert!(pipelines.iter().all(|p| p.provider == "kraken"));
        assert_eq!(pipelines[2], PipelineKey { provider: "kraken".to_string(), symbol: "ETC/USD".to_string(), feed_type: FeedType::Orders });
    }
}

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// The archive job is a systemd-timer oneshot that exits after every run, so the daemon's
/// :9091 can never scrape it directly. It leaves its last result in this file instead (next to
/// system.log), and the daemon's health poller republishes it as metrics.
pub fn archive_status_path(system_log_location: &str) -> PathBuf {
    Path::new(system_log_location).with_file_name("archive_status.json")
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArchiveRunSummary {
    pub uploaded: u32,
    pub failed: u32,
    pub corrupt: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArchiveRunStatus {
    pub finished_at: DateTime<Utc>,
    pub success: bool,
    pub summary: ArchiveRunSummary,
    pub error: Option<String>,
}

impl ArchiveRunStatus {
    /// `archiver()` returns Ok even when every upload failed (it logs and moves on), so a run
    /// only counts as a success if nothing failed or got quarantined - otherwise broken R2
    /// credentials would report "success" every hour.
    pub fn from_result(result: &Result<ArchiveRunSummary, anyhow::Error>) -> Self {
        match result {
            Ok(summary) => ArchiveRunStatus {
                finished_at: Utc::now(),
                success: summary.failed == 0 && summary.corrupt == 0,
                summary: *summary,
                error: None,
            },
            Err(e) => ArchiveRunStatus {
                finished_at: Utc::now(),
                success: false,
                summary: ArchiveRunSummary::default(),
                error: Some(e.to_string()),
            },
        }
    }

    /// Written to a temp file and renamed into place, so the daemon can never read a
    /// half-written file mid-poll.
    pub fn write_to(&self, path: &Path) -> Result<(), anyhow::Error> {
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, serde_json::to_string_pretty(self)?)?;
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn read_from(path: &Path) -> Result<Self, anyhow::Error> {
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_file_sits_next_to_system_log() {
        assert_eq!(archive_status_path("logs/system.log"), PathBuf::from("logs/archive_status.json"));
    }

    #[test]
    fn run_with_failures_is_not_a_success() {
        let clean = ArchiveRunStatus::from_result(&Ok(ArchiveRunSummary { uploaded: 3, failed: 0, corrupt: 0 }));
        let failed = ArchiveRunStatus::from_result(&Ok(ArchiveRunSummary { uploaded: 2, failed: 1, corrupt: 0 }));
        let errored = ArchiveRunStatus::from_result(&Err(anyhow::anyhow!("R2 unreachable")));

        assert!(clean.success);
        assert!(!failed.success);
        assert!(!errored.success);
        assert_eq!(errored.error.as_deref(), Some("R2 unreachable"));
    }

    #[test]
    fn status_round_trips_through_the_file() {
        let path = std::env::temp_dir().join(format!("helixfeed-test-archive-status-{}.json", std::process::id()));
        let status = ArchiveRunStatus::from_result(&Ok(ArchiveRunSummary { uploaded: 4, failed: 1, corrupt: 1 }));

        status.write_to(&path).unwrap();
        let read_back = ArchiveRunStatus::read_from(&path).unwrap();
        std::fs::remove_file(&path).unwrap();

        assert_eq!(read_back, status);
    }
}

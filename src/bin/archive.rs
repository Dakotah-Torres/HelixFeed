use helix_feed::archive::r2::{R2Archiver, PARQUET_ARCHIVE_FAILD, PARQUET_ARCHIVE_CORRUPT};
use helix_feed::archive::status::{archive_status_path, ArchiveRunStatus, ArchiveRunSummary};
use helix_feed::config::{load_config, R2Config};
use helix_feed::logging::sys_logger::SysLogger;

/// Standalone archive job — separate process from ingestion and normalization, same
/// reasoning as normalize.rs: a bug or crash in here can never take down live data
/// collection or the normalizer. Run via a systemd timer, driven by R2Config.upload_schedule.
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    let config = load_config("helix_config.yml")?;

    let r2_conf = match &config.database_conf.r2 {
        Some(r2_conf) => r2_conf,
        None => {
            println!("R2 archiving not configured - nothing to do.");
            return Ok(());
        }
    };

    let system_log = &config.log_config.system_log_location;
    let result = run(r2_conf, system_log).await;

    // Written before `result?` so a failed run still leaves its outcome for the daemon to report.
    let status_path = archive_status_path(system_log);
    if let Err(e) = ArchiveRunStatus::from_result(&result).write_to(&status_path) {
        eprintln!("Could not write archive status to {}: {e}", status_path.display());
    }

    let summary = result?;
    println!("Archive run complete: {} uploaded, {} failed, {} moved to corrupt.", summary.uploaded, summary.failed, summary.corrupt);
    Ok(())
}

async fn run(r2_conf: &R2Config, system_log: &str) -> Result<ArchiveRunSummary, anyhow::Error> {
    std::fs::create_dir_all(PARQUET_ARCHIVE_FAILD)?;
    std::fs::create_dir_all(PARQUET_ARCHIVE_CORRUPT)?;

    let logger = SysLogger::new(system_log.to_string(), "R2 Archiver".to_string())?;

    let mut archiver = R2Archiver::new(r2_conf, logger)?;
    archiver.archiver().await
}

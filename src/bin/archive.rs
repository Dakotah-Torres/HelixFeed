use helix_feed::archive::r2::{R2Archiver, PARQUET_ARCHIVE_FAILD, PARQUET_ARCHIVE_CORRUPT};
use helix_feed::config::load_config;
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

    std::fs::create_dir_all(PARQUET_ARCHIVE_FAILD)?;
    std::fs::create_dir_all(PARQUET_ARCHIVE_CORRUPT)?;

    let logger = SysLogger::new(config.log_config.system_log_location.clone(), "R2 Archiver".to_string())?;

    let mut archiver = R2Archiver::new(r2_conf, logger)?;
    archiver.archiver().await?;

    println!("Archive run complete.");
    Ok(())
}

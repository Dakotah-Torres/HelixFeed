use helix_feed::config::load_config;
use helix_feed::normalizer::run_all;
use sqlx::postgres::PgPoolOptions;

/// Standalone daily normalization job — separate process from the ingestion daemon,
/// on purpose: a bug or crash in here can never take down live data collection.
/// Run via a systemd timer / cron, not as part of `feed_runner`.
#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    let config = load_config("helix_config.yml")?;
    let pg = &config.database_conf.postgres_config;

    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        pg.user, pg.password, pg.host, pg.port, pg.database
    );

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await?;

    run_all(&pool).await?;

    println!("Normalization run complete.");
    Ok(())
}

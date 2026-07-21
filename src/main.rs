use helix_feed::runners::run_feed::run_feed;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    run_feed("helix_config.yml").await?;
    Ok(())
}
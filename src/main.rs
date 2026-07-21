use helix_feed::runners::feed_runner::feed_runner;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    feed_runner("helix_config.yml").await?;
    Ok(())
}
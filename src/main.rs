use helix_feed::connectors::kraken::ticker;
use helix_feed::config;

//#[tokio::main]
fn main() -> Result<(), anyhow::Error> {
    // 1. Print out exactly where the program is running right now
    let current_dir = std::env::current_dir()?;
    println!("Program is running from directory: {:?}", current_dir);

    // 2. Check if the file exists from the program's perspective
    let file_exists = std::path::Path::new("helix_config.yml").exists();
    println!("Does helix_config.yml exist here? {}", file_exists);

    // Your original code
    let config = config::load_config("helix_config.yml")?;
    println!("{:#?}", config);

    Ok(())
}
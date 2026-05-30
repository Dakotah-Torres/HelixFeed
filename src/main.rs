use helix_feed::connectors::kraken::ticker;

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect(".env file not found");
    println!(" ------ Engine Starting ------ ");
    ticker::kraken_ticker_data_feed().await;
}
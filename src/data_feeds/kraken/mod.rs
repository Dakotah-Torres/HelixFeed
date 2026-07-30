pub mod feeds;
pub mod raw_feed;
pub mod connection;

use std::sync::{Arc, Mutex}; 
use crate::logging::feed_logger::FeedLogger; 

pub fn create_logger(log_path: String) -> Arc<Mutex<FeedLogger>> {
    let provider = "kraken".to_string();
    let log = FeedLogger::new(log_path, provider).unwrap();
    Arc::new(Mutex::new(log))
}
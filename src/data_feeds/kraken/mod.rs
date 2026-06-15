pub mod feeds;
pub mod feed;
pub mod connection;

use std::sync::{Arc, Mutex}; 
use crate::logging::logger::Logger; 

pub fn create_logger(log_path: String) -> Arc<Mutex<Logger>> {
    let provider = "kraken".to_string();
    let log = Logger::new(log_path, provider).unwrap();
    Arc::new(Mutex::new(log))
}
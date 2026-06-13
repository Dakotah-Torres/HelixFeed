use crate::config::FeedType;
use crate::config::Mode;
use std::fs::{File, OpenOptions}; 
use std::io::BufWriter;
use std::io::Write;
use chrono::Local;


pub struct Logger {
    pub log_path: String, 
    pub provider: String, 
    pub symbol: String, 
    pub feed_type: FeedType,
    pub mode: Mode, 
    writer: BufWriter<File>
}

impl Logger {
    pub fn new(log_path: String, provider:String, symbol:String, feed_type:FeedType, mode:Mode) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?; 

        let writer = BufWriter::new(file); 
    
        Ok(Logger {
            log_path,
            provider,
            symbol, 
            feed_type,
            mode,
            writer,
        })
    }

    pub fn log_started(&mut self) {
        let line = format!(
            "[{}] [INFO] {} | {} | {:?} | {:?} | started\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            self.symbol,
            self.feed_type, 
            self.mode
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_stopped(&mut self) {
        let line = format!(
            "[{}] [INFO] {} | {} | {:?} | {:?} | stopped\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            self.symbol,
            self.feed_type, 
            self.mode
        ); 

        let _ = self.writer.write_all(line.as_bytes());

    }

    pub fn log_error(&mut self, reason: String) {
        let line = format!(
            "[{}] [ERROR] {} | {} | {:?} | {:?} | {}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            self.symbol,
            self.feed_type, 
            self.mode,
            reason
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_reconnecting(&mut self, attempt: u32, max: u32 ) {
        let line = format!(
            "[{}] [WARN] {} | {} | {:?} | {:?} | reconnecting attempt {}/{}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            self.symbol,
            self.feed_type,
            self.mode, 
            attempt, 
            max
        ); 
        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_reconnected(&mut self) {
        let line = format!(
            "[{}] [WARN] {} | {} | {:?} | {:?} | reconnected\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            self.symbol,
            self.feed_type,
            self.mode
        );
        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_reconnect_failed(&mut self) {
        let line = format!(
            "[{}] [WARN] {} | {} | {:?} | {:?} | reconnect failed\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            self.symbol,
            self.feed_type,
            self.mode 
        ); 
        let _ = self.writer.write_all(line.as_bytes());
    }
}
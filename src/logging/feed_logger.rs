use crate::config::FeedType;
use crate::config::Mode;
use std::fs::{File, OpenOptions}; 
use std::io::BufWriter;
use std::io::Write;
use chrono::Local;


pub struct FeedLogger {
    pub log_path: String, 
    pub provider: String,  
    writer: BufWriter<File>
}

#[derive(Clone)]
pub struct LoggerContext {
    pub symbol: String, 
    pub feed_type: FeedType, 
    pub mode: Mode
}

impl LoggerContext {
    pub fn new(symbol: String, feed_type: FeedType, mode: Mode) -> Self {
        LoggerContext { symbol, feed_type, mode }
    }
}

impl FeedLogger {
    pub fn new(log_path: String, provider:String) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?; 

        let writer = BufWriter::new(file); 
    
        Ok(FeedLogger {
            log_path,
            provider,
            writer,
        })
    }

    pub fn log_started(&mut self , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [INFO] {} | {} | {:?} | {:?} | started\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type, 
            ctx.mode
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_stopped(&mut self , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [INFO] {} | {} | {:?} | {:?} | stopped\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type, 
            ctx.mode
        ); 

        let _ = self.writer.write_all(line.as_bytes());

    }

    pub fn log_error(&mut self, reason: String , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [ERROR] {} | {} | {:?} | {:?} | {}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type, 
            ctx.mode,
            reason
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_info(&mut self, info: String , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [INFO] {} | {} | {:?} | {:?} | {}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type, 
            ctx.mode,
            info,
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }
    
    pub fn log_success(&mut self, success_msg: String , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [INFO] {} | {} | {:?} | {:?} | {}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type, 
            ctx.mode,
            success_msg,
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_reconnecting(&mut self, attempt: u32, max: u32  , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [WARN] {} | {} | {:?} | {:?} | reconnecting attempt {}/{}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type,
            ctx.mode, 
            attempt, 
            max
        ); 
        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_reconnected(&mut self , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [WARN] {} | {} | {:?} | {:?} | reconnected\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type,
            ctx.mode
        );
        let _ = self.writer.write_all(line.as_bytes());
    }

    pub fn log_reconnect_failed(&mut self , ctx: &LoggerContext) {
        let line = format!(
            "[{}] [WARN] {} | {} | {:?} | {:?} | reconnect failed\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            self.provider, 
            ctx.symbol,
            ctx.feed_type,
            ctx.mode 
        ); 
        let _ = self.writer.write_all(line.as_bytes());
    }

}
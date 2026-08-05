use crate::config::FeedType;
use crate::logging::LogType;
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
}

impl LoggerContext {
    pub fn new(symbol: String, feed_type: FeedType) -> Self {
        LoggerContext { symbol, feed_type }
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

    pub fn feed_log(&mut self, log_type: LogType, message: &str, ctx: &LoggerContext) {
        let label = match log_type {
            LogType::Debug => "DEBUG",
            LogType::Error => "ERROR",
            LogType::Info => "INFO",
            LogType::Warn => "WARN"
        };

        let line = format!(
            "[{}] [{}] | {} | {} | {:?} | {}\n",
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            label,
            self.provider,
            ctx.symbol,
            ctx.feed_type,
            message
        );

        let _ = self.writer.write_all(line.as_bytes());
    }
}

use std::fs::{File, OpenOptions};
use std::io::BufWriter;
use std::io::Write;
use chrono::Local;
use crate::logging::LogType;

pub struct SysLogger {
    pub log_path: String, 
    pub system: String,  
    writer: BufWriter<File>
}

impl SysLogger {
    pub fn new(log_path: String, system: String) -> Result<Self, anyhow::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let writer = BufWriter::new(file);

        Ok(SysLogger{
            log_path,
            system,
            writer
        })
    }

    pub fn sys_log(&mut self, log_type: LogType, message: &str) {
        let label = match log_type {
            LogType::Debug => "DEBUG",
            LogType::Error => "ERROR",
            LogType::Info => "INFO",
            LogType::Warn => "WARN"
        };
        
        let line = format!(
            "[{}] [{}] | {} | {}\n", 
            Local::now().format("%Y-%m-%d %H:%M:%S"),
            label,
            self.system,
            message
        ); 

        let _ = self.writer.write_all(line.as_bytes());
    }
}
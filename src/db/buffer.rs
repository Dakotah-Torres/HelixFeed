
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time:: {SystemTime, UNIX_EPOCH};
use std::sync::{ Mutex};
use crate::logging::feed_logger::FeedLogger; 
use crate::logging::feed_logger::LoggerContext;



pub struct DataBuffer {
    messages: Vec<String>,
    capacity: usize,
    cap_trigger: f32,
}

struct DataStore {
    active: DataBuffer,
    standby: DataBuffer,
}

pub struct DoubleBuffer {
    inner_store: Mutex<DataStore>,
}


impl DataBuffer {
    pub fn new(capacity: usize, trigger: f32) -> Self {
        DataBuffer{
            messages: Vec::with_capacity(capacity),
            capacity: capacity, 
            cap_trigger: trigger,
        }
    }

    pub fn push_message(&mut self, message: String){
        self.messages.push(message);
    }

    pub fn capacity_check(&self) -> usize{
        let current_cap = self.messages.len();
        current_cap
    }

    pub fn trigger_swap(&self) -> bool {
        
        self.messages.len() >= (self.capacity as f32 * self.cap_trigger) as usize 
    }

    pub fn get_messages(&self) -> Vec<String> {
        self.messages.clone()
    }
    // pub fn save_and_clean(&mut self, stream_type: &str, symbol: &str, db_location: &str) -> Result<String, anyhow::Error> {
    //     let timestamp = SystemTime::now()
    //         .duration_since(UNIX_EPOCH)
    //         .expect("Time went backwards")
    //         .as_millis()
    //         .to_string();
        
        
    //     let file_path = format!("{}/{}_{}_{}.bin", db_location, stream_type, symbol, timestamp);
    //     let file = File::create(&file_path)?;

    //     let mut writer = BufWriter::new(file);

    //     bincode::serialize_into(&mut writer, &self.messages).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    //     writer.flush()?;
    //     self.messages.clear();
    //     Ok(file_path)
    // }
}


impl DoubleBuffer {
    pub fn new(capacity: usize, trigger: f32 ) -> Self {
        let active = DataBuffer::new(capacity, trigger);
        let standby = DataBuffer::new(capacity, trigger);
        let inner_store = DataStore{active, standby};

        DoubleBuffer {
            inner_store: Mutex::new(inner_store), 
        }
    }

    pub fn buffer_push_and_swap(&self, message: String, logger: &mut FeedLogger, ctx: &LoggerContext) -> Result<Option<DataBuffer>, anyhow::Error> {
        logger.log_info("Initiating Push To Buffer".to_string(), ctx);
        
        let mut buffer = self.inner_store.lock().unwrap();
        let inner_store: &mut DataStore = &mut *buffer;
        inner_store.active.push_message(message);

        if inner_store.active.trigger_swap() {
            logger.log_info(format!("Buffer Trigger Limit Reached "), ctx);
            std::mem::swap(&mut inner_store.active, &mut inner_store.standby);

            let capacity = inner_store.standby.capacity;
            let trigger = inner_store.standby.cap_trigger;
            let return_data = std::mem::replace(&mut inner_store.standby  , DataBuffer::new(capacity, trigger));
            Ok(Some(return_data))
        }
        else {
            Ok(None)
        }
    }
}
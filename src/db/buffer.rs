
use std::fs::File;
use std::io::{BufWriter, Write};
use std::time:: {SystemTime, UNIX_EPOCH};
use std::sync::{ Mutex};

pub struct DataBuffer {
    messages: Vec<String>,
    capacity: usize,
    cap_trigger: f32,
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

    pub fn save_and_clean(&mut self, stream_type: &str, symbol: &str) -> Result<String, anyhow::Error> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
            .to_string();
        
        
        let file_path = format!("{}_{}_{}.bin", stream_type, symbol, timestamp);
        let file = File::create(&file_path)?;

        let mut writer = BufWriter::new(file);

        bincode::serialize_into(&mut writer, &self.messages).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

        writer.flush()?;
        self.messages.clear();
        Ok(file_path)
    }
}

struct Inner {
    active: DataBuffer,
    standby: DataBuffer,
}

pub struct DoubleBuffer {
    inner: Mutex<Inner>,
}



impl DoubleBuffer {
    pub fn new(capacity: usize, trigger: f32 ) -> Self {
        let active = DataBuffer::new(capacity, trigger);
        let standby = DataBuffer::new(capacity, trigger);
        let inner = Inner{active, standby};

        DoubleBuffer {
            inner: Mutex::new(inner), 
        }
    }

    pub fn push_swap_and_save(&self, message: String, stream_type: &str, symbol: &str) {
        let mut buffer = self.inner.lock().unwrap();
        let inner: &mut Inner = &mut *buffer;
            inner.active.push_message(message);
            if inner.active.trigger_swap() {
                std::mem::swap(&mut inner.active, &mut inner.standby);
                let _ = buffer.standby.save_and_clean(stream_type, symbol);
            }
    }
}
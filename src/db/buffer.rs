
use std::sync::{ Mutex};
use crate::logging::feed_logger::FeedLogger;
use crate::logging::feed_logger::LoggerContext;
use crate::logging::LogType;
use crate::config::FeedType;
use crate::config::SymbolConfig;




pub struct DataBuffer {
    provider: Option<String>,
    symbol: Option<String>,
    data_type: Option<FeedType>, 
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
            data_type:  None, 
            symbol:  None, 
            provider: None
        }
    }

    pub fn set_data_type(&mut self, data_type: FeedType)  {
        self.data_type = Some(data_type);
        
    }

    pub fn set_provider(&mut self, provider: String)  {
        self.provider = Some(provider);
        
    }

    pub fn set_symbol(&mut self, symbol: impl Into<String>) {
        self.symbol = Some(symbol.into());
        
    }

    pub fn push_message(&mut self, message: String) {
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

    pub fn get_data_type(&self) -> Option<FeedType> {
            self.data_type.clone()        
    }

    pub fn get_symbol(&self) -> Option<String> {
        self.symbol.clone()
    }

    pub fn get_provider(&self) -> Option<String> {
        self.provider.clone()
    }

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

    pub fn buffer_push_and_swap(&self, message: String, provider_name: String, symbol_conf: SymbolConfig,  logger: &mut FeedLogger, ctx: &LoggerContext) -> Result<Option<DataBuffer>, anyhow::Error> {
        logger.feed_log(LogType::Info, "Initiating Push To Buffer", ctx);
        
        let mut buffer = self.inner_store.lock().unwrap();
        let inner_store: &mut DataStore = &mut *buffer;
        inner_store.active.push_message(message);
        inner_store.active.set_symbol(symbol_conf.symbol);
        inner_store.active.set_data_type(symbol_conf.feed_type);
        inner_store.active.set_provider(provider_name);

        if inner_store.active.trigger_swap() {
            logger.feed_log(LogType::Info, "Buffer Trigger Limit Reached", ctx);
            std::mem::swap(&mut inner_store.active, &mut inner_store.standby);

            let capacity = inner_store.standby.capacity;
            let trigger = inner_store.standby.cap_trigger;
            let return_buffer = std::mem::replace(&mut inner_store.standby  , DataBuffer::new(capacity, trigger));
            Ok(Some(return_buffer))
        }
        else {
            Ok(None)
        }
    }
}
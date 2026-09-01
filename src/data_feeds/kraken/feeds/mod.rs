pub mod book;
pub mod orders;
pub mod trades;

use lazy_static::lazy_static; 
use std::sync::Mutex;

use crate::config::FeedType; 
use crate::data_feeds::traits::{ReplayLevel, Resolution, ReplayCapability};


lazy_static! {
    pub static ref FEED_REGISTRY: FeedRegistry = FeedRegistry::new();

}

pub struct FeedRegistry {
    feeds: Mutex<Vec<FeedType>>
}

impl FeedRegistry {
    pub fn new() -> Self {
        FeedRegistry { feeds: Mutex::new(vec![]) }
    }

    pub fn register(&self, feed_type: FeedType) {
        self.feeds.lock().unwrap().push(feed_type);
    }

    pub fn supported(&self) -> Vec<FeedType> {
        self.feeds.lock().unwrap().clone() 
    }
}

pub fn replay_capability() -> ReplayCapability {
    ReplayCapability {
        level: ReplayLevel::TickAndBook,
        resolution: Resolution::Millisecond,
    }
}
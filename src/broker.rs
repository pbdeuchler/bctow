#[cfg(feature = "metrics")]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::BctowConfig;
use crate::topic::TopicRegistry;

/// Shared state between `Producer` and `ConsumerContext`.
pub(crate) struct BrokerInner {
    pub(crate) config: BctowConfig,
    pub(crate) topics: TopicRegistry,
    #[cfg(feature = "metrics")]
    pub(crate) messages_published: AtomicU64,
    #[cfg(feature = "metrics")]
    pub(crate) messages_delivered: AtomicU64,
}

impl BrokerInner {
    pub(crate) fn new(config: BctowConfig) -> Self {
        assert!(
            config.ring_buffer_capacity > 0,
            "ring buffer capacity must be > 0"
        );

        Self {
            topics: TopicRegistry::new(config.ring_buffer_capacity, config.max_payload_size),
            #[cfg(feature = "metrics")]
            messages_published: AtomicU64::new(0),
            #[cfg(feature = "metrics")]
            messages_delivered: AtomicU64::new(0),
            config,
        }
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn inc_published(&self) {
        self.messages_published.fetch_add(1, Ordering::Relaxed);
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn inc_delivered(&self) {
        self.messages_delivered.fetch_add(1, Ordering::Relaxed);
    }
}

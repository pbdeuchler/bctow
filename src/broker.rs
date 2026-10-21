#[cfg(feature = "metrics")]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::BctowConfig;
use crate::topic::TopicRegistry;

/// Shared state between `Producer` and `ConsumerContext`.
pub(crate) struct BrokerInner<T> {
    #[allow(dead_code)]
    pub(crate) config: BctowConfig,
    pub(crate) topics: TopicRegistry<T>,
    #[cfg(feature = "metrics")]
    pub(crate) messages_published: AtomicU64,
    #[cfg(feature = "metrics")]
    pub(crate) messages_delivered: AtomicU64,
}

impl<T: Copy + Send + Sync + 'static> BrokerInner<T> {
    pub(crate) fn new(config: BctowConfig) -> Self {
        assert!(
            config.ring_buffer_capacity > 0,
            "ring buffer capacity must be > 0"
        );

        Self {
            topics: TopicRegistry::new(config.ring_buffer_capacity),
            #[cfg(feature = "metrics")]
            messages_published: AtomicU64::new(0),
            #[cfg(feature = "metrics")]
            messages_delivered: AtomicU64::new(0),
            config,
        }
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn inc_published(&self) {
        // Only the single producer updates this counter; snapshots only load it.
        let next = self
            .messages_published
            .load(Ordering::Relaxed)
            .wrapping_add(1);
        self.messages_published.store(next, Ordering::Relaxed);
    }

    #[cfg(feature = "metrics")]
    pub(crate) fn inc_delivered(&self) {
        self.messages_delivered.fetch_add(1, Ordering::Relaxed);
    }
}

#[cfg(all(test, feature = "metrics"))]
mod tests {
    use super::*;

    #[test]
    fn published_metrics_count_and_wrap_without_changing_deliveries() {
        let inner = BrokerInner::<u64>::new(BctowConfig::default());
        for count in [0, 1, u64::MAX - 1, u64::MAX] {
            inner.messages_published.store(count, Ordering::Relaxed);
            inner.inc_published();
            assert_eq!(
                inner.messages_published.load(Ordering::Relaxed),
                count.wrapping_add(1)
            );
            assert_eq!(inner.messages_delivered.load(Ordering::Relaxed), 0);
        }
    }
}

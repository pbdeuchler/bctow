use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[cfg(feature = "metrics")]
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::BctowConfig;
use crate::ring::RingBuffer;
use crate::topic::{TopicId, TopicInterner};

/// Shared state between Producer and ConsumerContext.
/// Holds per-topic ring buffers and the topic interner.
pub(crate) struct BrokerInner<T: Send + Sync + 'static> {
    pub(crate) config: BctowConfig,
    pub(crate) interner: TopicInterner,
    pub(crate) rings: RwLock<HashMap<TopicId, Arc<RingBuffer<T>>>>,
    #[cfg(feature = "metrics")]
    pub(crate) messages_published: AtomicU64,
    #[cfg(feature = "metrics")]
    pub(crate) messages_delivered: AtomicU64,
}

impl<T: Send + Sync + 'static> BrokerInner<T> {
    pub(crate) fn new(config: BctowConfig) -> Self {
        BrokerInner {
            rings: RwLock::new(HashMap::new()),
            interner: TopicInterner::new(),
            #[cfg(feature = "metrics")]
            messages_published: AtomicU64::new(0),
            #[cfg(feature = "metrics")]
            messages_delivered: AtomicU64::new(0),
            config,
        }
    }

    /// Get the ring buffer for `id`, creating one if it doesn't exist.
    pub(crate) fn get_or_create_ring(&self, id: TopicId) -> Arc<RingBuffer<T>> {
        // Fast path: read lock only.
        {
            let guard = self.rings.read().expect("rings read lock poisoned");
            if let Some(ring) = guard.get(&id) {
                return Arc::clone(ring);
            }
        }

        // Slow path: write lock to insert.
        let mut guard = self.rings.write().expect("rings write lock poisoned");
        // Re-check after acquiring write lock.
        if let Some(ring) = guard.get(&id) {
            return Arc::clone(ring);
        }
        let ring = Arc::new(RingBuffer::new(self.config.ring_buffer_capacity));
        guard.insert(id, Arc::clone(&ring));
        ring
    }

    /// Look up the ring buffer for `id` without creating one.
    pub(crate) fn get_ring(&self, id: TopicId) -> Option<Arc<RingBuffer<T>>> {
        let guard = self.rings.read().expect("rings read lock poisoned");
        guard.get(&id).map(Arc::clone)
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

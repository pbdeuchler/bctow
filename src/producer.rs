use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::broker::BrokerInner;
use crate::error::BctowError;
use crate::topic::{MatchKind, TopicEntry, TopicId, TopicKey};

/// Single-producer handle. `Producer` is `Send` but not `Sync`.
pub struct Producer {
    pub(crate) inner: Arc<BrokerInner>,
    pub(crate) cached_rings: RefCell<Vec<Option<Arc<crate::ring::RingBuffer>>>>,
    pub(crate) _not_sync: PhantomData<Cell<()>>,
}

impl Producer {
    /// Register `topic` if needed and return its stable broker-branded id.
    pub fn register_topic<K: TopicKey>(&self, topic: K) -> TopicId {
        debug_assert_eq!(topic.match_kind(), MatchKind::Exact);
        let entry = self.inner.topics.get_or_create(topic.topic_str());
        self.cache_entry(&entry);
        entry.id()
    }

    /// Publish `payload` to `topic`, creating the topic on first use.
    pub fn publish<K: TopicKey>(
        &self,
        topic: K,
        payload: impl AsRef<[u8]>,
    ) -> Result<u64, BctowError> {
        debug_assert_eq!(topic.match_kind(), MatchKind::Exact);
        let entry = self.inner.topics.get_or_create(topic.topic_str());
        self.cache_entry(&entry);
        self.publish_entry(&entry, payload.as_ref())
    }

    /// Publish directly to a known topic id.
    ///
    /// Once a topic has been cached in this producer, steady-state `publish_id`
    /// avoids broker registry locks.
    pub fn publish_id(&self, topic: TopicId, payload: impl AsRef<[u8]>) -> Result<u64, BctowError> {
        if topic.broker() != self.inner.topics.broker_id() {
            return Err(BctowError::TopicBrokerMismatch(topic));
        }

        let payload = payload.as_ref();
        self.ensure_payload_size(payload)?;

        let index = topic.index() as usize;
        {
            let cache = self.cached_rings.borrow();
            if let Some(Some(ring)) = cache.get(index) {
                let seq = ring.push(payload);
                #[cfg(feature = "metrics")]
                self.inner.inc_published();
                return Ok(seq);
            }
        }

        let entry = self
            .inner
            .topics
            .get(topic)
            .ok_or_else(|| BctowError::TopicNotFound(topic.to_string()))?;
        self.cache_entry(&entry);
        self.publish_entry(&entry, payload)
    }

    /// Resolve a topic id back to its original topic string.
    pub fn topic_name(&self, id: TopicId) -> Option<String> {
        self.inner.topics.name(id)
    }

    fn publish_entry(&self, entry: &Arc<TopicEntry>, payload: &[u8]) -> Result<u64, BctowError> {
        self.ensure_payload_size(payload)?;
        let seq = entry.ring().push(payload);
        #[cfg(feature = "metrics")]
        self.inner.inc_published();
        Ok(seq)
    }

    fn ensure_payload_size(&self, payload: &[u8]) -> Result<(), BctowError> {
        let max = self.inner.config.max_payload_size;
        if payload.len() > max {
            return Err(BctowError::PayloadTooLarge {
                size: payload.len(),
                max,
            });
        }
        Ok(())
    }

    fn cache_entry(&self, entry: &Arc<TopicEntry>) {
        let index = entry.id().index() as usize;
        let mut cache = self.cached_rings.borrow_mut();
        if cache.len() <= index {
            cache.resize_with(index + 1, || None);
        }
        if cache[index].is_none() {
            cache[index] = Some(Arc::clone(entry.ring()));
        }
    }
}

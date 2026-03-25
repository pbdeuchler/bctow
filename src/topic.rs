use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use crate::ring::RingBuffer;

static NEXT_BROKER_ID: AtomicU64 = AtomicU64::new(1);

/// Compact identifier for a topic.
///
/// Topic ids are broker-branded so an id obtained from one broker instance
/// cannot be used against another.
#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct TopicId {
    broker: u64,
    index: u32,
}

impl TopicId {
    pub fn broker(self) -> u64 {
        self.broker
    }

    pub fn index(self) -> u32 {
        self.index
    }
}

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.broker, self.index)
    }
}

/// Describes how a topic key should be matched against subscriptions.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum MatchKind {
    Exact,
}

/// Anything that can be used to identify a topic for publish/subscribe.
pub trait TopicKey {
    fn topic_str(&self) -> &str;
    fn match_kind(&self) -> MatchKind;
}

/// A topic identified by exact string equality.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ExactTopic(pub(crate) String);

impl ExactTopic {
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }
}

impl TopicKey for ExactTopic {
    fn topic_str(&self) -> &str {
        &self.0
    }

    fn match_kind(&self) -> MatchKind {
        MatchKind::Exact
    }
}

impl TopicKey for &str {
    fn topic_str(&self) -> &str {
        self
    }

    fn match_kind(&self) -> MatchKind {
        MatchKind::Exact
    }
}

impl TopicKey for String {
    fn topic_str(&self) -> &str {
        self.as_str()
    }

    fn match_kind(&self) -> MatchKind {
        MatchKind::Exact
    }
}

/// Placeholder for future pattern-based matching.
#[derive(Debug)]
pub struct TopicPattern {
    _private: (),
}

pub(crate) struct TopicEntry {
    id: TopicId,
    name: String,
    ring: Arc<RingBuffer>,
}

impl TopicEntry {
    pub(crate) fn id(&self) -> TopicId {
        self.id
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn ring(&self) -> &Arc<RingBuffer> {
        &self.ring
    }
}

struct RegistryInner {
    by_name: HashMap<String, TopicId>,
    topics: Vec<Arc<TopicEntry>>,
}

/// Thread-safe mapping between topic strings, topic ids, and per-topic rings.
pub(crate) struct TopicRegistry {
    broker_id: u64,
    ring_buffer_capacity: usize,
    max_payload_size: usize,
    inner: RwLock<RegistryInner>,
}

impl TopicRegistry {
    pub(crate) fn new(ring_buffer_capacity: usize, max_payload_size: usize) -> Self {
        Self {
            broker_id: NEXT_BROKER_ID.fetch_add(1, Ordering::Relaxed),
            ring_buffer_capacity,
            max_payload_size,
            inner: RwLock::new(RegistryInner {
                by_name: HashMap::new(),
                topics: Vec::new(),
            }),
        }
    }

    pub(crate) fn broker_id(&self) -> u64 {
        self.broker_id
    }

    pub(crate) fn get_or_create(&self, key: &str) -> Arc<TopicEntry> {
        {
            let guard = self
                .inner
                .read()
                .expect("topic registry read lock poisoned");
            if let Some(&id) = guard.by_name.get(key) {
                return Arc::clone(
                    guard
                        .topics
                        .get(id.index as usize)
                        .expect("topic registry index out of sync"),
                );
            }
        }

        let mut guard = self
            .inner
            .write()
            .expect("topic registry write lock poisoned");

        if let Some(&id) = guard.by_name.get(key) {
            return Arc::clone(
                guard
                    .topics
                    .get(id.index as usize)
                    .expect("topic registry index out of sync"),
            );
        }

        let index = guard.topics.len();
        assert!(
            index <= u32::MAX as usize,
            "topic registry overflow: exceeded u32::MAX topics"
        );

        let id = TopicId {
            broker: self.broker_id,
            index: index as u32,
        };
        let entry = Arc::new(TopicEntry {
            id,
            name: key.to_owned(),
            ring: Arc::new(RingBuffer::new(
                self.ring_buffer_capacity,
                self.max_payload_size,
            )),
        });
        guard.by_name.insert(entry.name.clone(), id);
        guard.topics.push(Arc::clone(&entry));
        entry
    }

    pub(crate) fn resolve(&self, key: &str) -> Option<Arc<TopicEntry>> {
        let guard = self
            .inner
            .read()
            .expect("topic registry read lock poisoned");
        let id = *guard.by_name.get(key)?;
        guard.topics.get(id.index as usize).map(Arc::clone)
    }

    pub(crate) fn get(&self, id: TopicId) -> Option<Arc<TopicEntry>> {
        if id.broker != self.broker_id {
            return None;
        }
        let guard = self
            .inner
            .read()
            .expect("topic registry read lock poisoned");
        guard.topics.get(id.index as usize).map(Arc::clone)
    }

    pub(crate) fn name(&self, id: TopicId) -> Option<String> {
        self.get(id).map(|entry| entry.name().to_owned())
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn topic_id_carries_broker_identity() {
        let a = TopicRegistry::new(4, 32);
        let b = TopicRegistry::new(4, 32);

        let a_id = a.get_or_create("topic").id();
        let b_id = b.get_or_create("topic").id();

        assert_ne!(a_id, b_id);
        assert_ne!(a_id.broker(), b_id.broker());
    }

    #[test]
    fn topic_key_supports_exact_topics() {
        let topic = ExactTopic::new("sensor/temp");
        assert_eq!(topic.topic_str(), "sensor/temp");
        assert_eq!(topic.match_kind(), MatchKind::Exact);
        assert_eq!("sensor/temp".match_kind(), MatchKind::Exact);
    }

    #[test]
    fn registry_resolves_existing_topics_without_duplication() {
        let registry = TopicRegistry::new(8, 64);

        let first = registry.get_or_create("alpha");
        let second = registry.get_or_create("alpha");
        let by_name = registry.resolve("alpha").unwrap();
        let by_id = registry.get(first.id()).unwrap();

        assert_eq!(first.id(), second.id());
        assert_eq!(first.id(), by_name.id());
        assert_eq!(first.id(), by_id.id());
    }

    #[test]
    fn registry_rejects_foreign_topic_ids() {
        let a = TopicRegistry::new(4, 32);
        let b = TopicRegistry::new(4, 32);

        let a_id = a.get_or_create("topic").id();
        assert!(b.get(a_id).is_none());
        assert!(b.name(a_id).is_none());
    }

    #[test]
    fn concurrent_get_or_create_returns_same_id() {
        let registry = Arc::new(TopicRegistry::new(4, 32));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let registry = Arc::clone(&registry);
                thread::spawn(move || registry.get_or_create("shared").id())
            })
            .collect();

        let ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        assert!(ids.windows(2).all(|pair| pair[0] == pair[1]));
    }
}

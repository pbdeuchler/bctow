use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::broker::BrokerInner;
use crate::config::OverflowPolicy;
use crate::error::BctowError;
use crate::ring::{Cursor, RingBuffer};
use crate::topic::{MatchKind, TopicId, TopicKey};

/// Borrowed message view backed by the client's internal scratch buffer.
#[derive(Debug, Clone, Copy)]
pub struct ReceivedMessage<'a> {
    pub topic_id: TopicId,
    pub payload: &'a [u8],
}

/// Factory for consumer clients. Clone-able and shareable across threads.
#[derive(Clone)]
pub struct ConsumerContext {
    pub(crate) inner: Arc<BrokerInner>,
    overflow_policy: OverflowPolicy,
}

impl ConsumerContext {
    pub(crate) fn new(inner: Arc<BrokerInner>, overflow_policy: OverflowPolicy) -> Self {
        Self {
            inner,
            overflow_policy,
        }
    }

    pub fn create_client(&self) -> ConsumerClient {
        ConsumerClient {
            inner: Arc::clone(&self.inner),
            overflow_policy: self.overflow_policy,
            subscriptions: HashMap::new(),
            topic_order: Vec::new(),
            next_poll_idx: 0,
            scratch: vec![0; self.inner.config.max_payload_size],
            _not_sync: PhantomData,
        }
    }

    pub fn topic_name(&self, id: TopicId) -> Option<String> {
        self.inner.topics.name(id)
    }

    #[cfg(feature = "metrics")]
    pub fn metrics(&self) -> Metrics {
        use std::sync::atomic::Ordering;

        Metrics {
            messages_published: self.inner.messages_published.load(Ordering::Relaxed),
            messages_delivered: self.inner.messages_delivered.load(Ordering::Relaxed),
        }
    }
}

struct Subscription {
    cursor: Cursor,
    ring: Arc<RingBuffer>,
}

/// A single consumer with per-topic cursors. `ConsumerClient` is `Send` but not `Sync`.
pub struct ConsumerClient {
    inner: Arc<BrokerInner>,
    overflow_policy: OverflowPolicy,
    subscriptions: HashMap<TopicId, Subscription>,
    topic_order: Vec<TopicId>,
    next_poll_idx: usize,
    scratch: Vec<u8>,
    _not_sync: PhantomData<Cell<()>>,
}

impl ConsumerClient {
    pub fn subscribe<K: TopicKey>(&mut self, topic: K) -> Result<(), BctowError> {
        debug_assert_eq!(topic.match_kind(), MatchKind::Exact);
        let entry = self
            .inner
            .topics
            .resolve(topic.topic_str())
            .ok_or_else(|| BctowError::TopicNotFound(topic.topic_str().to_owned()))?;
        self.subscribe_entry(entry)
    }

    pub fn subscribe_id(&mut self, topic: TopicId) -> Result<(), BctowError> {
        if topic.broker() != self.inner.topics.broker_id() {
            return Err(BctowError::TopicBrokerMismatch(topic));
        }
        let entry = self
            .inner
            .topics
            .get(topic)
            .ok_or_else(|| BctowError::TopicNotFound(topic.to_string()))?;
        self.subscribe_entry(entry)
    }

    pub fn unsubscribe(&mut self, topic: TopicId) {
        self.subscriptions.remove(&topic);
        self.topic_order.retain(|&candidate| candidate != topic);
        if self.next_poll_idx >= self.topic_order.len() {
            self.next_poll_idx = 0;
        }
    }

    pub fn unsubscribe_by_name<K: TopicKey>(&mut self, topic: K) {
        debug_assert_eq!(topic.match_kind(), MatchKind::Exact);
        if let Some(entry) = self.inner.topics.resolve(topic.topic_str()) {
            self.unsubscribe(entry.id());
        }
    }

    /// Poll into the client's internal scratch buffer.
    pub fn poll(&mut self) -> Option<ReceivedMessage<'_>> {
        let count = self.topic_order.len();
        if count == 0 {
            return None;
        }

        let start = self.next_poll_idx % count;
        let overflow_policy = self.overflow_policy;

        for offset in 0..count {
            let idx = (start + offset) % count;
            let topic_id = self.topic_order[idx];

            let len = {
                let (subscriptions, scratch) = (&mut self.subscriptions, &mut self.scratch);
                let subscription = subscriptions
                    .get_mut(&topic_id)
                    .expect("topic_order and subscriptions out of sync");
                try_read_with_policy(
                    &mut subscription.cursor,
                    &subscription.ring,
                    overflow_policy,
                    scratch.as_mut_slice(),
                )
                .expect("client scratch buffer is sized from broker max_payload_size")
            };

            if let Some(len) = len {
                self.next_poll_idx = (idx + 1) % count;
                #[cfg(feature = "metrics")]
                self.inner.inc_delivered();
                return Some(ReceivedMessage {
                    topic_id,
                    payload: &self.scratch[..len],
                });
            }
        }

        None
    }

    /// Poll into a caller-provided buffer.
    pub fn poll_into(&mut self, dst: &mut [u8]) -> Result<Option<(TopicId, usize)>, BctowError> {
        let count = self.topic_order.len();
        if count == 0 {
            return Ok(None);
        }

        let start = self.next_poll_idx % count;
        let overflow_policy = self.overflow_policy;

        for offset in 0..count {
            let idx = (start + offset) % count;
            let topic_id = self.topic_order[idx];

            let len = {
                let subscription = self
                    .subscriptions
                    .get_mut(&topic_id)
                    .expect("topic_order and subscriptions out of sync");
                try_read_with_policy(
                    &mut subscription.cursor,
                    &subscription.ring,
                    overflow_policy,
                    dst,
                )?
            };

            if let Some(len) = len {
                self.next_poll_idx = (idx + 1) % count;
                #[cfg(feature = "metrics")]
                self.inner.inc_delivered();
                return Ok(Some((topic_id, len)));
            }
        }

        Ok(None)
    }

    fn subscribe_entry(&mut self, entry: Arc<crate::topic::TopicEntry>) -> Result<(), BctowError> {
        match self.subscriptions.entry(entry.id()) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(vacant) => {
                vacant.insert(Subscription {
                    cursor: Cursor::new_latest(entry.ring()),
                    ring: Arc::clone(entry.ring()),
                });
                self.topic_order.push(entry.id());
                Ok(())
            }
        }
    }
}

fn try_read_with_policy(
    cursor: &mut Cursor,
    ring: &RingBuffer,
    policy: OverflowPolicy,
    dst: &mut [u8],
) -> Result<Option<usize>, BctowError> {
    let seq = cursor.next_seq();

    if let Some(len) = ring.read_into(seq, dst)? {
        cursor.advance();
        return Ok(Some(len));
    }

    let latest = ring.latest_seq();
    if seq > latest {
        return Ok(None);
    }

    let oldest = ring.oldest_available_seq();
    if seq >= oldest {
        return Ok(None);
    }

    match policy {
        OverflowPolicy::DropOldest => {
            cursor.skip_to(oldest);
            if let Some(len) = ring.read_into(oldest, dst)? {
                cursor.advance();
                return Ok(Some(len));
            }
            Ok(None)
        }
        OverflowPolicy::SkipToLatest => {
            cursor.skip_to(latest + 1);
            Ok(None)
        }
        OverflowPolicy::DropNewest => Ok(None),
    }
}

/// Snapshot of broker metrics. Only available with the `metrics` feature.
#[cfg(feature = "metrics")]
pub struct Metrics {
    pub messages_published: u64,
    pub messages_delivered: u64,
}

#[cfg(test)]
mod tests {
    #[test]
    fn poll_into_uses_caller_buffer() {
        let (producer, ctx) = crate::broker(crate::BctowConfig::default());
        producer.publish("topic", b"prime").unwrap();

        let mut client = ctx.create_client();
        client.subscribe("topic").unwrap();
        producer.publish("topic", b"hello").unwrap();

        let mut dst = [0u8; 32];
        let (_topic, len) = client.poll_into(&mut dst).unwrap().unwrap();
        assert_eq!(&dst[..len], b"hello");
    }

    #[test]
    fn poll_borrows_from_internal_scratch() {
        let (producer, ctx) = crate::broker(crate::BctowConfig::default());
        producer.publish("topic", b"prime").unwrap();

        let mut client = ctx.create_client();
        client.subscribe("topic").unwrap();
        producer.publish("topic", b"hello").unwrap();

        let msg = client.poll().unwrap();
        assert_eq!(msg.payload, b"hello");
    }
}

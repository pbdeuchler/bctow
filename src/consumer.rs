use std::cell::Cell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::sync::Arc;

use crate::broker::BrokerInner;
use crate::config::OverflowPolicy;
use crate::error::BctowError;
use crate::ring::{Cursor, RingBuffer};
use crate::topic::{MatchKind, TopicId, TopicKey};

/// Borrowed message view backed by the client's internal scratch buffer.
#[derive(Debug, Clone, Copy)]
pub struct ReceivedMessage<'a, T> {
    pub topic_id: TopicId,
    pub message: &'a T,
}

/// Factory for consumer clients. Clone-able and shareable across threads.
#[derive(Clone)]
pub struct ConsumerContext<T> {
    pub(crate) inner: Arc<BrokerInner<T>>,
    overflow_policy: OverflowPolicy,
}

impl<T: Copy + Send + Sync + 'static> ConsumerContext<T> {
    pub(crate) fn new(inner: Arc<BrokerInner<T>>, overflow_policy: OverflowPolicy) -> Self {
        Self {
            inner,
            overflow_policy,
        }
    }

    pub fn create_client(&self) -> ConsumerClient<T> {
        ConsumerClient {
            inner: Arc::clone(&self.inner),
            overflow_policy: self.overflow_policy,
            subscriptions: HashMap::new(),
            topic_order: Vec::new(),
            next_poll_idx: 0,
            scratch: MaybeUninit::uninit(),
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

struct Subscription<T> {
    cursor: Cursor,
    ring: Arc<RingBuffer<T>>,
}

/// A single consumer with per-topic cursors. `ConsumerClient` is `Send` but not `Sync`.
pub struct ConsumerClient<T> {
    inner: Arc<BrokerInner<T>>,
    overflow_policy: OverflowPolicy,
    subscriptions: HashMap<TopicId, Subscription<T>>,
    topic_order: Vec<TopicId>,
    next_poll_idx: usize,
    scratch: MaybeUninit<T>,
    _not_sync: PhantomData<Cell<()>>,
}

impl<T: Copy + Send + Sync + 'static> ConsumerClient<T> {
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
    pub fn poll(&mut self) -> Option<ReceivedMessage<'_, T>> {
        let count = self.topic_order.len();
        if count == 0 {
            return None;
        }

        let start = self.next_poll_idx % count;
        let overflow_policy = self.overflow_policy;

        for offset in 0..count {
            let idx = (start + offset) % count;
            let topic_id = self.topic_order[idx];

            let found = {
                let (subscriptions, scratch) = (&mut self.subscriptions, &mut self.scratch);
                let subscription = subscriptions
                    .get_mut(&topic_id)
                    .expect("topic_order and subscriptions out of sync");
                try_read_with_policy(
                    &mut subscription.cursor,
                    &subscription.ring,
                    overflow_policy,
                    scratch,
                )
            };

            if found.is_some() {
                self.next_poll_idx = (idx + 1) % count;
                #[cfg(feature = "metrics")]
                self.inner.inc_delivered();
                return Some(ReceivedMessage {
                    topic_id,
                    message: unsafe { self.scratch.assume_init_ref() },
                });
            }
        }

        None
    }

    /// Poll into a caller-provided buffer.
    pub fn poll_into(&mut self, dst: &mut MaybeUninit<T>) -> Option<TopicId> {
        let count = self.topic_order.len();
        if count == 0 {
            return None;
        }

        let start = self.next_poll_idx % count;
        let overflow_policy = self.overflow_policy;

        for offset in 0..count {
            let idx = (start + offset) % count;
            let topic_id = self.topic_order[idx];

            let found = {
                let subscription = self
                    .subscriptions
                    .get_mut(&topic_id)
                    .expect("topic_order and subscriptions out of sync");
                try_read_with_policy(
                    &mut subscription.cursor,
                    &subscription.ring,
                    overflow_policy,
                    dst,
                )
            };

            if found.is_some() {
                self.next_poll_idx = (idx + 1) % count;
                #[cfg(feature = "metrics")]
                self.inner.inc_delivered();
                return Some(topic_id);
            }
        }

        None
    }

    fn subscribe_entry(&mut self, entry: Arc<crate::topic::TopicEntry<T>>) -> Result<(), BctowError> {
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

fn try_read_with_policy<T: Copy>(
    cursor: &mut Cursor,
    ring: &RingBuffer<T>,
    policy: OverflowPolicy,
    dst: &mut MaybeUninit<T>,
) -> Option<()> {
    let seq = cursor.next_seq();

    if ring.read_into(seq, dst).is_some() {
        cursor.advance();
        return Some(());
    }

    let latest = ring.latest_seq();
    if seq > latest {
        return None;
    }

    let oldest = ring.oldest_available_seq();
    if seq >= oldest {
        return None;
    }

    match policy {
        OverflowPolicy::DropOldest => {
            cursor.skip_to(oldest);
            if ring.read_into(oldest, dst).is_some() {
                cursor.advance();
                return Some(());
            }
            None
        }
        OverflowPolicy::SkipToLatest => {
            cursor.skip_to(latest + 1);
            None
        }
        OverflowPolicy::DropNewest => None,
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
    use std::mem::MaybeUninit;

    #[test]
    fn poll_into_uses_caller_buffer() {
        let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig::default());
        producer.publish("topic", &42u64).unwrap();

        let mut client = ctx.create_client();
        client.subscribe("topic").unwrap();
        producer.publish("topic", &99u64).unwrap();

        let mut dst = MaybeUninit::<u64>::uninit();
        let _topic = client.poll_into(&mut dst).unwrap();
        assert_eq!(unsafe { dst.assume_init() }, 99u64);
    }

    #[test]
    fn poll_borrows_from_internal_scratch() {
        let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig::default());
        producer.publish("topic", &42u64).unwrap();

        let mut client = ctx.create_client();
        client.subscribe("topic").unwrap();
        producer.publish("topic", &99u64).unwrap();

        let msg = client.poll().unwrap();
        assert_eq!(*msg.message, 99u64);
    }
}

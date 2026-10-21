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
            subscriptions: Vec::new(),
            subscription_indices: HashMap::new(),
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
    topic_id: TopicId,
    cursor: Cursor,
    ring: Arc<RingBuffer<T>>,
}

/// A single consumer with per-topic cursors. `ConsumerClient` is `Send` but not `Sync`.
pub struct ConsumerClient<T> {
    inner: Arc<BrokerInner<T>>,
    overflow_policy: OverflowPolicy,
    subscriptions: Vec<Subscription<T>>,
    subscription_indices: HashMap<TopicId, usize>,
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
        let Some(index) = self.subscription_indices.remove(&topic) else {
            return;
        };
        self.subscriptions.remove(index);
        for (index, subscription) in self.subscriptions.iter().enumerate().skip(index) {
            *self
                .subscription_indices
                .get_mut(&subscription.topic_id)
                .unwrap() = index;
        }
        if self.next_poll_idx >= self.subscriptions.len() {
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
        let topic_id = scan_subscriptions(
            &mut self.subscriptions,
            &mut self.next_poll_idx,
            self.overflow_policy,
            &mut self.scratch,
        )?;
        #[cfg(feature = "metrics")]
        self.inner.inc_delivered();
        Some(ReceivedMessage {
            topic_id,
            // SAFETY: a successful scan initializes scratch with a copied T.
            message: unsafe { self.scratch.assume_init_ref() },
        })
    }

    /// Poll into a caller-provided buffer.
    pub fn poll_into(&mut self, dst: &mut MaybeUninit<T>) -> Option<TopicId> {
        let topic_id = scan_subscriptions(
            &mut self.subscriptions,
            &mut self.next_poll_idx,
            self.overflow_policy,
            dst,
        )?;
        #[cfg(feature = "metrics")]
        self.inner.inc_delivered();
        Some(topic_id)
    }

    fn subscribe_entry(
        &mut self,
        entry: Arc<crate::topic::TopicEntry<T>>,
    ) -> Result<(), BctowError> {
        match self.subscription_indices.entry(entry.id()) {
            Entry::Occupied(_) => Ok(()),
            Entry::Vacant(vacant) => {
                vacant.insert(self.subscriptions.len());
                self.subscriptions.push(Subscription {
                    topic_id: entry.id(),
                    cursor: Cursor::new_latest(entry.ring()),
                    ring: Arc::clone(entry.ring()),
                });
                Ok(())
            }
        }
    }
}

fn scan_subscriptions<T: Copy>(
    subscriptions: &mut [Subscription<T>],
    next_poll_idx: &mut usize,
    policy: OverflowPolicy,
    dst: &mut MaybeUninit<T>,
) -> Option<TopicId> {
    let count = subscriptions.len();
    let mut index = *next_poll_idx;
    for _ in 0..count {
        let subscription = &mut subscriptions[index];
        let found = try_read_with_policy(&mut subscription.cursor, &subscription.ring, policy, dst);
        index += 1;
        if index == count {
            index = 0;
        }
        if found.is_some() {
            *next_poll_idx = index;
            return Some(subscription.topic_id);
        }
    }
    None
}

fn try_read_with_policy<T: Copy>(
    cursor: &mut Cursor,
    ring: &RingBuffer<T>,
    policy: OverflowPolicy,
    dst: &mut MaybeUninit<T>,
) -> Option<()> {
    let seq = cursor.next_seq();

    // Idle subscriptions need only the ring header, not its slot allocation.
    if seq > ring.latest_seq() {
        return None;
    }

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

    use super::*;

    fn read(client: &mut ConsumerClient<u64>, into: bool) -> Option<(TopicId, u64)> {
        if into {
            let mut dst = MaybeUninit::uninit();
            client
                .poll_into(&mut dst)
                .map(|id| (id, unsafe { dst.assume_init() }))
        } else {
            client.poll().map(|msg| (msg.topic_id, *msg.message))
        }
    }

    #[test]
    fn removal_compacts_indices_preserves_order_and_duplicate_subscriptions() {
        for into in [false, true] {
            for removed in 0..4 {
                let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig::default());
                let ids: Vec<_> = ["a", "b", "c", "unknown"]
                    .into_iter()
                    .map(|name| producer.register_topic(name))
                    .collect();
                let mut client = ctx.create_client();
                for &id in &ids[..3] {
                    client.subscribe_id(id).unwrap();
                    producer.publish_id(id, &1).unwrap();
                }
                // Leave the next polling index at the last position. Removing
                // any real subscription must wrap it to zero, as before.
                assert_eq!(read(&mut client, into), Some((ids[0], 1)));
                assert_eq!(read(&mut client, into), Some((ids[1], 1)));
                client.unsubscribe(ids[removed]);
                let remaining: Vec<_> = ids[..3]
                    .iter()
                    .copied()
                    .filter(|&id| id != ids[removed])
                    .collect();
                assert_eq!(client.subscriptions.len(), remaining.len());
                for (index, &id) in remaining.iter().enumerate() {
                    assert_eq!(client.subscription_indices[&id], index);
                    assert_eq!(client.subscriptions[index].topic_id, id);
                    client.subscribe_id(id).unwrap();
                    client.subscribe_id(id).unwrap();
                }
                assert_eq!(client.subscriptions.len(), remaining.len());
                // Drain the one outstanding old message if c was retained.
                while read(&mut client, into).is_some() {}
                let start = client.next_poll_idx;
                for &id in &remaining {
                    producer.publish_id(id, &2).unwrap();
                }
                for offset in 0..remaining.len() {
                    assert_eq!(
                        read(&mut client, into),
                        Some((remaining[(start + offset) % remaining.len()], 2))
                    );
                }
                assert_eq!(read(&mut client, into), None);
            }
        }
    }

    #[test]
    fn resubscribing_starts_with_a_fresh_cursor() {
        for into in [false, true] {
            let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig::default());
            let id = producer.register_topic("a");
            let mut client = ctx.create_client();
            client.subscribe_id(id).unwrap();
            producer.publish_id(id, &1).unwrap();
            client.unsubscribe(id);
            producer.publish_id(id, &2).unwrap();
            client.subscribe_id(id).unwrap();
            assert_eq!(read(&mut client, into), None);
            producer.publish_id(id, &3).unwrap();
            assert_eq!(read(&mut client, into), Some((id, 3)));
            client.unsubscribe(id);
            assert!(client.subscriptions.is_empty());
            assert!(client.subscription_indices.is_empty());
            assert_eq!(client.next_poll_idx, 0);
            assert_eq!(read(&mut client, into), None);
        }
    }

    #[test]
    fn both_poll_methods_round_robin_past_inactive_subscriptions() {
        for into in [false, true] {
            let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig::default());
            let ids: Vec<_> = ["a", "inactive", "b", "c"]
                .into_iter()
                .map(|name| producer.register_topic(name))
                .collect();
            let mut client = ctx.create_client();
            for &id in &ids {
                client.subscribe_id(id).unwrap();
                if id != ids[1] {
                    producer.publish_id(id, &1).unwrap();
                    producer.publish_id(id, &2).unwrap();
                }
            }
            for value in [1, 2] {
                for &id in &[ids[0], ids[2], ids[3]] {
                    assert_eq!(read(&mut client, into), Some((id, value)));
                }
            }
            assert_eq!(read(&mut client, into), None);
            #[cfg(feature = "metrics")]
            assert_eq!(ctx.metrics().messages_delivered, 6);
        }
    }

    #[test]
    fn unsuccessful_scans_preserve_destination_and_metrics() {
        let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig::default());
        let id = producer.register_topic("inactive");
        let mut client = ctx.create_client();
        let mut dst = MaybeUninit::new(99);
        for subscribed in [false, true] {
            if subscribed {
                client.subscribe_id(id).unwrap();
            }
            assert!(client.poll_into(&mut dst).is_none());
            assert_eq!(unsafe { dst.assume_init() }, 99);
            assert!(client.poll().is_none());
        }
        #[cfg(feature = "metrics")]
        assert_eq!(ctx.metrics().messages_delivered, 0);
    }

    #[test]
    fn idle_polls_do_not_hide_later_publications() {
        for into in [false, true] {
            for policy in [
                OverflowPolicy::DropOldest,
                OverflowPolicy::DropNewest,
                OverflowPolicy::SkipToLatest,
            ] {
                let (producer, ctx) = crate::broker::<u64>(crate::BctowConfig {
                    ring_buffer_capacity: 3,
                    overflow_policy: policy,
                });
                let id = producer.register_topic("intermittent");
                let mut client = ctx.create_client();
                client.subscribe_id(id).unwrap();
                for value in 1..=10 {
                    assert_eq!(read(&mut client, into), None);
                    producer.publish_id(id, &value).unwrap();
                    assert_eq!(read(&mut client, into), Some((id, value)));
                    assert_eq!(read(&mut client, into), None);
                }
            }
        }
    }

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

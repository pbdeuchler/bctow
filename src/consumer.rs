use std::collections::HashMap;
use std::sync::Arc;

use crate::broker::BrokerInner;
use crate::config::OverflowPolicy;
use crate::error::BctowError;
use crate::ring::{Cursor, RingBuffer};
use crate::topic::TopicId;

/// Factory and manager for consumer clients. Clone-able and shareable across threads.
#[derive(Clone)]
pub struct ConsumerContext<T: Send + Sync + 'static> {
    pub(crate) inner: Arc<BrokerInner<T>>,
    overflow_policy: OverflowPolicy,
}

impl<T: Send + Sync + 'static> ConsumerContext<T> {
    pub(crate) fn new(inner: Arc<BrokerInner<T>>, overflow_policy: OverflowPolicy) -> Self {
        ConsumerContext {
            inner,
            overflow_policy,
        }
    }

    /// Create a new consumer client with no subscriptions.
    pub fn create_client(&self) -> ConsumerClient<T> {
        ConsumerClient {
            inner: Arc::clone(&self.inner),
            overflow_policy: self.overflow_policy,
            subscriptions: HashMap::new(),
            topic_order: Vec::new(),
            next_poll_idx: 0,
        }
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

/// A single consumer with per-topic cursors.
/// NOT Clone, NOT Sync — owned by a single consumer thread.
pub struct ConsumerClient<T: Send + Sync + 'static> {
    inner: Arc<BrokerInner<T>>,
    overflow_policy: OverflowPolicy,
    /// Per-topic cursor and cached ring buffer handle.
    subscriptions: HashMap<TopicId, (Cursor, Arc<RingBuffer<T>>)>,
    /// Stable ordered list of subscribed topics for round-robin fairness.
    topic_order: Vec<TopicId>,
    /// Index into topic_order for the next poll round-robin start.
    next_poll_idx: usize,
}

impl<T: Send + Sync + 'static> ConsumerClient<T> {
    /// Subscribe to `topic` by name. The topic must already exist (have been published to).
    /// New subscribers start at latest+1 — they do not see historical messages.
    pub fn subscribe(&mut self, topic: &str) -> Result<(), BctowError> {
        match self.inner.interner.resolve(topic) {
            Some(id) => self.subscribe_id(id),
            None => Err(BctowError::TopicNotFound(topic.to_owned())),
        }
    }

    /// Subscribe to a known `TopicId`.
    ///
    /// Subscribing to the same topic twice is a no-op.
    pub fn subscribe_id(&mut self, topic: TopicId) -> Result<(), BctowError> {
        let ring = self
            .inner
            .get_ring(topic)
            .ok_or_else(|| BctowError::TopicNotFound(format!("{topic:?}")))?;

        if !self.subscriptions.contains_key(&topic) {
            let cursor = Cursor::new_latest(&ring);
            self.subscriptions.insert(topic, (cursor, ring));
            self.topic_order.push(topic);
        }
        Ok(())
    }

    /// Remove subscription for `topic`.
    pub fn unsubscribe(&mut self, topic: TopicId) {
        self.subscriptions.remove(&topic);
        self.topic_order.retain(|&t| t != topic);
        // Keep next_poll_idx in bounds.
        let len = self.topic_order.len();
        if len > 0 && self.next_poll_idx >= len {
            self.next_poll_idx = 0;
        }
    }

    /// Remove subscription for the topic identified by `topic` name.
    ///
    /// If the topic name has never been interned (i.e. no message has ever been
    /// published to it), this is a no-op.
    pub fn unsubscribe_by_name(&mut self, topic: &str) {
        if let Some(id) = self.inner.interner.resolve(topic) {
            self.unsubscribe(id);
        }
    }

    /// Poll for the next available message across all subscribed topics.
    ///
    /// Returns `(TopicId, Arc<T>)` for the first available message, using a rotating
    /// start index for fairness. Returns `None` if no topic has a new message.
    pub fn poll(&mut self) -> Option<(TopicId, Arc<T>)> {
        let count = self.topic_order.len();
        if count == 0 {
            return None;
        }

        let start = self.next_poll_idx % count;

        for i in 0..count {
            let idx = (start + i) % count;
            let topic_id = self.topic_order[idx];

            // We need mutable access to the cursor but also need the ring ref.
            // Borrow from the hashmap directly.
            let result = {
                let (cursor, ring) = self.subscriptions.get_mut(&topic_id)
                    .expect("topic_order and subscriptions out of sync");
                try_read_with_policy(cursor, ring, self.overflow_policy)
            };

            if let Some(arc) = result {
                // Only advance the round-robin index when a message is actually found.
                // This prevents unfair starvation where idx advances even on a miss.
                self.next_poll_idx = (idx + 1) % count;
                #[cfg(feature = "metrics")]
                self.inner.inc_delivered();
                return Some((topic_id, arc));
            }
        }
        None
    }
}

/// Attempt to read the next message for `cursor` from `ring`, applying `policy`
/// if the consumer has lagged. Returns the message Arc or None.
fn try_read_with_policy<T: Send + Sync + 'static>(
    cursor: &mut Cursor,
    ring: &RingBuffer<T>,
    policy: OverflowPolicy,
) -> Option<Arc<T>> {
    let seq = cursor.next_seq();

    // Try the current cursor position first.
    if let Some(arc) = ring.read(seq) {
        cursor.advance();
        return Some(arc);
    }

    // Read returned None. Determine why.
    let latest = ring.latest_seq();

    if seq > latest {
        // Consumer is ahead of the producer — nothing new to read.
        // Also covers the case where latest == 0 (nothing written yet):
        // seq starts at 1 > 0, so this branch fires correctly.
        return None;
    }

    // Consumer has lagged: seq < oldest_available.
    let oldest = ring.oldest_available_seq();
    match policy {
        OverflowPolicy::DropOldest => {
            // Skip to oldest available and try once more.
            cursor.skip_to(oldest);
            if let Some(arc) = ring.read(oldest) {
                cursor.advance();
                return Some(arc);
            }
            None
        }
        OverflowPolicy::SkipToLatest => {
            // Jump past everything; consumer will read only new messages.
            cursor.skip_to(latest + 1);
            None
        }
        OverflowPolicy::DropNewest => {
            // Do nothing — consumer stays put, effectively dropping messages
            // that it can no longer reach.
            None
        }
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
    use super::*;
    use crate::ring::RingBuffer;

    fn make_ring(capacity: usize) -> Arc<RingBuffer<u64>> {
        Arc::new(RingBuffer::new(capacity))
    }

    fn push(ring: &RingBuffer<u64>, v: u64) -> u64 {
        unsafe { ring.push(v) }
    }

    // --- try_read_with_policy ---

    #[test]
    fn read_policy_empty_ring_returns_none() {
        let ring = make_ring(4);
        let mut cursor = Cursor::new();
        let result = try_read_with_policy(&mut cursor, &ring, OverflowPolicy::DropOldest);
        assert!(result.is_none());
        assert_eq!(cursor.next_seq(), 1); // cursor unchanged
    }

    #[test]
    fn read_policy_happy_path_returns_message_and_advances_cursor() {
        let ring = make_ring(4);
        push(&ring, 42);
        let mut cursor = Cursor::new();
        let result = try_read_with_policy(&mut cursor, &ring, OverflowPolicy::DropOldest);
        assert_eq!(result.map(|a| *a), Some(42));
        assert_eq!(cursor.next_seq(), 2);
    }

    #[test]
    fn read_policy_ahead_of_producer_returns_none() {
        let ring = make_ring(4);
        push(&ring, 1);
        let mut cursor = Cursor::new_latest(&ring);
        // cursor is at seq 2, latest is 1 — nothing new
        let result = try_read_with_policy(&mut cursor, &ring, OverflowPolicy::DropOldest);
        assert!(result.is_none());
        assert_eq!(cursor.next_seq(), 2); // unchanged
    }

    #[test]
    fn read_policy_drop_oldest_skips_to_oldest_and_reads() {
        let ring = make_ring(4);
        for i in 1u64..=6 { push(&ring, i); } // seqs 1-6; oldest=3
        let mut cursor = Cursor::new(); // stuck at seq=1 which is overwritten
        let result = try_read_with_policy(&mut cursor, &ring, OverflowPolicy::DropOldest);
        assert!(result.is_some());
        assert!(*result.unwrap() >= 3);
        assert_eq!(cursor.next_seq(), ring.oldest_available_seq() + 1);
    }

    #[test]
    fn read_policy_skip_to_latest_advances_past_everything() {
        let ring = make_ring(4);
        for i in 1u64..=6 { push(&ring, i); } // oldest=3, latest=6
        let mut cursor = Cursor::new(); // stuck at seq=1
        let result = try_read_with_policy(&mut cursor, &ring, OverflowPolicy::SkipToLatest);
        assert!(result.is_none()); // nothing returned; consumer skipped ahead
        assert_eq!(cursor.next_seq(), 7); // latest+1
    }

    #[test]
    fn read_policy_drop_newest_does_nothing() {
        let ring = make_ring(4);
        for i in 1u64..=6 { push(&ring, i); } // oldest=3
        let mut cursor = Cursor::new(); // stuck at seq=1
        let result = try_read_with_policy(&mut cursor, &ring, OverflowPolicy::DropNewest);
        assert!(result.is_none());
        assert_eq!(cursor.next_seq(), 1); // cursor unchanged
    }
}

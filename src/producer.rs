use std::sync::Arc;

use crate::broker::BrokerInner;
use crate::error::BctowError;
use crate::topic::TopicId;

/// Single-producer handle. There must be exactly one Producer per broker.
/// Send but not Sync — only one thread should call push at a time.
pub struct Producer<T: Send + Sync + 'static> {
    pub(crate) inner: Arc<BrokerInner<T>>,
}

// Producer is Send (can be moved to another thread) but not Sync (must not be
// shared between threads simultaneously, to uphold the single-producer invariant).
// The compiler derives these correctly since Arc<BrokerInner<T>> is Send+Sync but
// we never expose a way for two threads to call push concurrently through Producer.

impl<T: Send + Sync + 'static> Producer<T> {
    /// Publish `msg` to `topic`. Creates the topic and its ring buffer on first use.
    ///
    /// Returns the sequence number assigned to the message.
    pub fn publish(&self, topic: &str, msg: T) -> Result<u64, BctowError> {
        let id = self.inner.interner.intern(topic);
        self.publish_id(id, msg)
    }

    /// Publish `msg` directly to a known `TopicId`. Hot path — no string work.
    ///
    /// Returns the sequence number assigned to the message.
    pub fn publish_id(&self, topic: TopicId, msg: T) -> Result<u64, BctowError> {
        let ring = self.inner.get_or_create_ring(topic);
        // SAFETY: Producer is the sole caller of push. The single-producer
        // invariant is upheld by the type system: there is exactly one Producer
        // per broker and it is !Sync, so push can only be called from one thread.
        let seq = unsafe { ring.push(msg) };
        #[cfg(feature = "metrics")]
        self.inner.inc_published();
        Ok(seq)
    }
}

use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::broker::BrokerInner;
use crate::topic::TopicId;

/// Single-producer handle. There must be exactly one Producer per broker.
/// `Send` but not `Sync` — only one thread may call `publish` at a time.
pub struct Producer<T: Send + Sync + 'static> {
    pub(crate) inner: Arc<BrokerInner<T>>,
    /// Prevents `Producer` from being `Sync`. This enforces the single-producer
    /// invariant: only one thread can call `publish` at a time. Without this,
    /// the compiler would auto-derive `Sync` from `Arc<BrokerInner<T>>`, allowing
    /// callers to wrap `Producer` in an `Arc` and call `publish` concurrently.
    pub(crate) _not_sync: PhantomData<Cell<()>>,
}

impl<T: Send + Sync + 'static> Producer<T> {
    /// Publish `msg` to `topic`. Creates the topic and its ring buffer on first use.
    ///
    /// Returns the sequence number assigned to the message.
    #[inline]
    pub fn publish(&self, topic: &str, msg: T) -> u64 {
        let id = self.inner.interner.intern(topic);
        self.publish_id(id, msg)
    }

    /// Resolve a `TopicId` back to its original topic string.
    ///
    /// Returns `None` if the id has never been interned. Intended for debug
    /// and logging paths — do not call on hot paths.
    pub fn topic_name(&self, id: TopicId) -> Option<String> {
        self.inner.interner.name(id)
    }

    /// Intern `topic` and return its `TopicId` without publishing a message.
    ///
    /// Use this to obtain a `TopicId` for repeated `publish_id` calls, avoiding
    /// the string interning overhead on every publish.  The topic will not be
    /// visible to consumers (i.e. `subscribe` will fail) until at least one
    /// message has been published to it.
    pub fn intern_topic(&self, topic: &str) -> TopicId {
        self.inner.interner.intern(topic)
    }

    /// Publish `msg` directly to a known `TopicId`. Hot path — no string work.
    ///
    /// Returns the sequence number assigned to the message.
    ///
    /// **Note:** If `topic` has not been interned in this broker, a new ring
    /// buffer is created. Callers should use [`Producer::intern_topic`] or
    /// [`Producer::publish`] to obtain valid `TopicId`s.
    #[inline]
    pub fn publish_id(&self, topic: TopicId, msg: T) -> u64 {
        let ring = self.inner.get_or_create_ring(topic);
        // SAFETY: Producer is the sole caller of push. The single-producer
        // invariant is upheld by the type system: there is exactly one Producer
        // per broker and it is !Sync (PhantomData<Cell<()>>), so push can only
        // be called from one thread at a time.
        let seq = unsafe { ring.push(msg) };
        #[cfg(feature = "metrics")]
        self.inner.inc_published();
        seq
    }
}

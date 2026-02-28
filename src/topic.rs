use std::collections::HashMap;
use std::sync::RwLock;

/// Compact identifier for a topic. Used on hot paths — no string work involved.
#[derive(Copy, Clone, Hash, Eq, PartialEq, Debug)]
pub struct TopicId(pub(crate) u32);

/// Describes how a topic key should be matched against subscriptions.
/// Only `Exact` is implemented; the enum exists so future variants
/// (`Prefix`, `Wildcard`, …) can be added without breaking callers.
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
        ExactTopic(s.into())
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

/// Placeholder for future pattern-based matching (prefix, wildcard, …).
/// Not constructible outside this module.
#[derive(Debug)]
pub struct TopicPattern {
    _private: (),
}

/// Thread-safe bidirectional mapping between topic strings and `TopicId`s.
///
/// Write lock is only acquired when a truly new topic is first interned;
/// all subsequent lookups use a read lock.
pub struct TopicInterner {
    inner: RwLock<InternerInner>,
}

struct InternerInner {
    map: HashMap<String, TopicId>,
    names: Vec<String>,
}

impl TopicInterner {
    pub fn new() -> Self {
        TopicInterner {
            inner: RwLock::new(InternerInner {
                map: HashMap::new(),
                names: Vec::new(),
            }),
        }
    }

    /// Return the `TopicId` for `key`, creating one if this is the first time
    /// the string has been seen. Allocates only on the first call for each
    /// unique string.
    pub fn intern(&self, key: &str) -> TopicId {
        // Fast path: read lock only.
        {
            let guard = self.inner.read().expect("topic interner read lock poisoned");
            if let Some(&id) = guard.map.get(key) {
                return id;
            }
        }

        // Slow path: write lock to insert.
        let mut guard = self
            .inner
            .write()
            .expect("topic interner write lock poisoned");

        // Re-check after acquiring write lock (another thread may have raced us).
        if let Some(&id) = guard.map.get(key) {
            return id;
        }

        let len = guard.names.len();
        assert!(len <= u32::MAX as usize, "topic interner overflow: exceeded u32::MAX topics");
        let id = TopicId(len as u32);
        let owned = key.to_owned();
        guard.map.insert(owned.clone(), id);
        guard.names.push(owned);
        id
    }

    /// Look up `key` without allocating. Returns `None` if the topic has
    /// never been interned.
    pub fn resolve(&self, key: &str) -> Option<TopicId> {
        let guard = self.inner.read().expect("topic interner read lock poisoned");
        guard.map.get(key).copied()
    }

    /// Reverse lookup: return the topic string for a given `TopicId`.
    ///
    /// Allocates a new `String` on each call. Intended for debug and logging
    /// paths only — do not call on hot paths.
    pub fn name(&self, id: TopicId) -> Option<String> {
        let guard = self.inner.read().expect("topic interner read lock poisoned");
        guard.names.get(id.0 as usize).cloned()
    }
}

impl Default for TopicInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- TopicId ---

    #[test]
    fn topic_id_copy_and_eq() {
        let a = TopicId(0);
        let b = a; // Copy
        assert_eq!(a, b);
    }

    #[test]
    fn topic_id_hash_in_map() {
        let mut m = HashMap::new();
        m.insert(TopicId(1), "one");
        assert_eq!(m[&TopicId(1)], "one");
    }

    // --- ExactTopic / TopicKey ---

    #[test]
    fn exact_topic_str_and_kind() {
        let t = ExactTopic::new("sensor/temp");
        assert_eq!(t.topic_str(), "sensor/temp");
        assert_eq!(t.match_kind(), MatchKind::Exact);
    }

    #[test]
    fn str_impl_of_topic_key() {
        let s: &str = "sensor/temp";
        assert_eq!(s.topic_str(), "sensor/temp");
        assert_eq!(s.match_kind(), MatchKind::Exact);
    }

    // --- TopicInterner ---

    fn interner() -> TopicInterner {
        TopicInterner::new()
    }

    #[test]
    fn default_interner_works() {
        let ti = TopicInterner::default();
        let id = ti.intern("x");
        assert_eq!(ti.resolve("x"), Some(id));
    }

    #[test]
    fn intern_returns_stable_id() {
        let ti = interner();
        let id1 = ti.intern("foo");
        let id2 = ti.intern("foo");
        assert_eq!(id1, id2);
    }

    #[test]
    fn intern_distinct_topics_get_distinct_ids() {
        let ti = interner();
        let a = ti.intern("a");
        let b = ti.intern("b");
        assert_ne!(a, b);
    }

    #[test]
    fn resolve_hit() {
        let ti = interner();
        let id = ti.intern("bar");
        assert_eq!(ti.resolve("bar"), Some(id));
    }

    #[test]
    fn resolve_miss_returns_none() {
        let ti = interner();
        assert_eq!(ti.resolve("nonexistent"), None);
    }

    #[test]
    fn name_roundtrip() {
        let ti = interner();
        let id = ti.intern("my/topic");
        assert_eq!(ti.name(id).as_deref(), Some("my/topic"));
    }

    #[test]
    fn name_unknown_id_returns_none() {
        let ti = interner();
        assert_eq!(ti.name(TopicId(999)), None);
    }

    #[test]
    fn intern_many_topics_are_sequential() {
        let ti = interner();
        for i in 0u32..16 {
            let s = format!("topic/{i}");
            let id = ti.intern(&s);
            assert_eq!(id.0, i);
        }
    }

    #[test]
    fn concurrent_intern_is_consistent() {
        use std::sync::Arc;
        use std::thread;

        let ti = Arc::new(interner());
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ti = Arc::clone(&ti);
                thread::spawn(move || ti.intern("shared"))
            })
            .collect();

        let ids: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        // All threads must get the same id.
        assert!(ids.windows(2).all(|w| w[0] == w[1]));
    }
}

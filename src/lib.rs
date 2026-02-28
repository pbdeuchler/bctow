//! # bctow — high-performance topic-based pub/sub
//!
//! bctow is a single-producer, multiple-consumer (SPMC) message broker designed
//! for in-process, low-latency workloads.  Typical target: millions of messages
//! per second with payloads up to ~16 KB, all within a single process.
//!
//! ## Quick start
//!
//! ```rust
//! use bctow::{BctowConfig, OverflowPolicy, broker};
//!
//! let (producer, ctx) = broker::<String>(BctowConfig::default());
//!
//! producer.publish("events/sensor", "temp=42".to_owned());
//!
//! let mut client = ctx.create_client();
//! // subscribe after at least one publish so the topic exists
//! client.subscribe("events/sensor").unwrap();
//!
//! producer.publish("events/sensor", "temp=43".to_owned());
//! if let Some((_topic, msg)) = client.poll() {
//!     assert_eq!(*msg, "temp=43");
//! }
//! ```
//!
//! ## Architecture
//!
//! ```text
//!  ┌──────────────────────────────────────────────────────┐
//!  │  BrokerInner (Arc-shared)                            │
//!  │  ┌──────────────┐   ┌──────────────────────────────┐ │
//!  │  │ TopicInterner│   │ rings: HashMap<TopicId, Ring>│ │
//!  │  │  (RwLock)    │   │  (RwLock, write-on-creation) │ │
//!  │  └──────────────┘   └──────────────────────────────┘ │
//!  └──────────────────────────────────────────────────────┘
//!          ▲                         ▲
//!   Producer (!Sync)         ConsumerContext (Clone)
//!   one thread only          ──────────────────────
//!                             ConsumerClient  ConsumerClient
//!                             per-topic cursor  per-topic cursor
//! ```
//!
//! There is a **single shared log per topic**: all consumers read the same ring
//! buffer and each maintains its own lightweight cursor (a single `u64` sequence
//! number).  Consumers are responsible for keeping up; slow consumers experience
//! one of three configurable overflow policies rather than back-pressuring the
//! producer.
//!
//! ## Data structures
//!
//! Each topic's ring buffer is a pre-allocated `Box<[Slot<T>]>` of fixed capacity.
//! Every slot holds `Option<Arc<T>>` and a `sequence: AtomicU64`.  Messages are
//! addressed by a monotonically increasing sequence number; the slot index is
//! `(seq - 1) % capacity`.
//!
//! Storing `Arc<T>` instead of `T` directly solves a key memory-safety problem:
//! a consumer that is reading slot *k* just as the producer wraps around and starts
//! overwriting it would be a data race on the value if `T` were stored inline.
//! With `Arc<T>`, the producer replaces the `Arc` in the slot (decrementing the old
//! refcount) while any reader that has already called `Arc::clone` retains a valid
//! reference for as long as they hold it.
//!
//! ## Memory layout
//!
//! Ring buffers are **pre-allocated** at topic creation time — no per-message heap
//! allocation occurs on the steady-state publish path.  Topic creation itself (the
//! first `publish` call for a new topic string) allocates a `RingBuffer`, interns
//! the topic string, and acquires a write lock on the rings map; this is the only
//! allocation-heavy, lock-acquiring step.  All subsequent publishes to known topics
//! take only the read path of that lock.
//!
//! ## Concurrency model
//!
//! | Role              | Guarantee                                           |
//! |-------------------|-----------------------------------------------------|
//! | Producer          | `Send` + `!Sync` — enforced by `PhantomData<Cell<()>>` |
//! | ConsumerContext   | `Clone` + `Send` + `Sync` — freely shareable        |
//! | ConsumerClient    | `!Clone` + `!Sync` — owned by one consumer thread   |
//! | Ring buffer reads | Lock-free; use `Release`/`Acquire` atomics           |
//! | Topic creation    | `RwLock` write; happens once per unique topic string |
//! | Topic lookup      | `RwLock` read only after first publish               |
//!
//! The `Producer` is `!Sync` so the compiler statically prevents wrapping it in an
//! `Arc` and calling `publish` from multiple threads simultaneously.  This upholds
//! the single-producer invariant without a runtime mutex on the hot path.
//!
//! ## Fairness
//!
//! `ConsumerClient::poll` rotates its start index across subscribed topics on each
//! successful read.  This guarantees that a topic producing messages at a high rate
//! cannot starve topics with lower throughput: every topic is polled in turn before
//! returning to a previously serviced one.
//!
//! ## Overflow policies
//!
//! When a consumer's cursor falls behind the oldest available sequence (i.e. the
//! producer has lapped the consumer), the configured [`OverflowPolicy`] applies:
//!
//! | Policy         | Behaviour                                                   |
//! |----------------|-------------------------------------------------------------|
//! | `DropOldest`   | Skip the consumer's cursor to the oldest available message  |
//! | `DropNewest`   | Leave the cursor in place; the consumer misses new messages |
//! | `SkipToLatest` | Jump the cursor past the latest message; skip all backlog   |
//!
//! `DropOldest` is the default and is appropriate for most streaming workloads
//! where seeing recent data is more important than seeing every message.

pub mod config;
pub mod error;
pub mod topic;
pub(crate) mod ring;
pub(crate) mod broker;
pub mod producer;
pub mod consumer;

use std::marker::PhantomData;
use std::sync::Arc;

use broker::BrokerInner;

pub use config::{BctowConfig, OverflowPolicy};
pub use consumer::{ConsumerClient, ConsumerContext};
pub use error::BctowError;
pub use producer::Producer;
pub use topic::{ExactTopic, TopicId, TopicKey};

#[cfg(feature = "metrics")]
pub use consumer::Metrics;

/// Create a matched `Producer` + `ConsumerContext` pair sharing the same broker state.
pub fn broker<T: Send + Sync + 'static>(config: BctowConfig) -> (Producer<T>, ConsumerContext<T>) {
    let overflow_policy = config.overflow_policy;
    let inner = Arc::new(BrokerInner::new(config));
    (
        Producer {
            inner: Arc::clone(&inner),
            _not_sync: PhantomData,
        },
        ConsumerContext::new(inner, overflow_policy),
    )
}

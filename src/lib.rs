//! # bctow — high-throughput topic-based pub/sub
//!
//! bctow is a single-producer, many-consumer broker for in-process streaming
//! systems. The broker stores byte payloads in preallocated per-topic rings and
//! copies them into consumer-owned buffers on read, so steady-state publish and
//! receive paths avoid heap allocation.
//!
//! ## Quick start
//!
//! ```rust
//! use bctow::{BctowConfig, broker};
//!
//! let (producer, ctx) = broker(BctowConfig::default());
//!
//! producer.publish("events/sensor", b"temp=42").unwrap();
//!
//! let mut client = ctx.create_client();
//! client.subscribe("events/sensor").unwrap();
//!
//! producer.publish("events/sensor", b"temp=43").unwrap();
//! let msg = client.poll().unwrap();
//! assert_eq!(msg.payload, b"temp=43");
//! ```
//!
//! ## Architecture
//!
//! - One producer thread appends to a shared ring per topic.
//! - Each consumer maintains its own cursor per subscribed topic.
//! - Topic ids are broker-branded so cross-broker misuse fails fast.
//! - Messages are copied into preallocated slot buffers on publish and into
//!   caller-owned or client-owned buffers on receive.
//! - `publish_id` uses a producer-local ring cache so steady-state publishing to
//!   known topics avoids broker registry locks.

pub mod config;
pub mod consumer;
pub mod error;
pub mod producer;
pub mod topic;

pub(crate) mod broker;
pub(crate) mod ring;

use std::marker::PhantomData;
use std::sync::Arc;

use broker::BrokerInner;

pub use config::{BctowConfig, OverflowPolicy};
pub use consumer::{ConsumerClient, ConsumerContext, ReceivedMessage};
pub use error::BctowError;
pub use producer::Producer;
pub use topic::{ExactTopic, TopicId, TopicKey};

#[cfg(feature = "metrics")]
pub use consumer::Metrics;

/// Create a matched `Producer` + `ConsumerContext` pair sharing the same broker state.
pub fn broker(config: BctowConfig) -> (Producer, ConsumerContext) {
    let overflow_policy = config.overflow_policy;
    let inner = Arc::new(BrokerInner::new(config));
    (
        Producer {
            inner: Arc::clone(&inner),
            cached_rings: std::cell::RefCell::new(Vec::new()),
            _not_sync: PhantomData,
        },
        ConsumerContext::new(inner, overflow_policy),
    )
}

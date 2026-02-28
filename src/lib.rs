pub mod config;
pub mod error;
pub mod topic;
pub(crate) mod ring;
pub(crate) mod broker;
pub mod producer;
pub mod consumer;

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
    let overflow_policy = config.overflow_policy.clone();
    let inner = Arc::new(BrokerInner::new(config));
    (
        Producer {
            inner: Arc::clone(&inner),
        },
        ConsumerContext::new(inner, overflow_policy),
    )
}

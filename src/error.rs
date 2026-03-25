use std::fmt;

use crate::topic::TopicId;

/// Errors returned by the bctow public API.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BctowError {
    /// The payload is too large for this broker's configuration.
    PayloadTooLarge { size: usize, max: usize },
    /// The requested topic was not found in this broker.
    TopicNotFound(String),
    /// The supplied topic id belongs to a different broker instance.
    TopicBrokerMismatch(TopicId),
    /// The caller-supplied receive buffer is too small for the next message.
    ReceiveBufferTooSmall { required: usize, available: usize },
}

impl fmt::Display for BctowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BctowError::PayloadTooLarge { size, max } => {
                write!(f, "payload too large: size {size} exceeds maximum {max}")
            }
            BctowError::TopicNotFound(desc) => write!(f, "topic not found: {desc}"),
            BctowError::TopicBrokerMismatch(topic) => {
                write!(f, "topic belongs to a different broker: {topic}")
            }
            BctowError::ReceiveBufferTooSmall {
                required,
                available,
            } => {
                write!(
                    f,
                    "receive buffer too small: required {required} bytes, available {available}"
                )
            }
        }
    }
}

impl std::error::Error for BctowError {}

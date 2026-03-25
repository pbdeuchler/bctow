use std::fmt;

use crate::topic::TopicId;

/// Errors returned by the bctow public API.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BctowError {
    /// The requested topic was not found in this broker.
    TopicNotFound(String),
    /// The supplied topic id belongs to a different broker instance.
    TopicBrokerMismatch(TopicId),
    /// The message exceeds the fixed-size buffer capacity.
    MessageTooLarge { size: usize, max: usize },
}

impl fmt::Display for BctowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BctowError::TopicNotFound(desc) => write!(f, "topic not found: {desc}"),
            BctowError::TopicBrokerMismatch(topic) => {
                write!(f, "topic belongs to a different broker: {topic}")
            }
            BctowError::MessageTooLarge { size, max } => {
                write!(f, "message too large: size {size} exceeds capacity {max}")
            }
        }
    }
}

impl std::error::Error for BctowError {}

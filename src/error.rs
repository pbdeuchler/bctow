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
}

impl fmt::Display for BctowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BctowError::TopicNotFound(desc) => write!(f, "topic not found: {desc}"),
            BctowError::TopicBrokerMismatch(topic) => {
                write!(f, "topic belongs to a different broker: {topic}")
            }
        }
    }
}

impl std::error::Error for BctowError {}

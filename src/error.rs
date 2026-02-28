use std::fmt;

use crate::topic::TopicId;

#[derive(Debug, PartialEq, Eq)]
pub enum BctowError {
    PayloadTooLarge { size: usize, max: usize },
    TopicNotFound(TopicId),
    BufferFull,
    NoSubscriptions,
}

impl fmt::Display for BctowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BctowError::PayloadTooLarge { size, max } => {
                write!(f, "payload too large: size {size} exceeds maximum {max}")
            }
            BctowError::TopicNotFound(id) => {
                write!(f, "topic not found: {id:?}")
            }
            BctowError::BufferFull => {
                write!(f, "buffer full")
            }
            BctowError::NoSubscriptions => {
                write!(f, "no subscriptions")
            }
        }
    }
}

impl std::error::Error for BctowError {}

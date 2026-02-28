use std::fmt;

/// Errors returned by the bctow public API.
///
/// This enum is `#[non_exhaustive]` so that new variants can be added in
/// future minor releases without breaking downstream match expressions.
#[non_exhaustive]
#[derive(Debug, PartialEq, Eq)]
pub enum BctowError {
    /// The payload is too large for this broker's configuration.
    ///
    /// `max_payload_size` is reserved for future payload-aware implementations
    /// where `T` is a byte-oriented type. For generic `T`, the size is not
    /// knowable at the broker level, so this variant is never constructed today.
    PayloadTooLarge { size: usize, max: usize },
    /// The requested topic was not found.
    ///
    /// The inner `String` is either the topic name (for string-based lookups)
    /// or the debug representation of the `TopicId` (for ID-based lookups).
    TopicNotFound(String),
}

impl fmt::Display for BctowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BctowError::PayloadTooLarge { size, max } => {
                write!(f, "payload too large: size {size} exceeds maximum {max}")
            }
            BctowError::TopicNotFound(desc) => {
                write!(f, "topic not found: {desc}")
            }
        }
    }
}

impl std::error::Error for BctowError {}

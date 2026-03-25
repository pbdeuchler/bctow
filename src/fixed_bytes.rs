use std::fmt;

use crate::error::BctowError;

/// Fixed-capacity byte buffer for variable-length messages in the typed ring.
///
/// `FixedBytes<N>` stores up to `N` bytes inline, making it `Copy` and suitable
/// for use as `T` in `broker::<FixedBytes<N>>()`. This preserves byte-oriented
/// semantics on top of the typed infrastructure without reintroducing dynamic
/// slot sizing.
#[derive(Clone, Copy)]
pub struct FixedBytes<const N: usize> {
    len: usize,
    bytes: [u8; N],
}

impl<const N: usize> FixedBytes<N> {
    /// Creates a `FixedBytes<N>` from a byte slice.
    ///
    /// Returns `BctowError::MessageTooLarge` if `data.len() > N`.
    pub fn new(data: &[u8]) -> Result<Self, BctowError> {
        let size = data.len();
        if size > N {
            return Err(BctowError::MessageTooLarge { size, max: N });
        }
        let mut bytes = [0u8; N];
        bytes[..size].copy_from_slice(data);
        Ok(Self { len: size, bytes })
    }

    /// Returns the stored bytes as a slice of their actual length.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    /// Returns the number of bytes stored.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no bytes are stored.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the maximum number of bytes this buffer can hold.
    pub fn capacity() -> usize {
        N
    }
}

impl<const N: usize> fmt::Debug for FixedBytes<N> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBytes")
            .field("len", &self.len)
            .field("bytes", &self.as_bytes())
            .finish()
    }
}

impl<const N: usize> PartialEq for FixedBytes<N> {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl<const N: usize> Eq for FixedBytes<N> {}

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

#[cfg(test)]
mod tests {
    use super::*;

    // Helper type aliases to keep test cases concise.
    type FB16 = FixedBytes<16>;
    type FB5 = FixedBytes<5>;
    type FB4 = FixedBytes<4>;
    type FB32 = FixedBytes<32>;

    // --- new / construction ---

    #[test]
    fn new_from_slice() {
        let fb = FB16::new(b"hello").unwrap();
        assert_eq!(fb.as_bytes(), b"hello");
        assert_eq!(fb.len(), 5);
        assert!(!fb.is_empty());
    }

    #[test]
    fn new_empty_slice() {
        let fb = FB16::new(b"").unwrap();
        assert_eq!(fb.as_bytes(), b"");
        assert_eq!(fb.len(), 0);
        assert!(fb.is_empty());
    }

    #[test]
    fn new_exact_capacity() {
        let fb = FB5::new(b"hello").unwrap();
        assert_eq!(fb.as_bytes(), b"hello");
        assert_eq!(fb.len(), 5);
    }

    #[test]
    fn new_too_large() {
        let err = FB4::new(b"hello").unwrap_err();
        assert_eq!(err, BctowError::MessageTooLarge { size: 5, max: 4 });
    }

    // --- capacity ---

    #[test]
    fn capacity_is_const_n() {
        assert_eq!(FB32::capacity(), 32);
        assert_eq!(FB16::capacity(), 16);
        assert_eq!(FB4::capacity(), 4);
    }

    // --- PartialEq / Eq ---

    #[test]
    fn partial_eq_compares_content_not_padding() {
        let a = FB16::new(b"hi").unwrap();
        let b = FB16::new(b"hi").unwrap();
        // Both are zero-initialised so padding is identical, but the key
        // invariant is that equality depends only on the live bytes.
        assert_eq!(a, b);
    }

    #[test]
    fn partial_eq_different_content() {
        let a = FB16::new(b"hello").unwrap();
        let b = FB16::new(b"world").unwrap();
        assert_ne!(a, b);
    }

    #[test]
    fn partial_eq_same_prefix_different_length_are_not_equal() {
        let a = FB16::new(b"hello").unwrap();
        let b = FB16::new(b"hell").unwrap();
        assert_ne!(a, b);
    }

    // --- Debug ---

    #[test]
    fn debug_shows_content() {
        let fb = FB16::new(b"abc").unwrap();
        let s = format!("{fb:?}");
        // The Debug impl prints as_bytes(), so the content must appear.
        assert!(s.contains("97")); // b'a' == 97
        assert!(s.contains("98")); // b'b' == 98
        assert!(s.contains("99")); // b'c' == 99
        assert!(s.contains("len"));
    }

    // --- Copy semantics ---

    #[test]
    fn copy_semantics() {
        let original = FB16::new(b"copy me").unwrap();
        let copy = original; // bitwise copy, not a move
        // Both are independently readable — no borrow-checker error proves Copy.
        assert_eq!(original.as_bytes(), b"copy me");
        assert_eq!(copy.as_bytes(), b"copy me");
        // They are equal in value ...
        assert_eq!(original, copy);
        // ... but modifying a new value derived from `copy` does not affect original.
        let modified = FB16::new(b"changed").unwrap();
        assert_ne!(original, modified);
    }
}

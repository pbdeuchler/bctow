#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverflowPolicy {
    /// Oldest messages are overwritten when the buffer is full.
    DropOldest,
    /// New messages are rejected when the buffer is full.
    DropNewest,
    /// A lagging consumer jumps to the latest message on overrun.
    SkipToLatest,
}

impl Default for OverflowPolicy {
    fn default() -> Self {
        OverflowPolicy::DropOldest
    }
}

#[derive(Debug, Clone)]
pub struct BctowConfig {
    /// Maximum payload size in bytes. Default: 16384 (16KB).
    ///
    /// **Reserved for future use.** For a generic `T`, the wire size of a
    /// message is not known at the broker level, so this field is not checked
    /// today. It will be enforced by a future implementation that constrains
    /// `T` to a byte-oriented trait (e.g. `AsRef<[u8]>`).
    pub max_payload_size: usize,
    /// Number of slots in the ring buffer per topic. Default: 1024.
    pub ring_buffer_capacity: usize,
    /// Policy applied when the buffer is full. Default: DropOldest.
    pub overflow_policy: OverflowPolicy,
}

impl Default for BctowConfig {
    fn default() -> Self {
        BctowConfig {
            max_payload_size: 16_384,
            ring_buffer_capacity: 1_024,
            overflow_policy: OverflowPolicy::default(),
        }
    }
}

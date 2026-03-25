#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum OverflowPolicy {
    /// A lagging consumer skips to the oldest message still retained in the ring.
    #[default]
    DropOldest,
    /// A lagging consumer stays pinned to its old cursor and stops making progress.
    DropNewest,
    /// A lagging consumer jumps to `latest + 1` and only sees future messages.
    SkipToLatest,
}

#[derive(Debug, Clone)]
pub struct BctowConfig {
    /// Number of slots in the per-topic ring buffer.
    pub ring_buffer_capacity: usize,
    /// Policy applied when a consumer falls behind retention.
    pub overflow_policy: OverflowPolicy,
}

impl Default for BctowConfig {
    fn default() -> Self {
        Self {
            ring_buffer_capacity: 1_024,
            overflow_policy: OverflowPolicy::default(),
        }
    }
}

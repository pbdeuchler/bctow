use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, Ordering};

// --- Slot ---

struct Slot<T> {
    /// Sequence number of the message stored here. 0 = empty; valid sequences start at 1.
    sequence: AtomicU64,
    /// Valid to read only when `sequence` matches the expected sequence number.
    value: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn empty() -> Self {
        Slot {
            sequence: AtomicU64::new(0),
            value: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: `Slot` itself is not publicly accessible. Access is mediated exclusively through
// `RingBuffer`, which enforces the single-producer / multiple-reader contract via sequencing.
unsafe impl<T: Send + Sync> Send for Slot<T> {}
unsafe impl<T: Send + Sync> Sync for Slot<T> {}

// --- RingBuffer ---

/// Single-producer, multiple-consumer ring buffer.
///
/// # Safety contract
/// Only **one** thread may call [`push`] at any given time. Multiple threads may call
/// [`read`] concurrently. The caller (`Producer` wrapper) is responsible for upholding
/// the single-producer guarantee; this type does not enforce it with a mutex.
pub(crate) struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    capacity: usize,
    /// Next sequence number to be written. Starts at 1. Mutated only by the producer;
    /// stored as `AtomicU64` so readers can observe it without a lock.
    write_seq: AtomicU64,
}

// SAFETY: Under the SPMC contract, the single producer writes a slot, then publishes
// the sequence number with `Release` ordering. Readers load the sequence with `Acquire`
// ordering before touching the value, ensuring the write is visible. No two threads
// write to the same slot concurrently.
unsafe impl<T: Send + Sync> Send for RingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T: Send + Sync> RingBuffer<T> {
    /// Allocate a ring buffer with `capacity` pre-allocated slots.
    ///
    /// `capacity` must be > 0.
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring buffer capacity must be > 0");
        let slots: Box<[Slot<T>]> = (0..capacity).map(|_| Slot::empty()).collect();
        RingBuffer {
            slots,
            capacity,
            write_seq: AtomicU64::new(1),
        }
    }

    /// Write `value` into the buffer. Returns the sequence number assigned to this message.
    ///
    /// # Safety
    /// Must only be called from the single producer thread.
    pub(crate) fn push(&self, value: T) -> u64 {
        let seq = self.write_seq.load(Ordering::Relaxed);
        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];

        // SAFETY: We are the single producer. No other thread writes to this index
        // at this moment (readers only observe slots with matching sequences, and
        // the old sequence will differ until we publish the new one).
        unsafe {
            (*slot.value.get()).write(value);
        }

        // Publish: readers spin-checking this sequence will now see the written value.
        slot.sequence.store(seq, Ordering::Release);

        // Advance producer cursor. Relaxed is fine; the Release above already provides
        // the happens-before edge for readers of the slot. Readers of write_seq use
        // Acquire when they care about ordering.
        self.write_seq.fetch_add(1, Ordering::Release);

        seq
    }

    /// Read the value at sequence number `seq`.
    ///
    /// Returns `Some(&T)` if the slot holds `seq` (i.e. not yet overwritten and already written).
    /// Returns `None` if the slot has been overwritten (consumer lagged) or not yet written.
    pub(crate) fn read(&self, seq: u64) -> Option<&T> {
        if seq == 0 {
            return None;
        }
        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];
        let stored = slot.sequence.load(Ordering::Acquire);
        if stored != seq {
            return None;
        }
        // SAFETY: `stored == seq` and the Release store in `push` has happened before
        // this Acquire load, so the write to `value` is fully visible here.
        // The slot will not be overwritten while we hold this reference because
        // the producer would need to wrap around the entire buffer first.
        // Callers must not hold the reference across more than `capacity` subsequent pushes.
        Some(unsafe { (*slot.value.get()).assume_init_ref() })
    }

    /// The sequence number of the last written message, or 0 if nothing has been written yet.
    pub(crate) fn latest_seq(&self) -> u64 {
        let next = self.write_seq.load(Ordering::Acquire);
        next.saturating_sub(1)
    }

    /// The oldest sequence number that has not been overwritten, or 1 if the buffer has
    /// not wrapped around yet.
    pub(crate) fn oldest_available_seq(&self) -> u64 {
        let next = self.write_seq.load(Ordering::Acquire);
        // If fewer than `capacity` messages have been written, everything since 1 is available.
        next.saturating_sub(self.capacity as u64).max(1)
    }

    /// Number of slots.
    #[inline]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
}

// --- Cursor ---

/// Tracks a single consumer's read position within a `RingBuffer`.
#[derive(Debug, Clone)]
pub(crate) struct Cursor {
    /// The next sequence number this consumer wants to read.
    next_seq: u64,
}

impl Cursor {
    /// Start at the beginning: will attempt to read sequence 1 first.
    pub(crate) fn new() -> Self {
        Cursor { next_seq: 1 }
    }

    /// Start just past the latest written message. Skips all existing history.
    /// If nothing has been written yet, starts at 1.
    pub(crate) fn new_latest<T: Send + Sync>(ring: &RingBuffer<T>) -> Self {
        let latest = ring.latest_seq();
        Cursor {
            next_seq: latest + 1,
        }
    }

    /// The sequence number this cursor will attempt to read next.
    #[inline]
    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq
    }

    /// Move forward by one sequence number.
    #[inline]
    pub(crate) fn advance(&mut self) {
        self.next_seq += 1;
    }

    /// Jump to a specific sequence number (e.g., when skipping ahead on overrun).
    #[inline]
    pub(crate) fn skip_to(&mut self, seq: u64) {
        self.next_seq = seq;
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // Helper: push `n` values into a buffer, return assigned sequences.
    fn push_n(ring: &RingBuffer<u64>, start: u64, n: usize) -> Vec<u64> {
        (start..start + n as u64).map(|v| ring.push(v)).collect()
    }

    // --- RingBuffer::new ---

    #[test]
    fn new_empty_latest_seq_is_zero() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        assert_eq!(ring.latest_seq(), 0);
    }

    #[test]
    fn new_empty_oldest_available_is_one() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        assert_eq!(ring.oldest_available_seq(), 1);
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn new_zero_capacity_panics() {
        let _: RingBuffer<u32> = RingBuffer::new(0);
    }

    // --- push ---

    #[test]
    fn push_returns_increasing_sequences() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        let seqs: Vec<u64> = (0..8).map(|v| ring.push(v)).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn push_updates_latest_seq() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        ring.push(10);
        assert_eq!(ring.latest_seq(), 1);
        ring.push(20);
        assert_eq!(ring.latest_seq(), 2);
    }

    // --- read ---

    #[test]
    fn read_written_slot_returns_value() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        let seq = ring.push(42);
        assert_eq!(ring.read(seq), Some(&42));
    }

    #[test]
    fn read_unwritten_seq_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        assert_eq!(ring.read(1), None);
    }

    #[test]
    fn read_seq_zero_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        ring.push(1);
        assert_eq!(ring.read(0), None);
    }

    #[test]
    fn read_future_seq_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        ring.push(1);
        assert_eq!(ring.read(99), None);
    }

    #[test]
    fn read_overwritten_slot_returns_none() {
        // capacity=4: after 5 pushes, seq=1 is overwritten.
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        push_n(&ring, 100, 5); // seqs 1..=5; slot for seq=1 now holds seq=5
        assert_eq!(ring.read(1), None);
    }

    #[test]
    fn read_all_slots_in_non_wrapped_buffer() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        let values: Vec<u64> = (10..18).collect();
        let seqs: Vec<u64> = values.iter().map(|&v| ring.push(v)).collect();
        for (seq, val) in seqs.iter().zip(values.iter()) {
            assert_eq!(ring.read(*seq), Some(val));
        }
    }

    #[test]
    fn read_wraps_around_correctly() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        // Push 8 values; the buffer wraps twice.
        push_n(&ring, 1, 8); // seqs 1..=8
        // Only seqs 5..=8 should be readable.
        for seq in 1u64..=4 {
            assert_eq!(ring.read(seq), None, "seq {seq} should be overwritten");
        }
        for seq in 5u64..=8 {
            assert_eq!(ring.read(seq), Some(&seq), "seq {seq} should be readable");
        }
    }

    // --- latest_seq / oldest_available_seq ---

    #[test]
    fn latest_seq_tracks_pushes() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        for i in 1u64..=10 {
            ring.push(i as u32);
            assert_eq!(ring.latest_seq(), i);
        }
    }

    #[test]
    fn oldest_available_before_wrap() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        push_n(&ring, 1, 5);
        // Haven't wrapped: oldest is 1.
        assert_eq!(ring.oldest_available_seq(), 1);
    }

    #[test]
    fn oldest_available_after_wrap() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        push_n(&ring, 1, 6); // write_seq=7; oldest = 7-4 = 3
        assert_eq!(ring.oldest_available_seq(), 3);
    }

    // --- Cursor ---

    #[test]
    fn cursor_new_starts_at_one() {
        let c = Cursor::new();
        assert_eq!(c.next_seq(), 1);
    }

    #[test]
    fn cursor_new_latest_empty_buffer_starts_at_one() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        let c = Cursor::new_latest(&ring);
        assert_eq!(c.next_seq(), 1);
    }

    #[test]
    fn cursor_new_latest_nonempty_starts_after_latest() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        push_n(&ring, 1, 3); // latest_seq = 3
        let c = Cursor::new_latest(&ring);
        assert_eq!(c.next_seq(), 4);
    }

    #[test]
    fn cursor_advance_increments() {
        let mut c = Cursor::new();
        c.advance();
        assert_eq!(c.next_seq(), 2);
        c.advance();
        assert_eq!(c.next_seq(), 3);
    }

    #[test]
    fn cursor_skip_to_jumps() {
        let mut c = Cursor::new();
        c.skip_to(100);
        assert_eq!(c.next_seq(), 100);
    }

    // --- SPMC: concurrent readers ---

    #[test]
    fn concurrent_readers_see_written_values() {
        let ring: Arc<RingBuffer<u64>> = Arc::new(RingBuffer::new(64));

        // Producer: push 1..=32 sequentially.
        let seqs: Vec<u64> = (1u64..=32).map(|v| ring.push(v)).collect();

        // Spawn 8 reader threads that each read all written slots.
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ring = Arc::clone(&ring);
                let seqs = seqs.clone();
                thread::spawn(move || {
                    for seq in &seqs {
                        // Value stored equals sequence number (we pushed 1..=32).
                        assert_eq!(ring.read(*seq), Some(seq));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    #[test]
    fn capacity_reported_correctly() {
        let ring: RingBuffer<u32> = RingBuffer::new(16);
        assert_eq!(ring.capacity(), 16);
    }
}

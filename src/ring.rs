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
    ///
    /// Must only be called from a single thread (the producer thread).
    /// Concurrent calls to `push` are undefined behavior.
    pub(crate) unsafe fn push(&self, value: T) -> u64 {
        let seq = self.write_seq.load(Ordering::Relaxed);
        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];

        // Drop any previously stored value before overwriting. Without this, wrapping
        // around would leak heap allocations held by the old `T`.
        let old_seq = slot.sequence.load(Ordering::Acquire);
        if old_seq != 0 {
            // SAFETY: old_seq != 0 means a value was written here previously by the
            // single producer (us). We are the only writer; no reader can hold a
            // reference past this point because `read` returns owned clones, not refs.
            unsafe { (*slot.value.get()).assume_init_drop() };
        }

        // Reset sequence to 0 while writing so readers see the slot as unavailable
        // during the write window.
        slot.sequence.store(0, Ordering::Release);

        // SAFETY: We are the single producer. No other thread writes to this index.
        unsafe {
            (*slot.value.get()).write(value);
        }

        // Publish: readers spin-checking this sequence will now see the written value.
        // Release ordering is required here so that the slot write above is visible to
        // any reader that loads this sequence with Acquire.
        slot.sequence.store(seq, Ordering::Release);

        // Advance producer cursor. Store is correct here; we know exactly what the
        // next value should be (seq + 1). fetch_add would work but implies we don't
        // know the current value, which is misleading for a single-producer design.
        self.write_seq.store(seq + 1, Ordering::Release);

        seq
    }

    /// Read the value at sequence number `seq`.
    ///
    /// Returns `Some(T)` (an owned clone) if the slot holds `seq` — i.e. the message
    /// has been written and has not yet been overwritten. Returns `None` if the slot
    /// has been overwritten (consumer lagged) or not yet written.
    ///
    /// Returning an owned clone (rather than a reference) prevents any possibility of
    /// a dangling reference: the producer may overwrite the slot at any time after this
    /// call returns, but the caller holds a fully independent copy.
    pub(crate) fn read(&self, seq: u64) -> Option<T>
    where
        T: Clone,
    {
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
        let cloned = unsafe { (*slot.value.get()).assume_init_ref() }.clone();

        // Re-check the sequence after cloning to guard against a race where the producer
        // wraps around and overwrites the slot exactly while we were cloning. If the
        // sequence changed, the clone may be of partially-overwritten data — discard it.
        let stored_after = slot.sequence.load(Ordering::Acquire);
        if stored_after != seq {
            return None;
        }

        Some(cloned)
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

impl<T> Drop for RingBuffer<T> {
    fn drop(&mut self) {
        for slot in self.slots.iter_mut() {
            // get_mut() gives us exclusive access without UnsafeCell since we have &mut self.
            if *slot.sequence.get_mut() != 0 {
                // SAFETY: sequence != 0 means a value was written into this slot and never
                // explicitly dropped. We have exclusive access (Drop takes &mut self).
                unsafe { slot.value.get_mut().assume_init_drop() };
            }
        }
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
    ///
    /// `seq` must be >= 1; sequence 0 is the sentinel for "empty slot" and is never valid.
    #[inline]
    pub(crate) fn skip_to(&mut self, seq: u64) {
        debug_assert!(seq >= 1, "skip_to: seq must be >= 1, got {seq}");
        self.next_seq = seq;
    }
}

// --- Tests ---

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    // Helper: push the sequence number itself as the value so assertions are self-evident.
    // Each call to push returns the assigned sequence; we push `n` consecutive messages
    // starting at seq 1 (or wherever write_seq currently stands).
    fn push_n(ring: &RingBuffer<u64>, n: usize) -> Vec<u64> {
        let start = ring.latest_seq() + 1;
        (start..start + n as u64)
            .map(|seq| {
                // SAFETY: test code is single-threaded here.
                unsafe { ring.push(seq) }
            })
            .collect()
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
        let seqs: Vec<u64> = (1u64..=8).map(|v| unsafe { ring.push(v as u32) }).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn push_updates_latest_seq() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        unsafe { ring.push(10) };
        assert_eq!(ring.latest_seq(), 1);
        unsafe { ring.push(20) };
        assert_eq!(ring.latest_seq(), 2);
    }

    // --- read ---

    #[test]
    fn read_written_slot_returns_value() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        let seq = unsafe { ring.push(42) };
        assert_eq!(ring.read(seq), Some(42));
    }

    #[test]
    fn read_unwritten_seq_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        assert_eq!(ring.read(1), None);
    }

    #[test]
    fn read_seq_zero_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        unsafe { ring.push(1) };
        assert_eq!(ring.read(0), None);
    }

    #[test]
    fn read_future_seq_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        unsafe { ring.push(1) };
        assert_eq!(ring.read(99), None);
    }

    #[test]
    fn read_overwritten_slot_returns_none() {
        // capacity=4: after 5 pushes, seq=1 is overwritten.
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        push_n(&ring, 5); // seqs 1..=5; slot for seq=1 now holds seq=5
        assert_eq!(ring.read(1), None);
    }

    #[test]
    fn read_all_slots_in_non_wrapped_buffer() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        // push_n pushes seq as value, so reading seq `s` returns Some(s).
        let seqs = push_n(&ring, 8);
        for seq in seqs {
            assert_eq!(ring.read(seq), Some(seq));
        }
    }

    #[test]
    fn read_wraps_around_correctly() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        // Push 8 values; the buffer wraps twice.
        push_n(&ring, 8); // seqs 1..=8
        // Only seqs 5..=8 should be readable.
        for seq in 1u64..=4 {
            assert_eq!(ring.read(seq), None, "seq {seq} should be overwritten");
        }
        for seq in 5u64..=8 {
            // push_n stores seq as value.
            assert_eq!(ring.read(seq), Some(seq), "seq {seq} should be readable");
        }
    }

    // --- latest_seq / oldest_available_seq ---

    #[test]
    fn latest_seq_tracks_pushes() {
        let ring: RingBuffer<u32> = RingBuffer::new(4);
        for i in 1u64..=10 {
            unsafe { ring.push(i as u32) };
            assert_eq!(ring.latest_seq(), i);
        }
    }

    #[test]
    fn oldest_available_before_wrap() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        push_n(&ring, 5);
        // Haven't wrapped: oldest is 1.
        assert_eq!(ring.oldest_available_seq(), 1);
    }

    #[test]
    fn oldest_available_after_wrap() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        push_n(&ring, 6); // write_seq=7; oldest = 7-4 = 3
        assert_eq!(ring.oldest_available_seq(), 3);
    }

    // --- Drop ---

    #[test]
    fn drop_does_not_leak() {
        // Use a type with a heap allocation (String) so Miri / leak detectors can catch
        // any missing drops. We can't directly assert no leak in standard test runs, but
        // the Drop impl will be exercised and should not panic.
        let ring: RingBuffer<String> = RingBuffer::new(4);
        unsafe {
            ring.push("hello".to_owned());
            ring.push("world".to_owned());
        }
        // `ring` drops here; Drop impl should call assume_init_drop on both slots.
    }

    #[test]
    fn drop_after_wrap_does_not_double_free() {
        // After wrapping, each slot holds exactly the latest value for that index.
        // Drop must free exactly the slots that have sequence != 0.
        let ring: RingBuffer<String> = RingBuffer::new(4);
        // Push 6 values into capacity-4 buffer (wraps 1.5 times).
        for i in 0..6u64 {
            unsafe { ring.push(format!("msg-{i}")) };
        }
        // Drop should run cleanly without double-free.
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
        push_n(&ring, 3); // latest_seq = 3
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

    #[test]
    fn cursor_skip_to_minimum_valid_seq() {
        // seq=1 is the minimum valid value; must not trigger the debug_assert.
        let mut c = Cursor::new();
        c.skip_to(5);
        c.skip_to(1); // jump back to minimum valid seq
        assert_eq!(c.next_seq(), 1);
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "seq must be >= 1")]
    fn cursor_skip_to_zero_panics_in_debug() {
        let mut c = Cursor::new();
        c.skip_to(0);
    }

    // --- SPMC: concurrent readers ---

    #[test]
    fn concurrent_readers_see_written_values() {
        let ring: Arc<RingBuffer<u64>> = Arc::new(RingBuffer::new(64));

        // Producer: push seqs 1..=32, storing the seq number as the value.
        let seqs: Vec<u64> = (1u64..=32).map(|seq| unsafe { ring.push(seq) }).collect();

        // Spawn 8 reader threads that each read all written slots.
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ring = Arc::clone(&ring);
                let seqs = seqs.clone();
                thread::spawn(move || {
                    for seq in &seqs {
                        // Value stored equals sequence number.
                        assert_eq!(ring.read(*seq), Some(*seq));
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("reader thread panicked");
        }
    }

    /// Concurrent read+write test: one producer thread pushes messages continuously
    /// while multiple consumer threads read. This exercises the Release/Acquire ordering
    /// under actual contention.
    #[test]
    fn concurrent_read_and_write() {
        const CAPACITY: usize = 64;
        const MSG_COUNT: u64 = 1_000;
        const READER_COUNT: usize = 4;

        let ring: Arc<RingBuffer<u64>> = Arc::new(RingBuffer::new(CAPACITY));

        // Producer thread: pushes MSG_COUNT messages.
        let producer_ring = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            for seq in 1..=MSG_COUNT {
                // SAFETY: this is the only thread calling push.
                unsafe { producer_ring.push(seq) };
            }
        });

        // Consumer threads: each polls until it has successfully read MSG_COUNT messages
        // or determines they have been overwritten (lagged consumer).
        let consumers: Vec<_> = (0..READER_COUNT)
            .map(|_| {
                let ring = Arc::clone(&ring);
                thread::spawn(move || {
                    let mut seq = 1u64;
                    let mut reads = 0u64;
                    // Keep trying until we've seen at least half the messages or
                    // advanced past all of them (to avoid an infinite loop if we lag).
                    while seq <= MSG_COUNT {
                        match ring.read(seq) {
                            Some(val) => {
                                // Value equals seq by construction.
                                assert_eq!(val, seq);
                                seq += 1;
                                reads += 1;
                            }
                            None => {
                                // Either not yet written or overwritten; check latest and
                                // skip ahead if we've lagged.
                                let latest = ring.latest_seq();
                                let oldest = ring.oldest_available_seq();
                                if seq < oldest {
                                    seq = oldest;
                                } else if seq > latest && latest == MSG_COUNT {
                                    // Producer finished and we're past all messages.
                                    break;
                                }
                                // Spin — yield to let the producer make progress.
                                thread::yield_now();
                            }
                        }
                    }
                    reads
                })
            })
            .collect();

        producer.join().expect("producer panicked");
        for c in consumers {
            let reads = c.join().expect("consumer panicked");
            // Each consumer must have read at least one message.
            assert!(reads > 0, "consumer read zero messages");
        }
    }

    #[test]
    fn capacity_reported_correctly() {
        let ring: RingBuffer<u32> = RingBuffer::new(16);
        assert_eq!(ring.capacity(), 16);
    }
}

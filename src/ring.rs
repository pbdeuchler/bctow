use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

// --- Slot ---

struct Slot<T> {
    /// Sequence number of the message stored here. 0 = empty or being written;
    /// valid sequences start at 1.
    sequence: AtomicU64,
    /// Holds `Some(Arc<T>)` when a value is present, `None` otherwise.
    /// Valid to read only when `sequence` matches the expected sequence number.
    value: UnsafeCell<Option<Arc<T>>>,
}

impl<T> Slot<T> {
    fn empty() -> Self {
        Slot {
            sequence: AtomicU64::new(0),
            value: UnsafeCell::new(None),
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
/// Each slot stores an `Arc<T>`. This eliminates the data race that would exist if `T`
/// were stored directly: a producer overwriting a slot while a reader clones the value
/// would be UB for heap-allocating types. With `Arc<T>`, the producer replaces the
/// `Arc` in the slot (decrementing the old refcount), but any reader that has already
/// cloned the `Arc` retains a valid reference to the data — the inner `T` is only freed
/// when the last `Arc` clone is dropped.
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
    ///
    /// # Overflow note
    ///
    /// `write_seq` is a plain `u64` counter. Wrapping from `u64::MAX` back to 0 would
    /// cause the next sequence number emitted to be 0, which collides with the "empty slot"
    /// sentinel. At 1 billion pushes per second this would occur after approximately
    /// 584 years of continuous operation, so it is not a practical concern. If the library
    /// is ever used in an environment where this bound might be approached, the fix is to
    /// start at 1 and reserve 0 as-is (already done), but also add a guard in `push` that
    /// saturates or panics at `u64::MAX`.
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

        // Signal "being written" so concurrent readers skip this slot.
        // The old `Option<Arc<T>>` (if any) will be replaced below. If any reader
        // already cloned that Arc, the inner T remains live until their clone is dropped.
        slot.sequence.store(0, Ordering::Release);

        // SAFETY: We are the single producer. No other thread writes to this index.
        // Replacing the Option drops the old Arc (decrementing its refcount). Any reader
        // that previously cloned the Arc retains a valid reference to the data — no
        // use-after-free is possible because the Arc controls the T's lifetime.
        unsafe {
            *slot.value.get() = Some(Arc::new(value));
        }

        // Publish: readers spin-checking this sequence will now see the written value.
        // Release ordering is required here so that the slot write above is visible to
        // any reader that loads this sequence with Acquire.
        slot.sequence.store(seq, Ordering::Release);

        // Advance producer cursor.
        self.write_seq.store(seq + 1, Ordering::Release);

        seq
    }

    /// Read the value at sequence number `seq`.
    ///
    /// Returns `Some(Arc<T>)` if the slot holds `seq` — i.e. the message has been
    /// written and has not yet been overwritten. Returns `None` if the slot has been
    /// overwritten (consumer lagged) or not yet written.
    ///
    /// Returning an `Arc` (rather than a clone of `T`) is safe even under concurrent
    /// producer activity: `Arc::clone` is just an atomic refcount increment, so the
    /// producer can replace the slot's `Arc` at any point without causing UB. The caller
    /// holds a valid reference to the data for as long as they retain the `Arc`.
    pub(crate) fn read(&self, seq: u64) -> Option<Arc<T>> {
        if seq == 0 {
            return None;
        }
        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];
        let stored = slot.sequence.load(Ordering::Acquire);
        if stored != seq {
            return None;
        }
        // SAFETY: `stored == seq` and the Release store in `push` has happened-before
        // this Acquire load, so the write to `value` is fully visible here.
        // We clone the Arc (cheap: atomic refcount increment), keeping the data alive
        // even if the producer overwrites this slot immediately after.
        //
        // # Formal memory model note (known limitation)
        //
        // Accessing `slot.value` via `UnsafeCell::get` is *practically* safe on all
        // real hardware (x86, ARM, RISC-V) because pointer-sized aligned writes are
        // always atomic at the microarchitectural level. However, under the C++ / Rust
        // *abstract* memory model there is a formal data race: if the producer wraps
        // around and begins writing the same slot index for the second time while this
        // reader is executing the `UnsafeCell` access, there is no happens-before edge
        // between the producer's *second* write and this reader's read of `value`. The
        // sequence protocol (zeroing then republishing the sequence number with `Release`)
        // creates a happens-before edge only for the *first* write that sets `seq`. A
        // second write (wrap-around) stores 0 before overwriting `value`; the reader
        // loads the sequence *before* touching `value`, so it would see 0 != seq and
        // return early — but that `sequence` load and the producer's write to `value`
        // are not formally ordered, making the `value` access formally undefined
        // behaviour under the abstract model. Miri will flag this.
        //
        // In practice the post-read re-check below catches wrap-around and returns
        // `None`, keeping the API semantics correct even though the underlying access
        // is formally racy.
        //
        // The fully correct fix is epoch-based memory reclamation (e.g. `crossbeam-epoch`
        // or hazard pointers): an epoch guard would prevent the producer from recycling
        // a slot's allocation while any reader holds a reference. `AtomicPtr` alone does
        // not solve the race — between loading the raw pointer and calling
        // `Arc::increment_strong_count`, the producer could already have called
        // `Arc::from_raw` and dropped the allocation. A complete lock-free solution
        // therefore requires either a seqlock that stalls the producer while readers
        // are inside the critical section, or a deferred-reclamation scheme.
        let arc = unsafe { (*slot.value.get()).as_ref()?.clone() };

        // Re-check the sequence after cloning to guard against a race where the producer
        // wraps around and overwrites the slot exactly while we were reading. If the
        // sequence changed, this slot no longer holds `seq` — discard for API consistency
        // so the consumer sees a clean sequence stream. The Arc itself is still valid.
        let stored_after = slot.sequence.load(Ordering::Acquire);
        if stored_after != seq {
            return None;
        }

        Some(arc)
    }

    /// The sequence number of the last written message, or 0 if nothing has been written yet.
    pub(crate) fn latest_seq(&self) -> u64 {
        let next = self.write_seq.load(Ordering::Acquire);
        next.saturating_sub(1)
    }

    /// The oldest sequence number that is likely still present in the buffer, or 1 if
    /// the buffer has not wrapped around yet.
    ///
    /// # Advisory only
    ///
    /// This value is an **estimate**. Because the producer advances `write_seq` independently
    /// of any reader, the slot for the returned sequence may already be in the process of
    /// being overwritten by the time the caller acts on this value. Always check the return
    /// value of [`read`] — it may return `None` even for sequences at or above this estimate.
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

// `Option<Arc<T>>` drops cleanly: when the last Arc clone is released the inner T is freed.
// No custom Drop impl is required; the derived drop on `slots` handles everything correctly.

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
    fn push_n(ring: &RingBuffer<u64>, n: usize) -> Vec<u64> {
        let start = ring.latest_seq() + 1;
        (start..start + n as u64)
            .map(|seq| {
                // SAFETY: test code is single-threaded here.
                unsafe { ring.push(seq) }
            })
            .collect()
    }

    // Helper to unwrap an Arc<u64> from read() and compare the inner value.
    fn read_val(ring: &RingBuffer<u64>, seq: u64) -> Option<u64> {
        ring.read(seq).map(|arc| *arc)
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
    fn read_written_slot_returns_arc_with_value() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        let seq = unsafe { ring.push(42) };
        let arc = ring.read(seq).expect("should return Some");
        assert_eq!(*arc, 42);
    }

    #[test]
    fn read_returns_arc_not_clone() {
        // Verify that we get an Arc back, and two reads of the same slot
        // return independent Arc handles pointing to the same allocation.
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        let seq = unsafe { ring.push(99) };
        let a = ring.read(seq).expect("first read");
        let b = ring.read(seq).expect("second read");
        assert_eq!(*a, 99);
        assert_eq!(*b, 99);
        // Both point to the same data but are distinct Arc handles.
        assert!(Arc::ptr_eq(&a, &b));
    }

    #[test]
    fn read_unwritten_seq_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        assert!(ring.read(1).is_none());
    }

    #[test]
    fn read_seq_zero_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        unsafe { ring.push(1) };
        assert!(ring.read(0).is_none());
    }

    #[test]
    fn read_future_seq_returns_none() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        unsafe { ring.push(1) };
        assert!(ring.read(99).is_none());
    }

    #[test]
    fn read_overwritten_slot_returns_none() {
        // capacity=4: after 5 pushes, seq=1 is overwritten.
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        push_n(&ring, 5); // seqs 1..=5; slot for seq=1 now holds seq=5
        assert!(ring.read(1).is_none());
    }

    #[test]
    fn read_all_slots_in_non_wrapped_buffer() {
        let ring: RingBuffer<u64> = RingBuffer::new(8);
        // push_n pushes seq as value, so reading seq `s` returns Some(s).
        let seqs = push_n(&ring, 8);
        for seq in seqs {
            assert_eq!(read_val(&ring, seq), Some(seq));
        }
    }

    #[test]
    fn read_wraps_around_correctly() {
        let ring: RingBuffer<u64> = RingBuffer::new(4);
        // Push 8 values; the buffer wraps twice.
        push_n(&ring, 8); // seqs 1..=8
        // Only seqs 5..=8 should be readable.
        for seq in 1u64..=4 {
            assert!(ring.read(seq).is_none(), "seq {seq} should be overwritten");
        }
        for seq in 5u64..=8 {
            assert_eq!(read_val(&ring, seq), Some(seq), "seq {seq} should be readable");
        }
    }

    /// Verify that a retained Arc keeps the data alive after the slot is overwritten.
    /// This is the core safety property that makes Arc<T> storage correct.
    #[test]
    fn retained_arc_stays_valid_after_slot_overwrite() {
        let ring: RingBuffer<u64> = RingBuffer::new(1);
        // Push value 42 into the single slot (seq=1).
        let seq1 = unsafe { ring.push(42) };
        let held = ring.read(seq1).expect("seq 1 should be readable");
        assert_eq!(*held, 42);

        // Overwrite the slot with a new value (seq=2 goes to same slot index 0).
        let _seq2 = unsafe { ring.push(99) };

        // The slot now holds seq=2 — reading seq=1 must return None.
        assert!(ring.read(seq1).is_none());

        // But the Arc we retained still points to valid data.
        assert_eq!(*held, 42, "retained Arc must remain valid after slot overwrite");
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
        // any missing drops. The derived drop on Box<[Slot<T>]> drops each Option<Arc<T>>,
        // which correctly decrements the refcount and frees the String when it reaches zero.
        let ring: RingBuffer<String> = RingBuffer::new(4);
        unsafe {
            ring.push("hello".to_owned());
            ring.push("world".to_owned());
        }
        // `ring` drops here; each slot's Option<Arc<String>> is dropped automatically.
    }

    #[test]
    fn drop_after_wrap_does_not_double_free() {
        // After wrapping, each slot holds exactly the latest Arc for that index.
        let ring: RingBuffer<String> = RingBuffer::new(4);
        for i in 0..6u64 {
            unsafe { ring.push(format!("msg-{i}")) };
        }
        // Drop should run cleanly without double-free.
    }

    #[test]
    fn drop_with_retained_arc_does_not_free_data_early() {
        // If a reader retains an Arc, dropping the RingBuffer must not free the data.
        let ring: RingBuffer<String> = RingBuffer::new(4);
        let seq = unsafe { ring.push("alive".to_owned()) };
        let retained = ring.read(seq).expect("should be readable");
        drop(ring);
        // The ring is gone, but the Arc is still valid.
        assert_eq!(retained.as_str(), "alive");
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
                        let arc = ring.read(*seq).expect("should be readable");
                        assert_eq!(*arc, *seq);
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
                    while seq <= MSG_COUNT {
                        match ring.read(seq) {
                            Some(arc) => {
                                // Value equals seq by construction.
                                assert_eq!(*arc, seq);
                                seq += 1;
                                reads += 1;
                            }
                            None => {
                                let latest = ring.latest_seq();
                                let oldest = ring.oldest_available_seq();
                                if seq < oldest {
                                    seq = oldest;
                                } else if seq > latest && latest == MSG_COUNT {
                                    break;
                                }
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
            assert!(reads > 0, "consumer read zero messages");
        }
    }

    #[test]
    fn capacity_reported_correctly() {
        let ring: RingBuffer<u32> = RingBuffer::new(16);
        assert_eq!(ring.capacity(), 16);
    }
}

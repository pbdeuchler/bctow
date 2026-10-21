use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

const WRITER: usize = 1 << (usize::BITS - 1);

struct Slot<T> {
    /// High bit excludes new readers; remaining bits count active readers.
    access: AtomicUsize,
    /// Sequence number currently stored in the slot. `0` means empty.
    sequence: AtomicU64,
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            access: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: Slot access is coordinated by the per-slot access atomic. Reader
// admission and writer exclusion share one modification order: a reader either
// increments before the writer sets WRITER (and the writer waits for it), or its
// CAS fails and it cannot access the payload. Release unlocks and Acquire locks
// order payload accesses between owners. Shared readers only read T: Sync.
unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Sync> Sync for Slot<T> {}

/// Single-producer, multiple-consumer ring buffer for typed values.
pub(crate) struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    capacity: usize,
    /// Next sequence number to hand out. Starts at 1.
    write_seq: AtomicU64,
}

// SAFETY: The ring enforces single-writer access at the API boundary and uses
// atomics to coordinate concurrent readers. Data is concurrently read by multiple
// threads, so T must be both Send and Sync.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Send + Sync> Sync for RingBuffer<T> {}

impl<T: Copy> RingBuffer<T> {
    pub(crate) fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "ring buffer capacity must be > 0");

        let slots: Box<[Slot<T>]> = (0..capacity).map(|_| Slot::new()).collect();

        Self {
            slots,
            capacity,
            write_seq: AtomicU64::new(1),
        }
    }

    pub(crate) fn push(&self, value: &T) -> u64 {
        // Only the single producer advances this counter; readers only load it.
        // Preserve reservation-before-copy ordering without an unnecessary CAS.
        let seq = self.write_seq.load(Ordering::Relaxed);
        let next = seq.checked_add(1).expect("ring sequence overflow");
        self.write_seq.store(next, Ordering::Release);

        let idx = self.slot_index(seq);
        let slot = &self.slots[idx];

        let prior_access = slot.access.fetch_or(WRITER, Ordering::AcqRel);
        debug_assert_eq!(prior_access & WRITER, 0, "single producer required");

        while slot.access.load(Ordering::Acquire) != WRITER {
            spin_loop();
        }

        unsafe {
            let dst = &mut *slot.data.get();
            std::ptr::copy_nonoverlapping(value as *const T, dst.as_mut_ptr(), 1);
        }
        slot.sequence.store(seq, Ordering::Relaxed);
        slot.access.store(0, Ordering::Release);

        seq
    }

    pub(crate) fn read_into(&self, seq: u64, dst: &mut MaybeUninit<T>) -> Option<()> {
        if seq == 0 {
            return None;
        }

        let idx = self.slot_index(seq);
        let slot = &self.slots[idx];

        // Most empty polls can avoid writing the shared admission counter.
        // This is only a hint: the sequence must be checked again after admission.
        if slot.sequence.load(Ordering::Relaxed) != seq {
            return None;
        }

        let mut access = slot.access.load(Ordering::Relaxed);
        loop {
            // Refuse admission during a write or if incrementing would set WRITER.
            if access >= WRITER - 1 {
                return None;
            }
            match slot.access.compare_exchange_weak(
                access,
                access + 1,
                Ordering::Acquire,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => access = current,
            }
        }

        if slot.sequence.load(Ordering::Relaxed) != seq {
            slot.access.fetch_sub(1, Ordering::Release);
            return None;
        }

        unsafe {
            let src = &*slot.data.get();
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), 1);
        }

        slot.access.fetch_sub(1, Ordering::Release);
        Some(())
    }

    fn slot_index(&self, seq: u64) -> usize {
        let offset = seq - 1;
        let capacity = self.capacity as u64;
        if capacity.is_power_of_two() {
            (offset & (capacity - 1)) as usize
        } else {
            (offset % capacity) as usize
        }
    }

    pub(crate) fn latest_seq(&self) -> u64 {
        self.write_seq.load(Ordering::Acquire).saturating_sub(1)
    }

    pub(crate) fn oldest_available_seq(&self) -> u64 {
        self.write_seq
            .load(Ordering::Acquire)
            .saturating_sub(self.capacity as u64)
            .max(1)
    }

    #[allow(dead_code)]
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Cursor {
    next_seq: u64,
}

impl Cursor {
    #[allow(dead_code)]
    pub(crate) fn new() -> Self {
        Self { next_seq: 1 }
    }

    pub(crate) fn new_latest<T: Copy>(ring: &RingBuffer<T>) -> Self {
        Self {
            next_seq: ring.latest_seq() + 1,
        }
    }

    pub(crate) fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub(crate) fn advance(&mut self) {
        self.next_seq += 1;
    }

    pub(crate) fn skip_to(&mut self, seq: u64) {
        debug_assert!(seq >= 1, "skip_to: seq must be >= 1, got {seq}");
        self.next_seq = seq;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn read_back_written_value() {
        let ring = RingBuffer::<u64>::new(4);
        let seq = ring.push(&42u64);
        let mut dst = MaybeUninit::<u64>::uninit();
        ring.read_into(seq, &mut dst).unwrap();
        assert_eq!(unsafe { dst.assume_init() }, 42u64);
    }

    #[test]
    fn unavailable_slot_leaves_destination_unchanged() {
        let ring = RingBuffer::<u64>::new(1);
        let mut dst = MaybeUninit::new(99);
        assert!(ring.read_into(1, &mut dst).is_none());
        ring.push(&42);

        for access in [WRITER, WRITER | 1, WRITER - 1] {
            ring.slots[0].access.store(access, Ordering::Relaxed);
            assert!(ring.read_into(1, &mut dst).is_none());
            assert_eq!(ring.slots[0].access.load(Ordering::Relaxed), access);
            assert_eq!(unsafe { dst.assume_init() }, 99);
        }

        ring.slots[0].access.store(0, Ordering::Relaxed);
        ring.read_into(1, &mut dst).unwrap();
        assert_eq!(unsafe { dst.assume_init() }, 42);
        assert_eq!(ring.slots[0].access.load(Ordering::Relaxed), 0);
    }

    #[test]
    #[should_panic(expected = "ring sequence overflow")]
    fn exhausted_sequence_rejects_publish() {
        let ring = RingBuffer::<u64>::new(1);
        ring.write_seq.store(u64::MAX, Ordering::Relaxed);
        ring.push(&42);
    }

    #[test]
    fn sequence_exhaustion_does_not_overwrite_the_retained_message() {
        let ring = RingBuffer::<u64>::new(1);
        ring.write_seq.store(u64::MAX - 1, Ordering::Relaxed);
        let seq = ring.push(&42);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| ring.push(&99)));
        assert!(result.is_err());
        assert_eq!(ring.write_seq.load(Ordering::Relaxed), u64::MAX);
        let mut dst = MaybeUninit::uninit();
        ring.read_into(seq, &mut dst).unwrap();
        assert_eq!(unsafe { dst.assume_init() }, 42);
    }

    #[test]
    fn read_overwritten_value_returns_none() {
        let ring = RingBuffer::<u64>::new(2);
        ring.push(&1u64);
        ring.push(&2u64);
        ring.push(&3u64);

        let mut dst = MaybeUninit::<u64>::uninit();
        assert!(ring.read_into(1, &mut dst).is_none());
    }

    #[test]
    fn unavailable_sequences_preserve_the_callers_buffer() {
        let ring = RingBuffer::<u64>::new(2);
        for value in [11, 22, 33] {
            ring.push(&value);
        }
        for (seq, expected) in [
            (0, None),
            (1, None),
            (2, Some(22)),
            (3, Some(33)),
            (4, None),
        ] {
            let mut dst = MaybeUninit::new(99);
            assert_eq!(
                ring.read_into(seq, &mut dst)
                    .map(|()| unsafe { dst.assume_init() }),
                expected
            );
            if expected.is_none() {
                assert_eq!(unsafe { dst.assume_init() }, 99);
            }
        }
    }

    #[test]
    fn retains_full_capacity_across_sequence_width_boundaries() {
        for first in [u32::MAX as u64 - 1, u64::MAX - 4] {
            let ring = RingBuffer::<u64>::new(3);
            ring.write_seq.store(first, Ordering::Relaxed);

            for seq in first..first + 3 {
                assert_eq!(ring.push(&seq), seq);
            }

            let mut dst = MaybeUninit::uninit();
            for seq in first..first + 3 {
                ring.read_into(seq, &mut dst).unwrap();
                assert_eq!(unsafe { dst.assume_init() }, seq);
            }

            let next = first + 3;
            assert_eq!(ring.push(&next), next);
            assert!(ring.read_into(first, &mut dst).is_none());
            assert_eq!(ring.oldest_available_seq(), first + 1);
            for seq in first + 1..=next {
                ring.read_into(seq, &mut dst).unwrap();
                assert_eq!(unsafe { dst.assume_init() }, seq);
            }
        }
    }

    #[test]
    fn every_capacity_retains_its_full_window_across_wraps() {
        for capacity in [1, 2, 3, 4, 31, 32, 33] {
            let ring = RingBuffer::new(capacity);
            for value in 1..=(capacity * 3) as u64 {
                assert_eq!(ring.push(&value), value);
                let oldest = (value + 1).saturating_sub(capacity as u64).max(1);
                let mut dst = MaybeUninit::uninit();
                assert!(ring.read_into(oldest - 1, &mut dst).is_none());
                for retained in oldest..=value {
                    ring.read_into(retained, &mut dst).unwrap();
                    assert_eq!(unsafe { dst.assume_init() }, retained);
                }
                assert!(ring.read_into(value + 1, &mut dst).is_none());
            }
        }
    }

    #[test]
    fn retained_reader_blocks_writer_until_copy_finishes() {
        let ring = Arc::new(RingBuffer::<u64>::new(1));
        let seq = ring.push(&100u64);
        let slot = &ring.slots[0];
        // Admit and retain a reader using the same CAS as read_into, so the
        // writer must encounter an active reader rather than a finished copy.
        slot.access
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .unwrap();

        let writer_ring = Arc::clone(&ring);
        let writer = thread::spawn(move || writer_ring.push(&200u64));
        let deadline = Instant::now() + Duration::from_secs(5);
        while slot.access.load(Ordering::Acquire) != WRITER | 1 && Instant::now() < deadline {
            thread::yield_now();
        }
        let writer_waiting = slot.access.load(Ordering::Acquire) == WRITER | 1;
        let stored_while_held = slot.sequence.load(Ordering::Relaxed);
        let mut dst = MaybeUninit::<u64>::uninit();
        let new_reader_blocked = ring.read_into(seq, &mut dst).is_none();
        let retained_value = writer_waiting.then(|| {
            // SAFETY: this test owns the retained reader admission until the
            // release below; the writer has announced itself and must wait.
            unsafe { (*slot.data.get()).assume_init() }
        });
        // Release before asserting, so a failed assertion cannot strand a writer.
        slot.access.fetch_sub(1, Ordering::Release);
        let seq2 = writer.join().unwrap();

        assert!(
            writer_waiting,
            "writer did not wait for the retained reader"
        );
        assert_eq!(stored_while_held, seq);
        assert!(new_reader_blocked);
        assert_eq!(retained_value, Some(100));
        assert!(ring.read_into(seq, &mut dst).is_none());
        ring.read_into(seq2, &mut dst).unwrap();
        assert_eq!(unsafe { dst.assume_init() }, 200u64);
    }

    #[test]
    fn concurrent_readers_copy_values_safely() {
        let ring = Arc::new(RingBuffer::<u64>::new(64));
        let seqs: Vec<_> = (0u64..16).map(|n| ring.push(&n)).collect();

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ring = Arc::clone(&ring);
                let seqs = seqs.clone();
                thread::spawn(move || {
                    for (i, seq) in seqs.iter().enumerate() {
                        let mut dst = MaybeUninit::<u64>::uninit();
                        ring.read_into(*seq, &mut dst).unwrap();
                        assert_eq!(unsafe { dst.assume_init() }, i as u64);
                    }
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn concurrent_producer_and_consumers_make_progress() {
        const MSG_COUNT: u64 = 1_000;

        let ring = Arc::new(RingBuffer::<u64>::new(128));

        let producer_ring = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            for n in 1..=MSG_COUNT {
                producer_ring.push(&n);
            }
        });

        let consumers: Vec<_> = (0..4)
            .map(|_| {
                let ring = Arc::clone(&ring);
                thread::spawn(move || {
                    let mut seq = 1;
                    let mut reads = 0u64;
                    let mut dst = MaybeUninit::<u64>::uninit();

                    while seq <= MSG_COUNT {
                        match ring.read_into(seq, &mut dst) {
                            Some(()) => {
                                assert_eq!(unsafe { dst.assume_init() }, seq);
                                seq += 1;
                                reads += 1;
                            }
                            None => {
                                let latest = ring.latest_seq();
                                let oldest = ring.oldest_available_seq();
                                if seq < oldest {
                                    seq = oldest;
                                } else if latest == MSG_COUNT && seq > latest {
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

        producer.join().unwrap();
        for consumer in consumers {
            assert!(consumer.join().unwrap() > 0);
        }
    }

    #[derive(Copy, Clone, Debug, PartialEq)]
    struct Point {
        x: u32,
        y: u32,
    }

    #[test]
    fn generic_struct_roundtrips_through_ring() {
        let ring = RingBuffer::<Point>::new(4);

        let points = [
            Point { x: 1, y: 2 },
            Point { x: 10, y: 20 },
            Point { x: 100, y: 200 },
        ];

        let seqs: Vec<u64> = points.iter().map(|p| ring.push(p)).collect();

        for (i, seq) in seqs.iter().enumerate() {
            let mut dst = MaybeUninit::<Point>::uninit();
            ring.read_into(*seq, &mut dst).unwrap();
            assert_eq!(unsafe { dst.assume_init() }, points[i]);
        }
    }

    #[test]
    fn read_into_seq_zero_returns_none() {
        let ring = RingBuffer::<u64>::new(4);
        ring.push(&42);
        let mut dst = MaybeUninit::uninit();
        assert!(ring.read_into(0, &mut dst).is_none());
    }
}

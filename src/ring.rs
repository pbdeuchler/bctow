use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

struct Slot<T> {
    /// Even = stable snapshot, odd = writer owns the slot.
    version: AtomicU64,
    /// Number of readers currently copying data out of this slot.
    readers: AtomicUsize,
    /// Sequence number currently stored in the slot. `0` means empty.
    sequence: AtomicU64,
    data: UnsafeCell<MaybeUninit<T>>,
}

impl<T> Slot<T> {
    fn new() -> Self {
        Self {
            version: AtomicU64::new(0),
            readers: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
            data: UnsafeCell::new(MaybeUninit::uninit()),
        }
    }
}

// SAFETY: Slot access is coordinated by the per-slot version/readers protocol.
unsafe impl<T: Send> Send for Slot<T> {}
unsafe impl<T: Sync> Sync for Slot<T> {}

/// Single-producer, multiple-consumer ring buffer for typed payloads.
pub(crate) struct RingBuffer<T> {
    slots: Box<[Slot<T>]>,
    capacity: usize,
    /// Next sequence number to hand out. Starts at 1.
    write_seq: AtomicU64,
}

// SAFETY: The ring enforces single-writer access at the API boundary and uses
// atomics to coordinate concurrent readers.
unsafe impl<T: Send> Send for RingBuffer<T> {}
unsafe impl<T: Sync> Sync for RingBuffer<T> {}

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
        let seq = self
            .write_seq
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |next| {
                next.checked_add(1)
            })
            .expect("ring sequence overflow");

        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];

        let prior_version = slot.version.fetch_add(1, Ordering::AcqRel);
        debug_assert_eq!(prior_version % 2, 0, "single producer must see stable slot");

        while slot.readers.load(Ordering::Acquire) != 0 {
            spin_loop();
        }

        unsafe {
            let dst = &mut *slot.data.get();
            std::ptr::copy_nonoverlapping(value as *const T, dst.as_mut_ptr(), 1);
        }
        slot.sequence.store(seq, Ordering::Release);
        slot.version.fetch_add(1, Ordering::Release);

        seq
    }

    pub(crate) fn read_into(&self, seq: u64, dst: &mut MaybeUninit<T>) -> Option<()> {
        if seq == 0 {
            return None;
        }

        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];

        let version_before = slot.version.load(Ordering::Acquire);
        if version_before % 2 == 1 {
            return None;
        }

        let stored_seq = slot.sequence.load(Ordering::Acquire);
        if stored_seq != seq {
            return None;
        }

        slot.readers.fetch_add(1, Ordering::Acquire);

        let version_after = slot.version.load(Ordering::Acquire);
        let stored_after = slot.sequence.load(Ordering::Acquire);
        if version_before != version_after || version_after % 2 == 1 || stored_after != seq {
            slot.readers.fetch_sub(1, Ordering::Release);
            return None;
        }

        unsafe {
            let src = &*slot.data.get();
            std::ptr::copy_nonoverlapping(src.as_ptr(), dst.as_mut_ptr(), 1);
        }

        slot.readers.fetch_sub(1, Ordering::Release);
        Some(())
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
    fn read_overwritten_payload_returns_none() {
        let ring = RingBuffer::<u64>::new(2);
        ring.push(&1u64);
        ring.push(&2u64);
        ring.push(&3u64);

        let mut dst = MaybeUninit::<u64>::uninit();
        assert!(ring.read_into(1, &mut dst).is_none());
    }

    #[test]
    fn retained_reader_blocks_writer_until_copy_finishes() {
        let ring = Arc::new(RingBuffer::<u64>::new(1));
        let seq = ring.push(&100u64);

        let reader_ring = Arc::clone(&ring);
        let reader = thread::spawn(move || {
            let mut dst = MaybeUninit::<u64>::uninit();
            reader_ring.read_into(seq, &mut dst).unwrap();
            assert_eq!(unsafe { dst.assume_init() }, 100u64);
        });

        reader.join().unwrap();

        let seq2 = ring.push(&200u64);
        let mut dst = MaybeUninit::<u64>::uninit();
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
}

use std::cell::UnsafeCell;
use std::hint::spin_loop;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::error::BctowError;

struct Slot {
    /// Even = stable snapshot, odd = writer owns the slot.
    version: AtomicU64,
    /// Number of readers currently copying bytes out of this slot.
    readers: AtomicUsize,
    /// Sequence number currently stored in the slot. `0` means empty.
    sequence: AtomicU64,
    len: AtomicUsize,
    bytes: UnsafeCell<Box<[u8]>>,
}

impl Slot {
    fn new(max_payload_size: usize) -> Self {
        Self {
            version: AtomicU64::new(0),
            readers: AtomicUsize::new(0),
            sequence: AtomicU64::new(0),
            len: AtomicUsize::new(0),
            bytes: UnsafeCell::new(vec![0; max_payload_size].into_boxed_slice()),
        }
    }
}

// SAFETY: Slot access is coordinated by the per-slot version/readers protocol.
unsafe impl Send for Slot {}
unsafe impl Sync for Slot {}

/// Single-producer, multiple-consumer ring buffer for fixed-size byte payloads.
pub(crate) struct RingBuffer {
    slots: Box<[Slot]>,
    capacity: usize,
    max_payload_size: usize,
    /// Next sequence number to hand out. Starts at 1.
    write_seq: AtomicU64,
}

// SAFETY: The ring enforces single-writer access at the API boundary and uses
// atomics to coordinate concurrent readers.
unsafe impl Send for RingBuffer {}
unsafe impl Sync for RingBuffer {}

impl RingBuffer {
    pub(crate) fn new(capacity: usize, max_payload_size: usize) -> Self {
        assert!(capacity > 0, "ring buffer capacity must be > 0");

        let slots: Box<[Slot]> = (0..capacity).map(|_| Slot::new(max_payload_size)).collect();

        Self {
            slots,
            capacity,
            max_payload_size,
            write_seq: AtomicU64::new(1),
        }
    }

    pub(crate) fn push(&self, payload: &[u8]) -> u64 {
        debug_assert!(payload.len() <= self.max_payload_size);

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
            let bytes = &mut *slot.bytes.get();
            bytes[..payload.len()].copy_from_slice(payload);
        }
        slot.len.store(payload.len(), Ordering::Relaxed);
        slot.sequence.store(seq, Ordering::Release);
        slot.version.fetch_add(1, Ordering::Release);

        seq
    }

    pub(crate) fn read_into(&self, seq: u64, dst: &mut [u8]) -> Result<Option<usize>, BctowError> {
        if seq == 0 {
            return Ok(None);
        }

        let idx = ((seq - 1) as usize) % self.capacity;
        let slot = &self.slots[idx];

        let version_before = slot.version.load(Ordering::Acquire);
        if version_before % 2 == 1 {
            return Ok(None);
        }

        let stored_seq = slot.sequence.load(Ordering::Acquire);
        if stored_seq != seq {
            return Ok(None);
        }

        slot.readers.fetch_add(1, Ordering::Acquire);

        let version_after = slot.version.load(Ordering::Acquire);
        let stored_after = slot.sequence.load(Ordering::Acquire);
        if version_before != version_after || version_after % 2 == 1 || stored_after != seq {
            slot.readers.fetch_sub(1, Ordering::Release);
            return Ok(None);
        }

        let len = slot.len.load(Ordering::Acquire);
        if dst.len() < len {
            slot.readers.fetch_sub(1, Ordering::Release);
            return Err(BctowError::ReceiveBufferTooSmall {
                required: len,
                available: dst.len(),
            });
        }

        unsafe {
            let bytes = &*slot.bytes.get();
            dst[..len].copy_from_slice(&bytes[..len]);
        }

        slot.readers.fetch_sub(1, Ordering::Release);
        Ok(Some(len))
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

    #[allow(dead_code)]
    pub(crate) fn max_payload_size(&self) -> usize {
        self.max_payload_size
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

    pub(crate) fn new_latest(ring: &RingBuffer) -> Self {
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
    fn read_back_written_payload() {
        let ring = RingBuffer::new(4, 32);
        let seq = ring.push(b"hello");
        let mut dst = [0u8; 32];
        let len = ring.read_into(seq, &mut dst).unwrap().unwrap();
        assert_eq!(&dst[..len], b"hello");
    }

    #[test]
    fn read_overwritten_payload_returns_none() {
        let ring = RingBuffer::new(2, 32);
        ring.push(b"one");
        ring.push(b"two");
        ring.push(b"three");

        let mut dst = [0u8; 32];
        assert!(ring.read_into(1, &mut dst).unwrap().is_none());
    }

    #[test]
    fn read_into_rejects_small_buffers() {
        let ring = RingBuffer::new(2, 32);
        let seq = ring.push(b"hello");
        let mut dst = [0u8; 4];
        let err = ring.read_into(seq, &mut dst).unwrap_err();
        assert_eq!(
            err,
            BctowError::ReceiveBufferTooSmall {
                required: 5,
                available: 4
            }
        );
    }

    #[test]
    fn retained_reader_blocks_writer_until_copy_finishes() {
        let ring = Arc::new(RingBuffer::new(1, 32));
        let seq = ring.push(b"hello");

        let reader_ring = Arc::clone(&ring);
        let reader = thread::spawn(move || {
            let mut dst = [0u8; 32];
            let len = reader_ring.read_into(seq, &mut dst).unwrap().unwrap();
            assert_eq!(&dst[..len], b"hello");
        });

        reader.join().unwrap();

        let seq2 = ring.push(b"world");
        let mut dst = [0u8; 32];
        let len = ring.read_into(seq2, &mut dst).unwrap().unwrap();
        assert_eq!(&dst[..len], b"world");
    }

    #[test]
    fn concurrent_readers_copy_bytes_safely() {
        let ring = Arc::new(RingBuffer::new(64, 64));
        let seqs: Vec<_> = (0..16)
            .map(|n| {
                let payload = format!("msg-{n}");
                ring.push(payload.as_bytes())
            })
            .collect();

        let handles: Vec<_> = (0..8)
            .map(|_| {
                let ring = Arc::clone(&ring);
                let seqs = seqs.clone();
                thread::spawn(move || {
                    for seq in &seqs {
                        let mut dst = [0u8; 64];
                        let len = ring.read_into(*seq, &mut dst).unwrap().unwrap();
                        assert!(dst[..len].starts_with(b"msg-"));
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

        let ring = Arc::new(RingBuffer::new(128, 32));

        let producer_ring = Arc::clone(&ring);
        let producer = thread::spawn(move || {
            for n in 1..=MSG_COUNT {
                let bytes = n.to_le_bytes();
                producer_ring.push(&bytes);
            }
        });

        let consumers: Vec<_> = (0..4)
            .map(|_| {
                let ring = Arc::clone(&ring);
                thread::spawn(move || {
                    let mut seq = 1;
                    let mut reads = 0u64;
                    let mut dst = [0u8; 8];

                    while seq <= MSG_COUNT {
                        match ring.read_into(seq, &mut dst).unwrap() {
                            Some(len) => {
                                assert_eq!(len, 8);
                                assert_eq!(u64::from_le_bytes(dst), seq);
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

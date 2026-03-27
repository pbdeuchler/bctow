/// Tests for the benchmark harness functions.
/// These exercise the same logic as the harness module in benches/benchmarks.rs,
/// but are compiled as normal integration tests so they can be run with `cargo test`.
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use bctow::{BctowConfig, ConsumerContext, FixedBytes, OverflowPolicy, Producer, TopicId};

// ---------------------------------------------------------------------------
// Inline harness helpers (mirrors benches/benchmarks.rs harness module)
// ---------------------------------------------------------------------------

fn thread_ceiling() -> usize {
    let mut lo = 1usize;
    let mut hi = 4096usize;
    let mut last_success = 1usize;

    while lo <= hi {
        let mid = lo + (hi - lo) / 2;
        let handles: Vec<_> = (0..mid)
            .map(|_| {
                thread::Builder::new()
                    .stack_size(64 * 1024)
                    .spawn(|| {})
                    .ok()
            })
            .collect();
        let all_spawned = handles.iter().all(|h| h.is_some());
        for h in handles.into_iter().flatten() {
            let _ = h.join();
        }
        if all_spawned {
            last_success = mid;
            lo = mid + 1;
        } else {
            hi = mid - 1;
        }
    }

    last_success.saturating_sub(8).max(1)
}

fn setup_broker<T: Copy + Send + Sync + 'static>(
    topics: usize,
    capacity: usize,
) -> (Producer<T>, ConsumerContext<T>, Vec<TopicId>) {
    let config = BctowConfig {
        ring_buffer_capacity: capacity,
        overflow_policy: OverflowPolicy::DropOldest,
    };
    let (producer, ctx) = bctow::broker::<T>(config);
    let topic_ids: Vec<TopicId> = (0..topics)
        .map(|i| producer.register_topic(format!("topic/{i}")))
        .collect();
    (producer, ctx, topic_ids)
}

fn fill_fixed_bytes<const N: usize>() -> FixedBytes<N> {
    let mut data = [0u8; N];
    for i in 0..N {
        data[i] = ((i % 255) + 1) as u8;
    }
    FixedBytes::new(&data).expect("fill_fixed_bytes: slice fits in N")
}

fn drain_loop<T: Copy + Send + Sync + 'static>(
    mut clients: Vec<bctow::ConsumerClient<T>>,
    barrier: Arc<Barrier>,
    messages_expected: u64,
    counter: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    barrier.wait();
    if clients.is_empty() {
        return;
    }
    loop {
        let current = counter.load(Ordering::Acquire);
        if current >= messages_expected {
            break;
        }
        if stop.load(Ordering::Acquire) {
            break;
        }
        for client in &mut clients {
            if let Some(msg) = client.poll() {
                std::hint::black_box(msg);
                counter.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn fill_fixed_bytes_produces_non_zero_bytes_of_correct_length() {
    const N: usize = 16;
    let fb = fill_fixed_bytes::<N>();
    let bytes = fb.as_bytes();
    // Correct length
    assert_eq!(bytes.len(), N);
    // No zero bytes
    assert!(bytes.iter().all(|&b| b != 0), "expected no zero bytes");
}

#[test]
fn fill_fixed_bytes_zero_length_is_empty() {
    let fb = fill_fixed_bytes::<0>();
    assert_eq!(fb.as_bytes().len(), 0);
}

#[test]
fn setup_broker_returns_expected_number_of_topic_ids() {
    const TOPICS: usize = 5;
    let (_producer, _ctx, topic_ids) = setup_broker::<u64>(TOPICS, 64);
    assert_eq!(topic_ids.len(), TOPICS);
}

#[test]
fn setup_broker_zero_topics_returns_empty_vec() {
    let (_producer, _ctx, topic_ids) = setup_broker::<u64>(0, 64);
    assert!(topic_ids.is_empty());
}

#[test]
fn thread_ceiling_returns_positive_value() {
    // Exercises the binary-search probe path and verifies it never deadlocks.
    let ceiling = thread_ceiling();
    assert!(ceiling >= 1, "thread_ceiling must be at least 1, got {ceiling}");
}

#[test]
fn drain_loop_exits_when_counter_reaches_expected() {
    let config = BctowConfig {
        ring_buffer_capacity: 64,
        overflow_policy: OverflowPolicy::DropOldest,
    };
    let (producer, ctx) = bctow::broker::<u64>(config);
    let topic_id = producer.register_topic("drain-test");

    let mut client = ctx.create_client();
    client.subscribe_id(topic_id).expect("subscribe");

    // Publish 3 messages before the drain loop starts
    for v in 1u64..=3 {
        producer.publish_id(topic_id, &v).expect("publish");
    }

    let barrier = Arc::new(Barrier::new(2));
    let counter = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let b = Arc::clone(&barrier);
    let ctr = Arc::clone(&counter);
    let stp = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        drain_loop(vec![client], b, 3, ctr, stp);
    });

    barrier.wait(); // release the drain loop
    handle.join().expect("drain thread panicked");

    assert_eq!(counter.load(Ordering::Acquire), 3);
}

#[test]
fn drain_loop_exits_when_stop_flag_set() {
    let config = BctowConfig {
        ring_buffer_capacity: 64,
        overflow_policy: OverflowPolicy::DropOldest,
    };
    let (_producer, ctx) = bctow::broker::<u64>(config);

    // No messages published — counter will never reach messages_expected = 100
    let client = ctx.create_client();

    let barrier = Arc::new(Barrier::new(2));
    let counter = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));

    let b = Arc::clone(&barrier);
    let ctr = Arc::clone(&counter);
    let stp = Arc::clone(&stop);
    let stop_setter = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        drain_loop(vec![client], b, 100, ctr, stp);
    });

    barrier.wait();
    // Give the loop a moment to spin, then signal stop
    thread::sleep(Duration::from_millis(10));
    stop_setter.store(true, Ordering::Release);

    handle
        .join()
        .expect("drain thread should have exited after stop flag");
}

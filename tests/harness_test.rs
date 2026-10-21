/// Exercises the actual benchmark harness as normal integration tests.
#[allow(dead_code)]
#[path = "../benches/support/harness.rs"]
mod harness;

use bctow::{BctowConfig, OverflowPolicy};
use harness::*;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

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
    assert!(
        ceiling >= 1,
        "thread_ceiling must be at least 1, got {ceiling}"
    );
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
    let stop_watchdog = Arc::clone(&stop);

    let handle = thread::spawn(move || {
        drain_loop(vec![client], b, 3, ctr, stp);
    });

    barrier.wait(); // release the drain loop

    // Watchdog: set stop flag after 5s so test fails fast instead of hanging.
    let watchdog = thread::spawn(move || {
        thread::sleep(Duration::from_secs(5));
        stop_watchdog.store(true, Ordering::Release);
    });

    handle.join().expect("drain thread panicked");
    let _ = watchdog.join();

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

#[test]
fn spawn_consumers_distributes_clients_and_drains_messages() {
    let (producer, ctx, topic_ids) = setup_broker::<u64>(1, 1024);
    let topic_id = topic_ids[0];

    // 4 consumers across 2 threads, 10 messages each
    let handle = spawn_consumers(&ctx, &topic_ids, 4, 2, 10);

    // Publish 10 messages; each of the 4 consumers sees all 10 → 40 total.
    for i in 0u64..10 {
        producer.publish_id(topic_id, &i).unwrap();
    }

    handle.release();
    let completed = handle.wait(Duration::from_secs(10));
    assert!(
        completed,
        "spawn_consumers should drain 40 messages within timeout"
    );
}

#[test]
fn spawn_consumers_handles_more_consumers_than_threads() {
    let (producer, ctx, topic_ids) = setup_broker::<u64>(2, 1024);

    // 6 consumers across 3 threads, 5 messages per consumer per topic (10 total per consumer)
    let handle = spawn_consumers(&ctx, &topic_ids, 6, 3, 10);

    // Publish 5 messages to each of 2 topics
    for &id in &topic_ids {
        for i in 0u64..5 {
            producer.publish_id(id, &i).unwrap();
        }
    }

    handle.release();
    let completed = handle.wait(Duration::from_secs(10));
    assert!(completed, "multiplexed consumers should drain all messages");
}

#[test]
fn terminal_consumers_finish_after_ring_wraps_and_drops() {
    let (producer, ctx, topic_ids) = setup_broker::<bctow::FixedBytes<512>>(1, CONTENTION_CAPACITY);
    let handle = spawn_consumers_until(&ctx, &topic_ids, 5, 2, |message| message.is_empty());
    let value = fill_fixed_bytes::<512>();
    assert!(CONTENTION_MESSAGES > CONTENTION_CAPACITY as u64);
    // Hold consumers until after overflow to guarantee that delivery counts
    // cannot reach the number published. The terminal message remains retained.
    for _ in 0..CONTENTION_MESSAGES {
        producer.publish_id(topic_ids[0], &value).unwrap();
    }
    producer
        .publish_id(topic_ids[0], &bctow::FixedBytes::new(&[]).unwrap())
        .unwrap();
    handle.release();
    assert!(handle.wait(Duration::from_secs(5)));
}

#[test]
fn terminal_consumers_timeout_when_terminal_is_missing() {
    let (producer, ctx, topic_ids) = setup_broker::<u64>(1, 2);
    let handle = spawn_consumers_until(&ctx, &topic_ids, 3, 2, |message| *message == 99);
    producer.publish_id(topic_ids[0], &1).unwrap();
    handle.release();
    assert!(!handle.wait(Duration::ZERO));
}

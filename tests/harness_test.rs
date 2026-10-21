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
    let (completed_tx, completed_rx) = std::sync::mpsc::channel();

    let handle = thread::spawn(move || {
        drain_loop(vec![client], b, 3, ctr, stp);
        completed_tx.send(()).unwrap();
    });

    barrier.wait(); // release the drain loop

    let completed = completed_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    stop.store(true, Ordering::Release);
    handle.join().expect("drain thread panicked");
    assert!(completed, "drain loop did not reach the delivery target");
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

    stop_setter.store(true, Ordering::Release);
    barrier.wait();

    handle
        .join()
        .expect("drain thread should have exited after stop flag");
}

#[test]
fn drain_loop_with_no_clients_returns_without_deliveries() {
    let counter = Arc::new(AtomicU64::new(0));
    drain_loop::<u64>(
        Vec::new(),
        Arc::new(Barrier::new(1)),
        1,
        Arc::clone(&counter),
        Arc::new(AtomicBool::new(false)),
    );
    assert_eq!(counter.load(Ordering::Acquire), 0);
}

#[test]
fn spawn_consumers_distributes_clients_and_drains_messages() {
    let (producer, ctx, topic_ids) = setup_broker::<u64>(1, 1024);
    let topic_id = topic_ids[0];

    // 4 consumers across 2 threads, 10 messages each
    let handle = spawn_consumers(&ctx, &topic_ids, 4, 2, 10).unwrap();

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
    let handle = spawn_consumers(&ctx, &topic_ids, 6, 3, 10).unwrap();

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
    let handle =
        spawn_consumers_until(&ctx, &topic_ids, 5, 2, |message| message.is_empty()).unwrap();
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
    let handle = spawn_consumers_until(&ctx, &topic_ids, 3, 2, |message| *message == 99).unwrap();
    producer.publish_id(topic_ids[0], &1).unwrap();
    handle.release();
    assert!(!handle.wait(Duration::ZERO));
}

#[test]
fn worker_spawning_distributes_all_clients_across_live_workers() {
    for (count, requested, available, expected) in [
        (5, 3, 3, 3),
        (5, 3, 2, 2),
        (5, 3, 1, 1),
        (3, 5, 5, 3),
        (5, 3, 0, 0),
    ] {
        let (producer, ctx, topic_ids) = setup_broker::<u64>(1, 32);
        let exited = Arc::new(AtomicU64::new(0));
        let mut spawned = 0;
        let result = spawn_workers_with(
            &ctx,
            &topic_ids,
            count,
            requested,
            DrainTarget::Messages(10),
            |task| {
                if spawned == available {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "thread limit",
                    ));
                }
                spawned += 1;
                let exited = Arc::clone(&exited);
                thread::Builder::new().spawn(move || {
                    task();
                    exited.fetch_add(1, Ordering::Release);
                })
            },
        );
        if expected == 0 {
            assert_eq!(result.err().unwrap().kind(), std::io::ErrorKind::WouldBlock);
            assert_eq!(spawned, 0);
            continue;
        }
        let handle = result.unwrap();
        assert_eq!(handle.thread_count(), expected);
        for value in 0..10 {
            producer.publish_id(topic_ids[0], &value).unwrap();
        }
        handle.release();
        // Since the ring never overflowed, completion requires all messages to
        // reach every client, including clients redistributed after spawn failure.
        assert!(handle.wait(Duration::from_secs(5)));
        assert_eq!(exited.load(Ordering::Acquire), expected as u64);
    }
}

#[test]
fn worker_spawning_rejects_zero_counts_without_spawning() {
    let (_producer, ctx, topic_ids) = setup_broker::<u64>(1, 2);
    for (count, threads) in [(0, 1), (1, 0), (0, 0)] {
        let result = spawn_workers_with(
            &ctx,
            &topic_ids,
            count,
            threads,
            DrainTarget::Messages(1),
            |_| panic!("invalid counts must not spawn a thread"),
        );
        assert_eq!(
            result.err().unwrap().kind(),
            std::io::ErrorKind::InvalidInput
        );
    }
}

#[test]
fn timeout_joins_all_workers_before_returning() {
    let (_producer, ctx, topic_ids) = setup_broker::<u64>(1, 2);
    let exited = Arc::new(AtomicU64::new(0));
    let handle = spawn_workers_with(
        &ctx,
        &topic_ids,
        5,
        2,
        DrainTarget::Terminal(|message| *message == 99),
        |task| {
            let exited = Arc::clone(&exited);
            thread::Builder::new().spawn(move || {
                task();
                exited.fetch_add(1, Ordering::Release);
            })
        },
    )
    .unwrap();
    handle.release();
    assert!(!handle.wait(Duration::ZERO));
    assert_eq!(exited.load(Ordering::Acquire), 2);
}

#[test]
fn consumers_with_no_expected_messages_complete_without_publishing() {
    let (_producer, ctx, topic_ids) = setup_broker::<u64>(1, 2);
    let handle = spawn_consumers(&ctx, &topic_ids, 3, 2, 0).unwrap();
    handle.release();
    assert!(handle.wait(Duration::from_secs(5)));
}

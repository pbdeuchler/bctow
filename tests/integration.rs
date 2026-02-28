/// Integration tests for the bctow Producer/Consumer public API.
///
/// Each test is self-contained and creates its own broker pair.
/// Tests cover: single producer/consumer, multi-consumer, dynamic
/// subscribe/unsubscribe, overflow policies, error cases, and
/// multi-threaded operation.
///
/// NOTE: subscribe() starts the cursor at latest+1 — historical messages
/// (published before subscribe) are NOT delivered. Tests therefore publish
/// a "prime" message to create the ring, subscribe, then publish test messages.
use std::sync::Arc;
use std::thread;

use bctow::{broker, BctowConfig, BctowError, ConsumerContext, OverflowPolicy, Producer, TopicId};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn default_broker<T: Send + Sync + 'static>() -> (Producer<T>, ConsumerContext<T>) {
    broker(BctowConfig::default())
}

fn small_broker<T: Send + Sync + 'static>(
    capacity: usize,
    policy: OverflowPolicy,
) -> (Producer<T>, ConsumerContext<T>) {
    broker(BctowConfig {
        ring_buffer_capacity: capacity,
        overflow_policy: policy,
        ..BctowConfig::default()
    })
}

/// Drain all currently available messages from a client into a Vec.
fn drain<T: Send + Sync + 'static>(
    client: &mut bctow::ConsumerClient<T>,
) -> Vec<(TopicId, Arc<T>)> {
    let mut out = Vec::new();
    while let Some(item) = client.poll() {
        out.push(item);
    }
    out
}

// ---------------------------------------------------------------------------
// 1. Single producer / single consumer
// ---------------------------------------------------------------------------

/// Publish several messages, subscribe, then publish more — only post-subscribe
/// messages are visible. Verifies correct order via the string publish path.
#[test]
fn single_producer_single_consumer_order_preserved_string_path() {
    let (prod, ctx) = default_broker::<u64>();

    // Prime: creates the ring. subscribe() requires the ring to exist.
    prod.publish("ordered", 0);

    let mut client = ctx.create_client();
    client.subscribe("ordered").unwrap();

    for i in 1u64..=5 {
        prod.publish("ordered", i);
    }

    let msgs = drain(&mut client);
    let values: Vec<u64> = msgs.iter().map(|(_, v)| **v).collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5], "messages must arrive in publish order");
}

/// Verify the publish_id (TopicId) hot path. TopicIds are obtained from poll() return values.
#[test]
fn single_producer_single_consumer_publish_id_path() {
    let (prod, ctx) = default_broker::<u64>();

    // Prime the topic with a known sentinel value.
    let seq1 = prod.publish("events", 999);
    assert_eq!(seq1, 1, "first publish must return sequence 1");

    let mut client = ctx.create_client();
    client.subscribe("events").unwrap();

    // Publish via string; collect returned sequences.
    let s2 = prod.publish("events", 10);
    let s3 = prod.publish("events", 20);
    assert_eq!(s2, 2);
    assert_eq!(s3, 3);

    let msgs = drain(&mut client);
    let values: Vec<u64> = msgs.iter().map(|(_, v)| **v).collect();
    assert_eq!(values, vec![10, 20]);

    // Now use the TopicId obtained from poll() to publish via publish_id.
    let topic_id = msgs[0].0;
    let s4 = prod.publish_id(topic_id, 30);
    assert_eq!(s4, 4);
    prod.publish_id(topic_id, 40);

    let more = drain(&mut client);
    let more_values: Vec<u64> = more.iter().map(|(_, v)| **v).collect();
    assert_eq!(more_values, vec![30, 40]);
}

/// Publish returns monotonically increasing sequence numbers per topic.
#[test]
fn publish_returns_increasing_sequence_numbers() {
    let (prod, _ctx) = default_broker::<u64>();
    let seqs: Vec<u64> = (1u64..=5)
        .map(|i| prod.publish("seq-test", i))
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

// ---------------------------------------------------------------------------
// 2. Single producer / many consumers
// ---------------------------------------------------------------------------

/// Multiple clients from the same ConsumerContext each independently receive
/// all messages published after they subscribed. Run concurrently in threads.
#[test]
fn single_producer_many_consumers_all_receive_independently() {
    const NUM_CONSUMERS: usize = 4;
    const MSG_COUNT: u64 = 20;

    let (prod, ctx) = default_broker::<u64>();

    // Prime so subscribe works.
    prod.publish("broadcast", 0);

    // Create all clients and subscribe before publishing the test messages.
    let clients: Vec<_> = (0..NUM_CONSUMERS)
        .map(|_| {
            let mut c = ctx.create_client();
            c.subscribe("broadcast").unwrap();
            c
        })
        .collect();

    // Publish test payload.
    for i in 1..=MSG_COUNT {
        prod.publish("broadcast", i);
    }

    // Each client drains in its own thread.
    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            thread::spawn(move || {
                drain(&mut client)
                    .into_iter()
                    .map(|(_, v)| *v)
                    .collect::<Vec<u64>>()
            })
        })
        .collect();

    for handle in handles {
        let values = handle.join().expect("consumer thread panicked");
        assert_eq!(
            values,
            (1..=MSG_COUNT).collect::<Vec<u64>>(),
            "each consumer must independently receive all messages in order"
        );
    }
}

/// ConsumerContext is Clone; clients from a cloned context share the same broker.
#[test]
fn cloned_consumer_context_creates_independent_clients() {
    let (prod, ctx) = default_broker::<u64>();

    prod.publish("clone-test", 0);

    let ctx2 = ctx.clone();

    let mut c1 = ctx.create_client();
    let mut c2 = ctx2.create_client();

    c1.subscribe("clone-test").unwrap();
    c2.subscribe("clone-test").unwrap();

    prod.publish("clone-test", 1);
    prod.publish("clone-test", 2);

    let v1: Vec<u64> = drain(&mut c1).into_iter().map(|(_, v)| *v).collect();
    let v2: Vec<u64> = drain(&mut c2).into_iter().map(|(_, v)| *v).collect();

    assert_eq!(v1, vec![1, 2]);
    assert_eq!(v2, vec![1, 2]);
}

// ---------------------------------------------------------------------------
// 3. Dynamic subscribe / unsubscribe while publishing
// ---------------------------------------------------------------------------

/// Phase 1: subscribed to A only → receives A messages.
/// Phase 2: subscribes to B → receives both.
/// Phase 3: unsubscribes from A → receives only B.
#[test]
fn dynamic_subscribe_unsubscribe() {
    let (prod, ctx) = default_broker::<&'static str>();

    // Prime both topics.
    prod.publish("a", "a-prime");
    prod.publish("b", "b-prime");

    let mut client = ctx.create_client();
    client.subscribe("a").unwrap();

    // Phase 1: subscribed to A only.
    prod.publish("a", "a1");
    prod.publish("b", "b1");
    prod.publish("a", "a2");

    let phase1 = drain(&mut client);
    let values1: Vec<&str> = phase1.iter().map(|(_, v)| **v).collect();
    assert_eq!(values1, vec!["a1", "a2"], "phase 1: only A messages expected");

    // Capture id_a from a received message so we can use unsubscribe() later.
    let id_a: TopicId = phase1[0].0;

    // Phase 2: subscribe to B as well.
    client.subscribe("b").unwrap();
    prod.publish("a", "a3");
    prod.publish("b", "b2");

    let phase2 = drain(&mut client);
    let ids2: Vec<TopicId> = phase2.iter().map(|(id, _)| *id).collect();
    let has_a = ids2.iter().any(|&id| id == id_a);
    let has_b = ids2.iter().any(|&id| id != id_a);
    assert!(has_a && has_b, "phase 2: messages from both A and B expected");

    // Capture id_b.
    let id_b: TopicId = phase2.iter().find(|(id, _)| *id != id_a).unwrap().0;

    // Phase 3: unsubscribe from A.
    client.unsubscribe(id_a);
    prod.publish("a", "a4");
    prod.publish("b", "b3");

    let phase3 = drain(&mut client);
    let ids3: Vec<TopicId> = phase3.iter().map(|(id, _)| *id).collect();
    let values3: Vec<&str> = phase3.iter().map(|(_, v)| **v).collect();
    assert!(
        ids3.iter().all(|&id| id == id_b),
        "phase 3: only B messages expected after unsubscribing A"
    );
    assert_eq!(values3, vec!["b3"]);
}

/// unsubscribe_by_name removes a subscription the same as unsubscribe(TopicId).
#[test]
fn unsubscribe_by_name_removes_subscription() {
    let (prod, ctx) = default_broker::<u64>();

    prod.publish("named", 0);

    let mut client = ctx.create_client();
    client.subscribe("named").unwrap();

    prod.publish("named", 1);
    let msgs = drain(&mut client);
    assert_eq!(msgs.len(), 1);

    client.unsubscribe_by_name("named");

    prod.publish("named", 2);
    let after = drain(&mut client);
    assert!(after.is_empty(), "no messages expected after unsubscribe_by_name");
}

/// unsubscribe_by_name on an unknown topic is a no-op (no panic).
#[test]
fn unsubscribe_by_name_unknown_topic_is_noop() {
    let (_prod, ctx) = default_broker::<u64>();
    let mut client = ctx.create_client();
    // Must not panic.
    client.unsubscribe_by_name("never-published");
}

// ---------------------------------------------------------------------------
// 4. Lagging consumer overrun behaviour per overflow policy
// ---------------------------------------------------------------------------

/// DropOldest: consumer lags, on next poll skips to oldest available and returns it.
#[test]
fn overflow_drop_oldest_skips_to_oldest_available() {
    // capacity=4, overflow=DropOldest
    let (prod, ctx) = small_broker::<u64>(4, OverflowPolicy::DropOldest);

    // Prime the topic.
    prod.publish("lag", 0);

    let mut client = ctx.create_client();
    client.subscribe("lag").unwrap();

    // Flood: push 8 messages into a capacity-4 ring without polling.
    // After 8 pushes, oldest available = 8 - 4 + 1 = 5.
    for i in 1u64..=8 {
        prod.publish("lag", i);
    }

    // DropOldest must return the oldest still-available message (not None).
    let result = client.poll();
    assert!(
        result.is_some(),
        "DropOldest: lagging consumer must receive the oldest available message on next poll"
    );
    let (_, val) = result.unwrap();
    assert!(
        *val >= 5,
        "DropOldest: received {}, expected >= 5 (oldest available after 8 writes into cap-4 ring)",
        *val
    );
}

/// SkipToLatest: lagging consumer gets None on first poll; cursor jumps to latest+1;
/// subsequent new messages are received normally.
#[test]
fn overflow_skip_to_latest_returns_none_then_receives_new() {
    let (prod, ctx) = small_broker::<u64>(4, OverflowPolicy::SkipToLatest);

    prod.publish("skip", 0);

    let mut client = ctx.create_client();
    client.subscribe("skip").unwrap();

    // Flood 8 messages — consumer has lagged past the entire ring.
    for i in 1u64..=8 {
        prod.publish("skip", i);
    }

    // First poll after lag: returns None and advances cursor to latest+1.
    let overrun_result = client.poll();
    assert!(
        overrun_result.is_none(),
        "SkipToLatest: first poll after overrun must return None"
    );

    // Messages published after the cursor reset must be received.
    prod.publish("skip", 99);
    prod.publish("skip", 100);

    let after: Vec<u64> = drain(&mut client).into_iter().map(|(_, v)| *v).collect();
    assert_eq!(
        after,
        vec![99, 100],
        "SkipToLatest: messages published after cursor reset must be received"
    );
}

/// DropNewest: consumer lags, cursor stays put, every poll returns None.
#[test]
fn overflow_drop_newest_cursor_stays_put() {
    let (prod, ctx) = small_broker::<u64>(4, OverflowPolicy::DropNewest);

    prod.publish("drop", 0);

    let mut client = ctx.create_client();
    client.subscribe("drop").unwrap();

    // Flood past capacity.
    for i in 1u64..=8 {
        prod.publish("drop", i);
    }

    // Every poll must return None — cursor doesn't move.
    for attempt in 0..5 {
        let result = client.poll();
        assert!(
            result.is_none(),
            "DropNewest attempt {attempt}: poll must return None when consumer has lagged beyond ring capacity"
        );
    }
}

// ---------------------------------------------------------------------------
// 5. Error cases
// ---------------------------------------------------------------------------

/// subscribe() on a topic that has never been published to → TopicNotFound.
#[test]
fn subscribe_nonexistent_topic_by_name_returns_topic_not_found() {
    let (_prod, ctx) = default_broker::<u64>();
    let mut client = ctx.create_client();
    let err = client.subscribe("does-not-exist").unwrap_err();
    assert!(
        matches!(err, BctowError::TopicNotFound(_)),
        "expected TopicNotFound, got: {:?}",
        err
    );
}

/// subscribe_id() with a TopicId for a ring that does not exist → TopicNotFound.
/// We cannot construct a valid TopicId for a non-existent topic from outside the crate,
/// but we can obtain one from a different broker instance (the ring won't be found in
/// this broker's ring map).
#[test]
fn subscribe_id_for_unknown_ring_returns_topic_not_found() {
    // Broker A: create a topic and publish a message so subscribe + poll works.
    let (prod_a, ctx_a) = default_broker::<u64>();
    prod_a.publish("foreign", 0);  // prime
    let mut tmp = ctx_a.create_client();
    tmp.subscribe("foreign").unwrap();
    prod_a.publish("foreign", 1);
    // Obtain a TopicId from a received message.
    let foreign_id = tmp.poll().unwrap().0;

    // Broker B: this broker has no topics — foreign_id is unknown here.
    let (_prod_b, ctx_b) = default_broker::<u64>();
    let mut client = ctx_b.create_client();
    let err = client.subscribe_id(foreign_id).unwrap_err();
    assert!(
        matches!(err, BctowError::TopicNotFound(_)),
        "expected TopicNotFound, got: {:?}",
        err
    );
}

/// poll() with no subscriptions returns None (not a panic).
#[test]
fn poll_with_no_subscriptions_returns_none() {
    let (_prod, ctx) = default_broker::<u64>();
    let mut client = ctx.create_client();
    assert!(
        client.poll().is_none(),
        "poll with no subscriptions must return None"
    );
}

/// TopicNotFound error carries the topic name string for string-based lookups.
#[test]
fn topic_not_found_error_carries_topic_name() {
    let (_prod, ctx) = default_broker::<u64>();
    let mut client = ctx.create_client();
    let err = client.subscribe("my-missing-topic").unwrap_err();
    match err {
        BctowError::TopicNotFound(desc) => {
            assert!(
                desc.contains("my-missing-topic"),
                "error description should contain the topic name, got: {desc}"
            );
        }
        _ => panic!("expected TopicNotFound, got: {:?}", err),
    }
}

// ---------------------------------------------------------------------------
// 6. Subscribe idempotency
// ---------------------------------------------------------------------------

/// Subscribing to the same topic twice does not duplicate messages.
#[test]
fn subscribe_twice_does_not_duplicate_messages() {
    let (prod, ctx) = default_broker::<u64>();

    // Prime the topic.
    prod.publish("idem", 0);

    let mut client = ctx.create_client();

    // Subscribe to the same topic twice — second call must be a no-op.
    client.subscribe("idem").unwrap();
    client.subscribe("idem").unwrap();

    prod.publish("idem", 1);
    prod.publish("idem", 2);
    prod.publish("idem", 3);

    let msgs = drain(&mut client);
    let values: Vec<u64> = msgs.iter().map(|(_, v)| **v).collect();

    assert_eq!(
        values,
        vec![1, 2, 3],
        "subscribing twice must not cause duplicate message delivery"
    );
}

/// subscribe_id twice is also a no-op.
#[test]
fn subscribe_id_twice_does_not_duplicate_messages() {
    let (prod, ctx) = default_broker::<u64>();

    prod.publish("idem2", 0);

    let mut client = ctx.create_client();
    client.subscribe("idem2").unwrap();

    // Resolve the TopicId and subscribe by ID twice.
    prod.publish("idem2", 1);
    let (topic_id, _) = client.poll().unwrap();

    // Re-subscribe by ID — must be no-op.
    client.subscribe_id(topic_id).unwrap();

    prod.publish("idem2", 2);
    prod.publish("idem2", 3);

    let msgs = drain(&mut client);
    let values: Vec<u64> = msgs.iter().map(|(_, v)| **v).collect();
    assert_eq!(
        values,
        vec![2, 3],
        "subscribe_id twice must not cause duplicate delivery"
    );
}

// ---------------------------------------------------------------------------
// 7. Metrics (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "metrics")]
mod metrics_tests {
    use super::*;

    #[test]
    fn metrics_published_and_delivered_counts() {
        let (prod, ctx) = default_broker::<u64>();
        const N: u64 = 10;

        // Prime.
        prod.publish("m", 0);

        let mut client = ctx.create_client();
        client.subscribe("m").unwrap();

        for i in 1..=N {
            prod.publish("m", i);
        }

        let delivered_msgs = drain(&mut client);
        let delivered_count = delivered_msgs.len() as u64;

        let m = ctx.metrics();
        // Total published = 1 (prime) + N
        assert_eq!(m.messages_published, N + 1, "published count mismatch");
        assert_eq!(
            m.messages_delivered, delivered_count,
            "delivered count must match number of successful poll() returns"
        );
    }

    #[test]
    fn metrics_delivered_zero_when_no_polls() {
        let (prod, ctx) = default_broker::<u64>();
        prod.publish("m2", 42);

        let m = ctx.metrics();
        assert_eq!(m.messages_published, 1);
        assert_eq!(m.messages_delivered, 0, "no polls means zero delivered");
    }

    #[test]
    fn metrics_accumulate_across_multiple_topics() {
        let (prod, ctx) = default_broker::<u64>();

        prod.publish("t1", 0);
        prod.publish("t2", 0);

        let mut c1 = ctx.create_client();
        c1.subscribe("t1").unwrap();
        c1.subscribe("t2").unwrap();

        prod.publish("t1", 1);
        prod.publish("t2", 2);

        let n = drain(&mut c1).len() as u64;

        let m = ctx.metrics();
        assert_eq!(m.messages_published, 4); // 2 primes + 2 payloads
        assert_eq!(m.messages_delivered, n);
    }
}

// ---------------------------------------------------------------------------
// 8. Multi-threaded producer + consumers
// ---------------------------------------------------------------------------

/// Producer on one thread, 4 consumers on separate threads. Each consumer
/// subscribes to the same topic and must receive at least some messages.
/// Verifies no panics or data races.
#[test]
fn multithreaded_producer_and_consumers_no_panics() {
    const MSG_COUNT: u64 = 200;
    const NUM_CONSUMERS: usize = 4;
    // Large enough ring to avoid excessive lag in this test.
    const CAPACITY: usize = 512;

    let (prod, ctx) = broker::<u64>(BctowConfig {
        ring_buffer_capacity: CAPACITY,
        ..BctowConfig::default()
    });

    // Prime the topic before spawning anything.
    prod.publish("shared", 0);

    // Create and subscribe all consumers before starting the producer.
    let clients: Vec<_> = (0..NUM_CONSUMERS)
        .map(|_| {
            let mut c = ctx.create_client();
            c.subscribe("shared").unwrap();
            c
        })
        .collect();

    // Spawn the producer on its own thread.
    let producer_handle = thread::spawn(move || {
        for i in 1..=MSG_COUNT {
            prod.publish("shared", i);
        }
    });

    producer_handle.join().expect("producer thread panicked");

    // Drain each consumer; must receive at least 1 message in correct order.
    for (idx, mut client) in clients.into_iter().enumerate() {
        let msgs = drain(&mut client);
        assert!(
            !msgs.is_empty(),
            "consumer {idx}: received zero messages — expected at least 1"
        );
        let values: Vec<u64> = msgs.iter().map(|(_, v)| **v).collect();
        let is_sorted = values.windows(2).all(|w| w[0] < w[1]);
        assert!(
            is_sorted,
            "consumer {idx}: messages must arrive in monotonically increasing order"
        );
    }
}

/// Producer and consumers run truly concurrently (producer not joined before consumers drain).
#[test]
fn multithreaded_truly_concurrent_producer_and_consumers() {
    const MSG_COUNT: u64 = 500;
    const NUM_CONSUMERS: usize = 4;
    const CAPACITY: usize = 1024;

    let (prod, ctx) = broker::<u64>(BctowConfig {
        ring_buffer_capacity: CAPACITY,
        ..BctowConfig::default()
    });

    prod.publish("concurrent", 0);

    let clients: Vec<_> = (0..NUM_CONSUMERS)
        .map(|_| {
            let mut c = ctx.create_client();
            c.subscribe("concurrent").unwrap();
            c
        })
        .collect();

    // Consumer threads: each spins until it has seen MSG_COUNT messages or given up.
    let consumer_handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            thread::spawn(move || {
                let mut received: Vec<u64> = Vec::new();
                let mut attempts = 0usize;
                while received.len() < MSG_COUNT as usize && attempts < 100_000 {
                    if let Some((_, v)) = client.poll() {
                        received.push(*v);
                    } else {
                        attempts += 1;
                        thread::yield_now();
                    }
                }
                received
            })
        })
        .collect();

    // Producer thread.
    let producer_handle = thread::spawn(move || {
        for i in 1..=MSG_COUNT {
            prod.publish("concurrent", i);
        }
    });

    producer_handle.join().expect("producer thread panicked");

    for (idx, handle) in consumer_handles.into_iter().enumerate() {
        let received = handle.join().expect("consumer thread panicked");
        assert!(
            !received.is_empty(),
            "concurrent consumer {idx}: received zero messages"
        );
        // Verify ordering of what was received.
        let is_sorted = received.windows(2).all(|w| w[0] < w[1]);
        assert!(
            is_sorted,
            "concurrent consumer {idx}: received messages must be in order"
        );
    }
}

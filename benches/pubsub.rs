use bctow::{BctowConfig, ConsumerContext, FixedBytes, OverflowPolicy, Producer, TopicId};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};

// ---------------------------------------------------------------------------
// Fixture types
// ---------------------------------------------------------------------------

/// 128-byte struct representing a typical sensor reading or event.
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct MediumPayload {
    timestamp: u64,
    sensor_id: u64,
    sequence: u64,
    flags: u64,
    value_0: u64,
    value_1: u64,
    value_2: u64,
    value_3: u64,
    value_4: u64,
    value_5: u64,
    value_6: u64,
    value_7: u64,
    value_8: u64,
    value_9: u64,
    value_10: u64,
    value_11: u64,
}

// ---------------------------------------------------------------------------
// Fixture constructors
// ---------------------------------------------------------------------------

fn make_medium_payload() -> MediumPayload {
    MediumPayload {
        timestamp: 0,
        sensor_id: 1,
        sequence: 2,
        flags: 3,
        value_0: 4,
        value_1: 5,
        value_2: 6,
        value_3: 7,
        value_4: 8,
        value_5: 9,
        value_6: 10,
        value_7: 11,
        value_8: 12,
        value_9: 13,
        value_10: 14,
        value_11: 15,
    }
}

fn make_fixed_bytes<const N: usize>(fill: u8) -> FixedBytes<N> {
    let buf: Vec<u8> = vec![fill; N];
    FixedBytes::<N>::new(&buf).expect("fill fits in FixedBytes<N>")
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

fn setup_broker_with_topics<T: Copy + Send + Sync + 'static>(
    config: BctowConfig,
    topic_count: usize,
) -> (Producer<T>, ConsumerContext<T>, Vec<TopicId>) {
    let (producer, ctx) = bctow::broker::<T>(config);
    let topic_ids = (0..topic_count)
        .map(|i| producer.register_topic(format!("topic/{i}")))
        .collect();
    (producer, ctx, topic_ids)
}

fn spawn_consumers<T: Copy + Send + Sync + 'static>(
    ctx: &ConsumerContext<T>,
    topic_ids: &[TopicId],
    consumer_count: usize,
) -> Vec<bctow::ConsumerClient<T>> {
    (0..consumer_count)
        .map(|_| {
            let mut client = ctx.create_client();
            for &id in topic_ids {
                client.subscribe_id(id).expect("subscribe succeeds");
            }
            client
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Benchmarks — publish throughput by message size
// ---------------------------------------------------------------------------

fn bench_publish_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_throughput");

    // u64 — 8 bytes
    {
        let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
        let (producer, _ctx, topics) = setup_broker_with_topics::<u64>(config, 1);
        let tid = topics[0];
        let val = 42u64;
        group.bench_function("u64_8B", |b| {
            b.iter(|| black_box(producer.publish_id(tid, black_box(&val)).unwrap()));
        });
    }

    // MediumPayload — 128 bytes
    {
        let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
        let (producer, _ctx, topics) = setup_broker_with_topics::<MediumPayload>(config, 1);
        let tid = topics[0];
        let val = make_medium_payload();
        group.bench_function("medium_128B", |b| {
            b.iter(|| black_box(producer.publish_id(tid, black_box(&val)).unwrap()));
        });
    }

    // FixedBytes<256>
    {
        let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
        let (producer, _ctx, topics) = setup_broker_with_topics::<FixedBytes<256>>(config, 1);
        let tid = topics[0];
        let val = make_fixed_bytes::<256>(0xAB);
        group.bench_function("fixed_256B", |b| {
            b.iter(|| black_box(producer.publish_id(tid, black_box(&val)).unwrap()));
        });
    }

    // FixedBytes<4096>
    {
        let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
        let (producer, _ctx, topics) = setup_broker_with_topics::<FixedBytes<4096>>(config, 1);
        let tid = topics[0];
        let val = make_fixed_bytes::<4096>(0xCD);
        group.bench_function("fixed_4096B", |b| {
            b.iter(|| black_box(producer.publish_id(tid, black_box(&val)).unwrap()));
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmarks — consumer scaling (single topic, varying consumer count)
// ---------------------------------------------------------------------------

fn bench_consumer_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("consumer_scaling");

    for &n_consumers in &[1, 2, 4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_consumers),
            &n_consumers,
            |b, &n| {
                let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
                let (producer, ctx, topics) = setup_broker_with_topics::<u64>(config, 1);
                let tid = topics[0];
                let mut clients = spawn_consumers(&ctx, &topics, n);

                b.iter(|| {
                    black_box(producer.publish_id(tid, black_box(&99u64)).unwrap());
                    for client in clients.iter_mut() {
                        black_box(client.poll());
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmarks — topic scaling (varying topic count, round-robin publish+poll)
// ---------------------------------------------------------------------------

fn bench_topic_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("topic_scaling");

    for &n_topics in &[1, 4, 16, 64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(n_topics),
            &n_topics,
            |b, &n| {
                let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
                let (producer, ctx, topics) = setup_broker_with_topics::<u64>(config, n);
                let mut clients = spawn_consumers(&ctx, &topics, 1);
                let client = &mut clients[0];

                b.iter(|| {
                    for &tid in &topics {
                        black_box(producer.publish_id(tid, black_box(&77u64)).unwrap());
                    }
                    for _ in 0..n {
                        black_box(client.poll());
                    }
                });
            },
        );
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmarks — pathological cases
// ---------------------------------------------------------------------------

fn bench_pathological(c: &mut Criterion) {
    let mut group = c.benchmark_group("pathological");

    // Tiny ring (capacity=2): every publish overwrites, consumers always lag
    group.bench_function("tiny_ring_cap2", |b| {
        let config = BctowConfig {
            ring_buffer_capacity: 2,
            overflow_policy: OverflowPolicy::DropOldest,
        };
        let (producer, ctx, topics) = setup_broker_with_topics::<u64>(config, 1);
        let tid = topics[0];
        let mut clients = spawn_consumers(&ctx, &topics, 4);

        b.iter(|| {
            black_box(producer.publish_id(tid, black_box(&1u64)).unwrap());
            black_box(producer.publish_id(tid, black_box(&2u64)).unwrap());
            black_box(producer.publish_id(tid, black_box(&3u64)).unwrap());
            for client in clients.iter_mut() {
                black_box(client.poll());
            }
        });
    });

    // Many topics, single message each: stresses topic registry lookup
    group.bench_function("many_topics_256", |b| {
        let config = BctowConfig { ring_buffer_capacity: 16, ..Default::default() };
        let (producer, ctx, topics) = setup_broker_with_topics::<u64>(config, 256);
        let mut clients = spawn_consumers(&ctx, &topics, 1);
        let client = &mut clients[0];

        b.iter(|| {
            for &tid in &topics {
                black_box(producer.publish_id(tid, black_box(&42u64)).unwrap());
            }
            while black_box(client.poll()).is_some() {}
        });
    });

    // Subscribe/unsubscribe churn: measures pure sub/unsub overhead.
    // poll() returns None because Cursor::new_latest positions past existing data.
    group.bench_function("subscribe_churn", |b| {
        let config = BctowConfig { ring_buffer_capacity: 64, ..Default::default() };
        let (producer, ctx, topics) = setup_broker_with_topics::<u64>(config, 8);
        // Pre-publish so topics have data
        for &tid in &topics {
            producer.publish_id(tid, &1u64).unwrap();
        }

        b.iter(|| {
            let mut client = ctx.create_client();
            for &tid in &topics {
                client.subscribe_id(tid).unwrap();
            }
            black_box(client.poll());
            for &tid in &topics {
                client.unsubscribe(tid);
            }
        });
    });

    // SkipToLatest overflow: consumer re-creates lag every iteration
    // Publishes capacity+1 messages so the consumer's cursor is always behind
    // oldest_available_seq, forcing the SkipToLatest policy path on every poll.
    group.bench_function("skip_to_latest_overflow", |b| {
        let cap = 4usize;
        let config = BctowConfig {
            ring_buffer_capacity: cap,
            overflow_policy: OverflowPolicy::SkipToLatest,
        };
        let (producer, ctx, topics) = setup_broker_with_topics::<u64>(config, 1);
        let tid = topics[0];
        let mut clients = spawn_consumers(&ctx, &topics, 1);
        let client = &mut clients[0];

        b.iter(|| {
            // Push enough to guarantee the consumer is behind oldest_available_seq
            for i in 0..(cap as u64 + 1) {
                producer.publish_id(tid, black_box(&i)).unwrap();
            }
            // Poll triggers SkipToLatest → cursor jumps to latest+1, returns None
            black_box(client.poll());
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Benchmark — publish by name (string lookup) vs by id (cached ring)
// ---------------------------------------------------------------------------

fn bench_publish_name_vs_id(c: &mut Criterion) {
    let mut group = c.benchmark_group("publish_path");

    let config = BctowConfig { ring_buffer_capacity: 1024, ..Default::default() };
    let (producer, _ctx, topics) = setup_broker_with_topics::<u64>(config, 1);
    let tid = topics[0];

    group.bench_function("by_id", |b| {
        b.iter(|| black_box(producer.publish_id(tid, black_box(&42u64)).unwrap()));
    });

    group.bench_function("by_name", |b| {
        b.iter(|| black_box(producer.publish("topic/0", black_box(&42u64)).unwrap()));
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_publish_throughput,
    bench_consumer_scaling,
    bench_topic_scaling,
    bench_pathological,
    bench_publish_name_vs_id,
);
criterion_main!(benches);

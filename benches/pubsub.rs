use bctow::{BctowConfig, ConsumerContext, FixedBytes, Producer, TopicId};
use criterion::{black_box, criterion_group, criterion_main, Criterion};

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

#[allow(dead_code)]
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

#[allow(dead_code)]
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

#[allow(dead_code)]
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
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_single_publish(c: &mut Criterion) {
    let config = BctowConfig {
        ring_buffer_capacity: 1024,
        ..Default::default()
    };
    let (producer, _ctx, topics) = setup_broker_with_topics::<u64>(config, 1);
    let topic_id = topics[0];
    c.bench_function("publish/single/u64", |b| {
        b.iter(|| {
            black_box(producer.publish_id(topic_id, black_box(&42u64)).unwrap());
        });
    });
}

criterion_group!(benches, bench_single_publish);
criterion_main!(benches);

use bctow::{BctowConfig, ConsumerContext, FixedBytes, Producer, TopicId};
use criterion::{criterion_group, criterion_main, Criterion};

// ---------------------------------------------------------------------------
// Fixture types
// ---------------------------------------------------------------------------

/// 128-byte struct representing a typical sensor reading or event.
#[allow(dead_code)]
#[derive(Copy, Clone)]
struct MediumPayload {
    f0: u64,
    f1: u64,
    f2: u64,
    f3: u64,
    f4: u64,
    f5: u64,
    f6: u64,
    f7: u64,
    f8: u64,
    f9: u64,
    f10: u64,
    f11: u64,
    f12: u64,
    f13: u64,
    f14: u64,
    f15: u64,
}

// ---------------------------------------------------------------------------
// Fixture constructors
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn make_medium_payload() -> MediumPayload {
    MediumPayload {
        f0: 0,
        f1: 1,
        f2: 2,
        f3: 3,
        f4: 4,
        f5: 5,
        f6: 6,
        f7: 7,
        f8: 8,
        f9: 9,
        f10: 10,
        f11: 11,
        f12: 12,
        f13: 13,
        f14: 14,
        f15: 15,
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
            producer.publish_id(topic_id, &42u64).unwrap();
        });
    });
}

criterion_group!(benches, bench_single_publish);
criterion_main!(benches);

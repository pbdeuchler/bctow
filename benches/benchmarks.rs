#[allow(dead_code)]
#[path = "support/harness.rs"]
mod harness;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use criterion::{black_box, criterion_group, BenchmarkId, Criterion};

fn operations_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("operations");

    // publish_id_warm: idealized hot path — no lock, no string lookup.
    group.bench_function("publish_id_warm", |b| {
        let (producer, _ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];
        // Warm the producer's ring cache with one publish before measuring.
        producer.publish_id(topic_id, &0u64).unwrap();
        b.iter(|| {
            producer
                .publish_id(black_box(topic_id), black_box(&42u64))
                .unwrap();
        });
    });

    // publish_string_lookup: same publish path but via string name.
    // Delta vs publish_id_warm isolates RwLock read + HashMap lookup cost.
    group.bench_function("publish_string_lookup", |b| {
        let (producer, _ctx, _topic_ids) = harness::setup_broker::<u64>(1, 1024);
        // Warm with one publish so the ring cache entry exists.
        producer.publish("topic/0", &0u64).unwrap();
        b.iter(|| {
            producer
                .publish(black_box("topic/0"), black_box(&42u64))
                .unwrap();
        });
    });

    // poll_hit: publish one message then poll it. Measures publish+poll round-trip.
    // Subtracting publish_id_warm from this isolates the poll cost, assuming
    // publish_id overhead is identical in both benchmarks (same code path).
    group.bench_function("poll_hit", |b| {
        let (producer, ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        b.iter(|| {
            producer.publish_id(topic_id, &42u64).unwrap();
            black_box(client.poll());
        });
    });

    // poll_miss: consumer subscribed, no messages pending. Measures scan overhead baseline.
    group.bench_function("poll_miss", |b| {
        let (_producer, ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        b.iter(|| {
            black_box(client.poll());
        });
    });

    group.finish();
}

fn message_size_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("message_size");

    // u64 baseline — intentionally overlaps with operations/poll_hit to establish
    // the copy-cost floor within this group for comparison against larger types.
    group.bench_function("u64", |b| {
        let (producer, ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        let val: u64 = 42;
        b.iter(|| {
            producer
                .publish_id(black_box(topic_id), black_box(&val))
                .unwrap();
            black_box(client.poll());
        });
    });

    group.bench_function("fixed_bytes_64", |b| {
        let (producer, ctx, topic_ids) = harness::setup_broker::<bctow::FixedBytes<64>>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        let val = harness::fill_fixed_bytes::<64>();
        b.iter(|| {
            producer
                .publish_id(black_box(topic_id), black_box(&val))
                .unwrap();
            black_box(client.poll());
        });
    });

    group.bench_function("fixed_bytes_512", |b| {
        let (producer, ctx, topic_ids) = harness::setup_broker::<bctow::FixedBytes<512>>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        let val = harness::fill_fixed_bytes::<512>();
        b.iter(|| {
            producer
                .publish_id(black_box(topic_id), black_box(&val))
                .unwrap();
            black_box(client.poll());
        });
    });

    group.bench_function("fixed_bytes_4096", |b| {
        let (producer, ctx, topic_ids) = harness::setup_broker::<bctow::FixedBytes<4096>>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        let val = harness::fill_fixed_bytes::<4096>();
        b.iter(|| {
            producer
                .publish_id(black_box(topic_id), black_box(&val))
                .unwrap();
            black_box(client.poll());
        });
    });

    group.finish();
}

// criterion_group! generates a callable function `benches` that creates a Criterion
// instance and runs the registered benchmark groups. We call it from our custom main().
fn subscription_scan_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription_scan");
    for count in [1, 32, 1024, 10_000] {
        for active in [false, true] {
            let (producer, ctx, topics) = harness::setup_broker::<u64>(count, 4);
            let mut client = ctx.create_client();
            for &topic in &topics {
                client.subscribe_id(topic).unwrap();
            }
            let last = *topics.last().unwrap();
            let name = if active { "last_active" } else { "idle" };
            group.bench_with_input(BenchmarkId::new(name, count), &count, |b, _| {
                b.iter(|| {
                    if active {
                        producer.publish_id(last, black_box(&42)).unwrap();
                        black_box(client.poll().expect("last topic has a pending message"));
                    } else {
                        assert!(black_box(client.poll()).is_none());
                    }
                });
            });
        }
    }
    group.finish();
}

fn ring_capacity_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_capacity");
    for capacity in [1, 3, 31, 32, 1024] {
        let (producer, ctx, topics) = harness::setup_broker::<u64>(1, capacity);
        let topic = topics[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic).unwrap();
        group.bench_with_input(BenchmarkId::from_parameter(capacity), &capacity, |b, _| {
            b.iter(|| {
                producer.publish_id(topic, black_box(&42)).unwrap();
                black_box(client.poll().expect("published message must be retained"));
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    operations_benchmark,
    message_size_benchmark,
    subscription_scan_benchmark,
    ring_capacity_benchmark
);

fn consumer_scaling() {
    let consumer_counts = [1, 10, 50, 100, 250, 500, 1000, 2000];
    let message_count = 1000u64;

    for &consumers in &consumer_counts {
        let (producer, ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];

        // Each consumer sees all message_count messages; total deliveries = consumers * message_count.
        let handle =
            match harness::spawn_consumers(&ctx, &topic_ids, consumers, consumers, message_count) {
                Ok(handle) => handle,
                Err(error) => {
                    eprintln!(
                        "[scaling/consumer_scaling] consumers={consumers} unavailable: {error}"
                    );
                    continue;
                }
            };
        let actual_threads = handle.thread_count();

        // Publish all messages before releasing consumers so they drain in one burst.
        // capacity=1024 >= message_count=1000 so all fit in the ring.
        for i in 0..message_count {
            producer.publish_id(topic_id, &i).unwrap();
        }

        handle.release();
        let start = std::time::Instant::now();
        let completed = handle.wait(std::time::Duration::from_secs(30));
        let elapsed = start.elapsed();

        let total_deliveries = (consumers as u64) * message_count;
        let elapsed_ns = elapsed.as_nanos() as f64;
        let throughput = total_deliveries as f64 / (elapsed_ns / 1_000_000_000.0);
        let latency_ns = elapsed_ns / total_deliveries as f64;

        println!(
            "[scaling/consumer_scaling] consumers={consumers} threads={} messages={message_count} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} completed={completed}",
            actual_threads,
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

fn topic_scaling() {
    let topic_counts = [1, 10, 100, 1000, 10000];
    let messages_per_topic = 100u64;

    for &topics in &topic_counts {
        // Reduce capacity for high topic counts to control memory usage.
        let capacity = if topics >= 10000 { 128 } else { 1024 };
        let (producer, ctx, topic_ids) = harness::setup_broker::<u64>(topics, capacity);

        let mut client = ctx.create_client();
        for &id in &topic_ids {
            client.subscribe_id(id).unwrap();
        }

        // Publish all messages up front; at 10K topics this primarily measures
        // poll()'s O(N) HashMap scan over subscriptions, not ring performance.
        for &id in &topic_ids {
            for i in 0..messages_per_topic {
                producer.publish_id(id, &i).unwrap();
            }
        }

        let total_messages = topics as u64 * messages_per_topic;
        let mut received = 0u64;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        let completed = loop {
            if received >= total_messages {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                break false;
            }
            if let Some(msg) = client.poll() {
                std::hint::black_box(msg);
                received += 1;
            }
        };
        let elapsed = start.elapsed();

        let elapsed_ns = elapsed.as_nanos() as f64;
        let throughput = received as f64 / (elapsed_ns / 1_000_000_000.0);
        let latency_ns = if received > 0 {
            elapsed_ns / received as f64
        } else {
            0.0
        };

        // At 10K topics the measurement is dominated by poll()'s O(N) HashMap scan.
        let note = if topics >= 10000 {
            " note=primarily_measures_poll_scan"
        } else {
            ""
        };
        println!(
            "[scaling/topic_scaling] topics={topics} messages_per_topic={messages_per_topic} total_messages={total_messages} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} completed={completed}{note}",
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

fn combined_scaling() {
    // [topics, consumers] configurations
    let configs: &[(usize, usize)] = &[(10, 100), (100, 10), (100, 100)];
    let messages_per_topic = 100u64;

    for &(topics, consumers) in configs {
        let (producer, ctx, topic_ids) = harness::setup_broker::<u64>(topics, 1024);

        // Each consumer is subscribed to all topics and expects topics * messages_per_topic messages.
        let msgs_per_consumer = topics as u64 * messages_per_topic;
        let handle = match harness::spawn_consumers(
            &ctx,
            &topic_ids,
            consumers,
            consumers,
            msgs_per_consumer,
        ) {
            Ok(handle) => handle,
            Err(error) => {
                eprintln!("[scaling/combined_scaling] consumers={consumers} unavailable: {error}");
                continue;
            }
        };
        let actual_threads = handle.thread_count();

        // Publish all messages before releasing consumers.
        for &id in &topic_ids {
            for i in 0..messages_per_topic {
                producer.publish_id(id, &i).unwrap();
            }
        }

        handle.release();
        let start = std::time::Instant::now();
        let completed = handle.wait(std::time::Duration::from_secs(60));
        let elapsed = start.elapsed();

        let total_deliveries = (consumers as u64) * msgs_per_consumer;
        let elapsed_ns = elapsed.as_nanos() as f64;
        let throughput = total_deliveries as f64 / (elapsed_ns / 1_000_000_000.0);
        let latency_ns = elapsed_ns / total_deliveries as f64;

        println!(
            "[scaling/combined_scaling] topics={topics} consumers={consumers} threads={actual_threads} messages_per_topic={messages_per_topic} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} completed={completed}",
            elapsed.as_secs_f64() * 1000.0
        );
    }
}

fn scaling_benchmarks() {
    println!("\n=== Scaling Benchmarks ===\n");
    consumer_scaling();
    topic_scaling();
    combined_scaling();
}

// ── Stress Benchmarks ────────────────────────────────────────────────────────

/// Power-of-2 consumer sweep on a single topic. Each consumer gets its own
/// thread where resources permit. Reuses slots while consumers poll, making
/// reader/writer contention possible. Publish latency includes scheduling and
/// copying costs; this workload does not isolate or prove reader spin-wait.
fn contention_cliff() {
    let consumer_counts: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    let message_count = harness::CONTENTION_MESSAGES;

    for &consumers in consumer_counts {
        let config = bctow::BctowConfig {
            ring_buffer_capacity: harness::CONTENTION_CAPACITY,
            overflow_policy: bctow::OverflowPolicy::DropOldest,
        };
        let (producer, ctx) = bctow::broker::<bctow::FixedBytes<512>>(config);
        let topic_id = producer.register_topic("topic/0");
        let val = harness::fill_fixed_bytes::<512>();

        // Thread-per-consumer: pass `consumers` as both count and thread limit.
        let handle = match harness::spawn_consumers_until(
            &ctx,
            &[topic_id],
            consumers,
            consumers,
            |message| message.is_empty(),
        ) {
            Ok(handle) => handle,
            Err(error) => {
                eprintln!("[stress/contention_cliff] consumers={consumers} unavailable: {error}");
                continue;
            }
        };
        let actual_threads = handle.thread_count();

        // Release consumers first, then publish through many complete ring wraps.
        handle.release();
        let publish_start = std::time::Instant::now();
        for _ in 0..message_count {
            producer.publish_id(topic_id, &val).unwrap();
        }
        let publish_elapsed = publish_start.elapsed();

        // The retained terminal message lets every consumer finish even if it
        // missed earlier messages. Keep its publication outside the measurement.
        producer
            .publish_id(topic_id, &bctow::FixedBytes::new(&[]).unwrap())
            .unwrap();
        let completed = handle.wait(std::time::Duration::from_secs(60));

        let publish_ns = publish_elapsed.as_nanos() as f64;
        let publish_ns_per_msg = publish_ns / message_count as f64;
        let throughput = message_count as f64 / (publish_ns / 1_000_000_000.0);

        if actual_threads < consumers {
            println!(
                "  WARNING: contention_cliff consumers={consumers} threads={actual_threads}, results reflect multiplexed contention"
            );
        }
        println!(
            "[stress/contention_cliff] consumers={consumers} threads={actual_threads} messages={message_count} publish_latency_ns_per_msg={publish_ns_per_msg:.0} throughput_msg_sec={throughput:.0} elapsed_ms={:.2} completed={completed}",
            publish_elapsed.as_secs_f64() * 1000.0,
        );
    }
}

/// Single consumer subscribed to 10K topics, but messages land on only one.
/// Surfaces the O(subscriptions) round-robin scan cost per successful poll().
fn sparse_poll() {
    let topic_count = 10_000usize;
    let message_count = 100u64;

    let config = bctow::BctowConfig {
        ring_buffer_capacity: 128,
        overflow_policy: bctow::OverflowPolicy::DropOldest,
    };
    let (producer, ctx) = bctow::broker::<bctow::FixedBytes<512>>(config);

    let topic_ids: Vec<bctow::TopicId> = (0..topic_count)
        .map(|i| producer.register_topic(format!("topic/{i}")))
        .collect();

    let mut client = ctx.create_client();
    for &id in &topic_ids {
        client.subscribe_id(id).unwrap();
    }

    // Only publish to topic 0 — all other 9999 subscriptions are dead weight
    // that poll() must iterate past on every round-robin cycle.
    let val = harness::fill_fixed_bytes::<512>();
    for _ in 0..message_count {
        producer.publish_id(topic_ids[0], &val).unwrap();
    }

    let mut received = 0u64;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    let start = std::time::Instant::now();
    loop {
        if received >= message_count {
            break;
        }
        if std::time::Instant::now() >= deadline {
            break;
        }
        if let Some(msg) = client.poll() {
            std::hint::black_box(msg);
            received += 1;
        }
    }
    let elapsed = start.elapsed();

    let elapsed_ns = elapsed.as_nanos() as f64;
    let throughput = received as f64 / (elapsed_ns / 1_000_000_000.0);
    let latency_ns = if received > 0 {
        elapsed_ns / received as f64
    } else {
        0.0
    };

    println!(
        "[stress/sparse_poll] topics={topic_count} active_topics=1 messages={message_count} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} note=O(subscriptions)_scan_per_poll",
        elapsed.as_secs_f64() * 1000.0,
    );
}

/// Compare DropOldest / DropNewest / SkipToLatest at 2x overproduce rate.
/// Consumers poll with a deliberate 100µs sleep so the ring fills and the
/// overflow policy engages. Reports received vs published to quantify drops.
///
/// This benchmark cannot use `spawn_consumers`/`drain_loop` because those
/// poll at full speed. We need per-poll sleep to ensure the producer outpaces
/// consumers and the overflow policy actually engages.
fn overflow_policy_comparison() {
    let policies = [
        bctow::OverflowPolicy::DropOldest,
        bctow::OverflowPolicy::DropNewest,
        bctow::OverflowPolicy::SkipToLatest,
    ];
    let ring_capacity = 32usize;
    let consumer_count = 100usize;
    let published = 500u64;

    for policy in policies {
        let config = bctow::BctowConfig {
            ring_buffer_capacity: ring_capacity,
            overflow_policy: policy,
        };
        let (producer, ctx) = bctow::broker::<bctow::FixedBytes<512>>(config);
        let topic_id = producer.register_topic("topic/0");
        let val = harness::fill_fixed_bytes::<512>();

        // +1 for the main thread which acts as the release signal
        let barrier = Arc::new(Barrier::new(consumer_count + 1));
        let received_total = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));

        let mut handles = Vec::with_capacity(consumer_count);
        for _ in 0..consumer_count {
            let mut client = ctx.create_client();
            client.subscribe_id(topic_id).unwrap();

            let b = Arc::clone(&barrier);
            let counter = Arc::clone(&received_total);
            let stop_flag = Arc::clone(&stop);

            let h = std::thread::spawn(move || {
                b.wait();
                while !stop_flag.load(Ordering::Acquire) {
                    if let Some(msg) = client.poll() {
                        std::hint::black_box(msg);
                        counter.fetch_add(1, Ordering::AcqRel);
                    }
                    // Sleep after every poll (hit or miss) to ensure the producer
                    // outpaces consumers and the overflow policy engages.
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
            });
            handles.push(h);
        }

        // Release all consumers, then publish as fast as possible.
        barrier.wait();
        let publish_start = std::time::Instant::now();
        for _ in 0..published {
            // Result is always Ok(seq) — overflow is handled on the consumer side
            // via try_read_with_policy, not on the producer side.
            let _ = producer.publish_id(topic_id, &val);
        }
        let publish_elapsed = publish_start.elapsed();

        // Yield the main thread so consumer threads get CPU time to drain.
        std::thread::sleep(std::time::Duration::from_secs(2));
        stop.store(true, Ordering::Release);
        for h in handles {
            let _ = h.join();
        }

        let total_received = received_total.load(Ordering::Acquire);
        let avg_per_consumer = total_received as f64 / consumer_count as f64;
        let policy_name = match policy {
            bctow::OverflowPolicy::DropOldest => "DropOldest",
            bctow::OverflowPolicy::DropNewest => "DropNewest",
            bctow::OverflowPolicy::SkipToLatest => "SkipToLatest",
        };

        println!(
            "[stress/overflow_policy_comparison] policy={policy_name} capacity={ring_capacity} consumers={consumer_count} published={published} received_total={total_received} avg_received_per_consumer={avg_per_consumer:.1} publish_ms={:.2}",
            publish_elapsed.as_secs_f64() * 1000.0,
        );
    }
}

/// Fanout benchmark with 4KB messages to surface memory-bandwidth ceiling.
/// At 500 consumers × 4KB, each publish drives ~2MB of DRAM reads.
fn large_message_fanout() {
    let consumer_counts: &[usize] = &[10, 50, 100, 500];
    let message_count = 100u64;

    for &consumers in consumer_counts {
        let (producer, ctx, topic_ids) = harness::setup_broker::<bctow::FixedBytes<4096>>(1, 1024);
        let topic_id = topic_ids[0];
        let val = harness::fill_fixed_bytes::<4096>();

        let handle =
            match harness::spawn_consumers(&ctx, &topic_ids, consumers, consumers, message_count) {
                Ok(handle) => handle,
                Err(error) => {
                    eprintln!(
                        "[stress/large_message_fanout] consumers={consumers} unavailable: {error}"
                    );
                    continue;
                }
            };
        let actual_threads = handle.thread_count();

        for _ in 0..message_count {
            producer.publish_id(topic_id, &val).unwrap();
        }

        handle.release();
        let start = std::time::Instant::now();
        let completed = handle.wait(std::time::Duration::from_secs(60));
        let elapsed = start.elapsed();

        let total_deliveries = (consumers as u64) * message_count;
        let elapsed_ns = elapsed.as_nanos() as f64;
        let throughput = total_deliveries as f64 / (elapsed_ns / 1_000_000_000.0);
        let latency_ns = elapsed_ns / total_deliveries as f64;

        println!(
            "[stress/large_message_fanout] consumers={consumers} threads={actual_threads} message_size=4096 messages={message_count} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} completed={completed}",
            elapsed.as_secs_f64() * 1000.0,
        );
    }
}

fn stress_benchmarks() {
    println!("\n=== Stress Benchmarks ===\n");
    contention_cliff();
    sparse_poll();
    overflow_policy_comparison();
    large_message_fanout();
}

fn main() {
    // Criterion passes positional filter args after `--`. Detect whether the
    // user supplied a filter so we can skip custom benchmarks when they only
    // target a Criterion group (e.g., `-- operations`). Note: cargo always
    // injects `--bench`, so we must ignore flags when detecting a user filter.
    let positional_filter: Option<String> = std::env::args().skip(1).find(|a| !a.starts_with('-'));

    let run_scaling = positional_filter
        .as_ref()
        .is_none_or(|f| f.contains("scaling"));
    let run_stress = positional_filter
        .as_ref()
        .is_none_or(|f| f.contains("stress"));

    if run_scaling {
        scaling_benchmarks();
    }
    if run_stress {
        stress_benchmarks();
    }
    benches();
}

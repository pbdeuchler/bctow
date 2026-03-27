#[allow(dead_code)]
mod harness {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    use bctow::{BctowConfig, ConsumerClient, ConsumerContext, FixedBytes, OverflowPolicy, Producer, TopicId};

    static THREAD_CEILING: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

    /// Empirically determines the maximum number of threads this process can spawn.
    /// Binary searches for the highest N where spawning N threads succeeds. Result
    /// is cached in a OnceLock so the probe only runs once.
    pub fn thread_ceiling() -> usize {
        *THREAD_CEILING.get_or_init(|| {
            // Binary search between 1 and 4096 for the thread ceiling.
            // Probe by spawning N threads with trivial bodies and joining them.
            // No barrier — threads just exit immediately to avoid any blocking.
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

            // Reserve headroom for the caller's own threads
            last_success.saturating_sub(8).max(1)
        })
    }

    /// Create a broker with `capacity`-slot rings, register `topics` topics by the
    /// pattern `"topic/{i}"`, and return the producer, consumer context, and topic IDs.
    pub fn setup_broker<T: Copy + Send + Sync + 'static>(
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

    /// Handle returned by `spawn_consumers`. Allows the caller to release the
    /// start barrier and then wait for all consumers to drain their messages.
    pub struct ConsumerHandle {
        barrier: Arc<Barrier>,
        done: Arc<AtomicU64>,
        thread_count: u64,
        stop: Arc<AtomicBool>,
        threads: Vec<thread::JoinHandle<()>>,
    }

    impl ConsumerHandle {
        /// Release the barrier so all consumer threads begin polling.
        pub fn release(&self) {
            self.barrier.wait();
        }

        /// Wait up to `timeout` for all consumers to finish.
        /// Returns `true` if they completed within the deadline.
        pub fn wait(mut self, timeout: Duration) -> bool {
            let deadline = std::time::Instant::now() + timeout;
            // Spin-check the done counter rather than parking, to avoid
            // re-introducing scheduler-induced latency in bench measurements.
            while self.done.load(Ordering::Acquire) < self.thread_count {
                if std::time::Instant::now() >= deadline {
                    self.stop.store(true, Ordering::Release);
                    return false;
                }
                std::hint::spin_loop();
            }
            for h in self.threads.drain(..) {
                let _ = h.join();
            }
            true
        }
    }

    /// Spawn consumer threads. Creates `count` ConsumerClient instances, each
    /// subscribed to all `topic_ids`. Distributes clients across
    /// `min(count, thread_ceiling())` threads. Returns a ConsumerHandle whose
    /// `release()` unblocks all threads, and whose `wait()` blocks until all
    /// clients have drained `messages_per_consumer` messages.
    pub fn spawn_consumers<T: Copy + Send + Sync + 'static>(
        ctx: &ConsumerContext<T>,
        topic_ids: &[TopicId],
        count: usize,
        threads: usize,
        messages_per_consumer: u64,
    ) -> ConsumerHandle {
        let ceiling = thread_ceiling();
        let actual_threads = count.min(threads).min(ceiling);

        // +1 for the caller which calls release()
        let barrier = Arc::new(Barrier::new(actual_threads + 1));
        // Incremented once per thread when it exits the drain loop
        let done = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let total_messages = (count as u64) * messages_per_consumer;

        // Create all clients up front on the calling thread (ConsumerContext is Sync)
        let mut all_clients: Vec<ConsumerClient<T>> = (0..count)
            .map(|_| {
                let mut client = ctx.create_client();
                for &id in topic_ids {
                    client.subscribe_id(id).expect("subscribe_id failed");
                }
                client
            })
            .collect();

        // Distribute clients across threads in round-robin chunks
        let clients_per_thread = count / actual_threads;
        let remainder = count % actual_threads;

        let mut thread_handles = Vec::with_capacity(actual_threads);
        let counter = Arc::new(AtomicU64::new(0));

        for t in 0..actual_threads {
            // Each thread gets clients_per_thread + (1 if t < remainder)
            let extra = if t < remainder { 1 } else { 0 };
            let batch_size = clients_per_thread + extra;
            let batch: Vec<ConsumerClient<T>> = all_clients.drain(..batch_size).collect();

            let b = Arc::clone(&barrier);
            let ctr = Arc::clone(&counter);
            let done_flag = Arc::clone(&done);
            let stop_flag = Arc::clone(&stop);

            let handle = thread::spawn(move || {
                drain_loop(batch, b, total_messages, ctr, stop_flag);
                done_flag.fetch_add(1, Ordering::Release);
            });
            thread_handles.push(handle);
        }

        ConsumerHandle {
            barrier,
            done,
            thread_count: actual_threads as u64,
            stop,
            threads: thread_handles,
        }
    }

    /// Per-thread poll loop. Waits on `barrier`, then polls all owned clients in
    /// round-robin, incrementing `counter` on each received message. Exits when
    /// the shared counter reaches `messages_expected` or `stop` is set.
    pub fn drain_loop<T: Copy + Send + Sync + 'static>(
        mut clients: Vec<ConsumerClient<T>>,
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

    /// Produces a deterministic non-zero fill pattern for `FixedBytes<N>`.
    /// Cycles through 0x01..=0xFF so there are no zero bytes.
    pub fn fill_fixed_bytes<const N: usize>() -> FixedBytes<N> {
        let mut data = [0u8; N];
        for i in 0..N {
            data[i] = ((i % 255) + 1) as u8;
        }
        FixedBytes::new(&data).expect("fill_fixed_bytes: slice fits in N")
    }
}

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};

use criterion::{black_box, criterion_group, Criterion};

fn operations_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("operations");

    // publish_id_warm: idealized hot path — no lock, no string lookup.
    group.bench_function("publish_id_warm", |b| {
        let (producer, _ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];
        // Warm the producer's ring cache with one publish before measuring.
        producer.publish_id(topic_id, &0u64).unwrap();
        b.iter(|| {
            producer.publish_id(black_box(topic_id), black_box(&42u64)).unwrap();
        });
    });

    // publish_string_lookup: same publish path but via string name.
    // Delta vs publish_id_warm isolates RwLock read + HashMap lookup cost.
    group.bench_function("publish_string_lookup", |b| {
        let (producer, _ctx, _topic_ids) = harness::setup_broker::<u64>(1, 1024);
        // Warm with one publish so the ring cache entry exists.
        producer.publish("topic/0", &0u64).unwrap();
        b.iter(|| {
            producer.publish(black_box("topic/0"), black_box(&42u64)).unwrap();
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
            producer.publish_id(black_box(topic_id), black_box(&val)).unwrap();
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
            producer.publish_id(black_box(topic_id), black_box(&val)).unwrap();
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
            producer.publish_id(black_box(topic_id), black_box(&val)).unwrap();
            black_box(client.poll());
        });
    });

    group.bench_function("fixed_bytes_4096", |b| {
        let (producer, ctx, topic_ids) =
            harness::setup_broker::<bctow::FixedBytes<4096>>(1, 1024);
        let topic_id = topic_ids[0];
        let mut client = ctx.create_client();
        client.subscribe_id(topic_id).unwrap();
        let val = harness::fill_fixed_bytes::<4096>();
        b.iter(|| {
            producer.publish_id(black_box(topic_id), black_box(&val)).unwrap();
            black_box(client.poll());
        });
    });

    group.finish();
}

// criterion_group! generates a callable function `benches` that creates a Criterion
// instance and runs the registered benchmark groups. We call it from our custom main().
criterion_group!(benches, operations_benchmark, message_size_benchmark);

fn consumer_scaling() {
    let consumer_counts = [1, 10, 50, 100, 250, 500, 1000, 2000];
    let message_count = 1000u64;

    for &consumers in &consumer_counts {
        let (producer, ctx, topic_ids) = harness::setup_broker::<u64>(1, 1024);
        let topic_id = topic_ids[0];

        // Each consumer sees all message_count messages; total deliveries = consumers * message_count.
        let handle = harness::spawn_consumers(&ctx, &topic_ids, consumers, consumers, message_count);

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
            consumers.min(harness::thread_ceiling()),
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
        let latency_ns = if received > 0 { elapsed_ns / received as f64 } else { 0.0 };

        // At 10K topics the measurement is dominated by poll()'s O(N) HashMap scan.
        let note = if topics >= 10000 { " note=primarily_measures_poll_scan" } else { "" };
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
        let handle = harness::spawn_consumers(&ctx, &topic_ids, consumers, consumers, msgs_per_consumer);

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
            "[scaling/combined_scaling] topics={topics} consumers={consumers} messages_per_topic={messages_per_topic} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} completed={completed}",
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
/// thread (no multiplexing) so that readers-atomic contention is maximal.
/// Measures publish-side latency to locate the exact cliff where the
/// ring.push() spin-wait on `readers == 0` inflates from ns to µs.
fn contention_cliff() {
    let consumer_counts: &[usize] = &[1, 2, 4, 8, 16, 32, 64, 128, 256, 512];
    let message_count = 500u64;

    for &consumers in consumer_counts {
        let config = bctow::BctowConfig {
            ring_buffer_capacity: 1024,
            overflow_policy: bctow::OverflowPolicy::DropOldest,
        };
        let (producer, ctx) = bctow::broker::<bctow::FixedBytes<512>>(config);
        let topic_id = producer.register_topic("topic/0");
        let val = harness::fill_fixed_bytes::<512>();

        // Thread-per-consumer: pass `consumers` as both count and thread limit.
        let handle = harness::spawn_consumers(&ctx, &[topic_id], consumers, consumers, message_count);

        // Release consumers first, then time publish to capture the spin-wait cost.
        handle.release();
        let publish_start = std::time::Instant::now();
        for _ in 0..message_count {
            producer.publish_id(topic_id, &val).unwrap();
        }
        let publish_elapsed = publish_start.elapsed();

        let completed = handle.wait(std::time::Duration::from_secs(60));

        let publish_ns = publish_elapsed.as_nanos() as f64;
        let publish_ns_per_msg = publish_ns / message_count as f64;
        let throughput = message_count as f64 / (publish_ns / 1_000_000_000.0);

        let actual_threads = consumers.min(harness::thread_ceiling());
        if actual_threads < consumers {
            println!(
                "  WARNING: contention_cliff consumers={consumers} exceeds thread_ceiling={actual_threads}, results reflect multiplexed contention"
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
    let latency_ns = if received > 0 { elapsed_ns / received as f64 } else { 0.0 };

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
        let (producer, ctx, topic_ids) =
            harness::setup_broker::<bctow::FixedBytes<4096>>(1, 1024);
        let topic_id = topic_ids[0];
        let val = harness::fill_fixed_bytes::<4096>();

        let handle = harness::spawn_consumers(&ctx, &topic_ids, consumers, consumers, message_count);

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
            "[stress/large_message_fanout] consumers={consumers} message_size=4096 messages={message_count} throughput_msg_sec={throughput:.0} latency_ns_per_msg={latency_ns:.0} elapsed_ms={:.2} completed={completed}",
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
    let positional_filter: Option<String> = std::env::args()
        .skip(1)
        .find(|a| !a.starts_with('-'));

    let run_scaling = positional_filter.as_ref().map_or(true, |f| f.contains("scaling"));
    let run_stress = positional_filter.as_ref().map_or(true, |f| f.contains("stress"));

    if run_scaling {
        scaling_benchmarks();
    }
    if run_stress {
        stress_benchmarks();
    }
    benches();
}

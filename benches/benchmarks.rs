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

use criterion::{black_box, criterion_group, criterion_main, Criterion};

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

criterion_group!(benches, operations_benchmark, message_size_benchmark);
criterion_main!(benches);

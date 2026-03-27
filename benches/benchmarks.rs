#[allow(dead_code)]
mod harness {
    use std::sync::atomic::{AtomicU64, Ordering};
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
            // We probe by spawning N threads that immediately park, then join them.
            let mut lo = 1usize;
            let mut hi = 4096usize;
            let mut last_success = 1usize;

            while lo <= hi {
                let mid = lo + (hi - lo) / 2;
                let barrier = Arc::new(Barrier::new(mid + 1));
                let handles: Vec<_> = (0..mid)
                    .map(|_| {
                        let b = Arc::clone(&barrier);
                        thread::Builder::new()
                            .stack_size(64 * 1024)
                            .spawn(move || {
                                b.wait();
                            })
                            .ok()
                    })
                    .collect();

                let all_spawned = handles.iter().all(|h| h.is_some());
                if all_spawned {
                    barrier.wait();
                    for h in handles.into_iter().flatten() {
                        let _ = h.join();
                    }
                    last_success = mid;
                    lo = mid + 1;
                } else {
                    // Some failed — drop any that succeeded without joining
                    for h in handles.into_iter().flatten() {
                        let _ = h.join();
                    }
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
        expected_consumers: u64,
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
            while self.done.load(Ordering::Acquire) < self.expected_consumers {
                if std::time::Instant::now() >= deadline {
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
        // Each "done" unit represents one consumer finishing
        let done = Arc::new(AtomicU64::new(0));
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

            let handle = thread::spawn(move || {
                drain_loop(batch, b, total_messages, ctr);
                done_flag.fetch_add(1, Ordering::Release);
            });
            thread_handles.push(handle);
        }

        ConsumerHandle {
            barrier,
            done,
            expected_consumers: actual_threads as u64,
            threads: thread_handles,
        }
    }

    /// Per-thread poll loop. Waits on `barrier`, then polls all owned clients in
    /// round-robin, incrementing `counter` on each received message. Exits when
    /// the shared counter reaches `messages_expected`.
    pub fn drain_loop<T: Copy + Send + Sync + 'static>(
        mut clients: Vec<ConsumerClient<T>>,
        barrier: Arc<Barrier>,
        messages_expected: u64,
        counter: Arc<AtomicU64>,
    ) {
        barrier.wait();
        let n = clients.len();
        if n == 0 {
            return;
        }
        loop {
            let current = counter.load(Ordering::Relaxed);
            if current >= messages_expected {
                break;
            }
            for client in &mut clients {
                if let Some(msg) = client.poll() {
                    std::hint::black_box(msg);
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    /// Produces a deterministic non-zero fill pattern for `FixedBytes<N>`.
    /// Cycles through 0x01..=0xFF so there are no zero bytes.
    pub fn fill_fixed_bytes<const N: usize>() -> FixedBytes<N> {
        let data: Vec<u8> = (0..N).map(|i| ((i % 255) + 1) as u8).collect();
        FixedBytes::new(&data).expect("fill_fixed_bytes: slice fits in N")
    }
}

fn main() {}

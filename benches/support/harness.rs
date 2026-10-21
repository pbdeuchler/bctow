use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Barrier};
use std::thread;
use std::time::Duration;

use bctow::{
    BctowConfig, ConsumerClient, ConsumerContext, FixedBytes, OverflowPolicy, Producer, TopicId,
};

pub const CONTENTION_CAPACITY: usize = 64;
pub const CONTENTION_MESSAGES: u64 = 4096;

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
    counter: Arc<AtomicU64>,
    thread_count: u64,
    stop: Arc<AtomicBool>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl ConsumerHandle {
    /// Number of workers actually running, including when spawning was limited.
    pub fn thread_count(&self) -> usize {
        self.threads.len()
    }

    /// Release the barrier so all consumer threads begin polling.
    pub fn release(&self) {
        self.barrier.wait();
    }

    /// Wait up to `timeout` for all consumers to finish.
    /// Returns `true` if they completed within the deadline.
    pub fn wait(self, timeout: Duration) -> bool {
        self.wait_with_count(timeout).0
    }

    /// Wait for completion and return the actual deliveries, including terminal
    /// messages and any partial progress made before cancellation on timeout.
    pub fn wait_with_count(mut self, timeout: Duration) -> (bool, u64) {
        let deadline = std::time::Instant::now() + timeout;
        // Spin-check the done counter rather than parking, to avoid
        // re-introducing scheduler-induced latency in bench measurements.
        let completed = loop {
            if self.done.load(Ordering::Acquire) >= self.thread_count {
                break true;
            }
            if std::time::Instant::now() >= deadline {
                self.stop.store(true, Ordering::Release);
                break false;
            }
            std::hint::spin_loop();
        };
        for h in self.threads.drain(..) {
            h.join().expect("consumer thread panicked");
        }
        (completed, self.counter.load(Ordering::Acquire))
    }
}

/// Completion condition for each workload.
#[derive(Clone, Copy)]
pub enum DrainTarget<T> {
    Messages(u64),
    Terminal(fn(&T) -> bool),
}

/// Spawn up to `min(count, threads)` live workers, then distribute all clients
/// among those that could be created. No speculative thread probe is needed.
/// Returns an error for zero counts or if no worker can be spawned.
pub fn spawn_consumers<T: Copy + Send + Sync + 'static>(
    ctx: &ConsumerContext<T>,
    topic_ids: &[TopicId],
    count: usize,
    threads: usize,
    messages_per_consumer: u64,
) -> io::Result<ConsumerHandle> {
    spawn_workers_with(
        ctx,
        topic_ids,
        count,
        threads,
        DrainTarget::Messages(messages_per_consumer),
        |task| thread::Builder::new().spawn(task),
    )
}

/// Drain each client until it observes the terminal message. Earlier messages
/// may be overwritten, so completion does not depend on a delivery count.
pub fn spawn_consumers_until<T: Copy + Send + Sync + 'static>(
    ctx: &ConsumerContext<T>,
    topic_ids: &[TopicId],
    count: usize,
    threads: usize,
    is_terminal: fn(&T) -> bool,
) -> io::Result<ConsumerHandle> {
    spawn_workers_with(
        ctx,
        topic_ids,
        count,
        threads,
        DrainTarget::Terminal(is_terminal),
        |task| thread::Builder::new().spawn(task),
    )
}

/// The spawn capability is explicit so resource exhaustion can be tested
/// without relying on process-wide limits or creating thousands of threads.
pub fn spawn_workers_with<T: Copy + Send + Sync + 'static>(
    ctx: &ConsumerContext<T>,
    topic_ids: &[TopicId],
    count: usize,
    threads: usize,
    target: DrainTarget<T>,
    mut spawn: impl FnMut(Box<dyn FnOnce() + Send>) -> io::Result<thread::JoinHandle<()>>,
) -> io::Result<ConsumerHandle> {
    if count == 0 || threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "consumer and thread counts must be positive",
        ));
    }
    // Incremented once per thread when it exits the drain loop
    let done = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let (messages_per_consumer, is_terminal) = match target {
        DrainTarget::Messages(messages) => (messages, None),
        DrainTarget::Terminal(predicate) => (0, Some(predicate)),
    };

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

    // Each worker waits for its assignment. Keeping successful workers alive
    // makes later spawn failures reflect the actual concurrent resource limit.
    let mut assignments = Vec::new();
    let mut thread_handles = Vec::new();
    let counter = Arc::new(AtomicU64::new(0));

    for _ in 0..count.min(threads) {
        let (send, receive) = mpsc::channel::<(Vec<ConsumerClient<T>>, Arc<Barrier>)>();
        let ctr = Arc::clone(&counter);
        let done_flag = Arc::clone(&done);
        let stop_flag = Arc::clone(&stop);

        let task = Box::new(move || {
            if let Ok((batch, barrier)) = receive.recv() {
                let messages_expected = batch.len() as u64 * messages_per_consumer;
                drain_loop_until(
                    batch,
                    barrier,
                    messages_expected,
                    ctr,
                    stop_flag,
                    is_terminal,
                );
                done_flag.fetch_add(1, Ordering::Release);
            }
        });
        match spawn(task) {
            Ok(handle) => {
                assignments.push(send);
                thread_handles.push(handle);
            }
            Err(error) if thread_handles.is_empty() => return Err(error),
            Err(_) => break,
        }
    }

    let actual_threads = thread_handles.len();
    let barrier = Arc::new(Barrier::new(actual_threads + 1));
    let clients_per_thread = count / actual_threads;
    let remainder = count % actual_threads;
    for (t, send) in assignments.into_iter().enumerate() {
        let batch_size = clients_per_thread + usize::from(t < remainder);
        let batch = all_clients.drain(..batch_size).collect();
        assert!(
            send.send((batch, Arc::clone(&barrier))).is_ok(),
            "consumer thread exited before assignment"
        );
    }

    Ok(ConsumerHandle {
        barrier,
        done,
        counter,
        thread_count: actual_threads as u64,
        stop,
        threads: thread_handles,
    })
}

/// Per-thread poll loop. Waits on `barrier`, then polls all owned clients in
/// round-robin until this worker receives `messages_expected` or `stop` is set.
/// Adds its local delivery count to the shared aggregate once, on exit.
pub fn drain_loop<T: Copy + Send + Sync + 'static>(
    clients: Vec<ConsumerClient<T>>,
    barrier: Arc<Barrier>,
    messages_expected: u64,
    counter: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
) {
    drain_loop_until(clients, barrier, messages_expected, counter, stop, None);
}

fn drain_loop_until<T: Copy + Send + Sync + 'static>(
    mut clients: Vec<ConsumerClient<T>>,
    barrier: Arc<Barrier>,
    messages_expected: u64,
    counter: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    is_terminal: Option<fn(&T) -> bool>,
) {
    let mut finished = vec![false; clients.len()];
    let mut delivered = 0;
    barrier.wait();
    if clients.is_empty() {
        return;
    }
    'drain: loop {
        if is_terminal.is_none() && delivered >= messages_expected {
            break;
        }
        if stop.load(Ordering::Acquire) {
            break;
        }
        for (client, finished) in clients.iter_mut().zip(&mut finished) {
            if *finished {
                continue;
            }
            if let Some(msg) = client.poll() {
                std::hint::black_box(msg);
                delivered += 1;
                if let Some(is_terminal) = is_terminal {
                    *finished = is_terminal(msg.message);
                } else if delivered >= messages_expected {
                    break 'drain;
                }
            }
        }
        if is_terminal.is_some() && finished.iter().all(|&done| done) {
            break;
        }
    }
    counter.fetch_add(delivered, Ordering::Relaxed);
}

/// Produces a deterministic non-zero fill pattern for `FixedBytes<N>`.
/// Cycles through 0x01..=0xFF so there are no zero bytes.
pub fn fill_fixed_bytes<const N: usize>() -> FixedBytes<N> {
    let mut data = [0u8; N];
    for (i, byte) in data.iter_mut().enumerate() {
        *byte = ((i % 255) + 1) as u8;
    }
    FixedBytes::new(&data).expect("fill_fixed_bytes: slice fits in N")
}

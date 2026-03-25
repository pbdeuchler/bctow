// Allocation-counting integration tests for bctow steady-state hot paths.
//
// The counting allocator uses thread-local storage so each test thread tracks
// only its own allocations, making the tests safe to run with multiple threads.
// For the most reliable results, run with a single thread:
//
//   cargo test --all-features --test alloc_tests -- --test-threads=1
//
// This file is intentionally separate from integration.rs so that the custom
// global allocator does not affect other test suites.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::mem::MaybeUninit;

use bctow::{BctowConfig, ExactTopic};

// ---------------------------------------------------------------------------
// Thread-local counting allocator
// ---------------------------------------------------------------------------

thread_local! {
    static THREAD_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    static THREAD_COUNTING: Cell<bool> = const { Cell::new(false) };
}

struct CountingAlloc;

impl CountingAlloc {
    /// Begin counting allocations on this thread. Resets the per-thread counter.
    fn start_counting() {
        THREAD_ALLOC_COUNT.with(|c| c.set(0));
        THREAD_COUNTING.with(|c| c.set(true));
    }

    /// Stop counting and return the number of allocations recorded on this thread.
    fn stop_counting() -> usize {
        THREAD_COUNTING.with(|c| c.set(false));
        THREAD_ALLOC_COUNT.with(|c| c.get())
    }
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let counting = THREAD_COUNTING.try_with(|c| c.get()).unwrap_or(false);
        if counting {
            THREAD_ALLOC_COUNT.with(|c| c.set(c.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn make_broker() -> (bctow::Producer<u64>, bctow::ConsumerContext<u64>) {
    bctow::broker::<u64>(BctowConfig::default())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// publish_id on a warmed producer cache must not allocate in steady state.
#[test]
fn publish_id_steady_state_zero_allocations() {
    let (producer, _ctx) = make_broker();
    let topic_id = producer.register_topic(ExactTopic::new("alloc-pub"));

    // Warm: first publish populates the producer ring cache.
    producer.publish_id(topic_id, &0u64).unwrap();

    CountingAlloc::start_counting();
    for i in 1..=1000u64 {
        producer.publish_id(topic_id, &i).unwrap();
    }
    let n = CountingAlloc::stop_counting();
    assert_eq!(n, 0, "publish_id allocated {n} times in steady state");
}

/// publish + poll cycle on a warmed client must not allocate in steady state.
#[test]
fn poll_steady_state_zero_allocations() {
    let (producer, ctx) = make_broker();
    let topic_id = producer.register_topic(ExactTopic::new("alloc-poll"));

    // Anchor the consumer cursor before subscribe.
    producer.publish_id(topic_id, &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe_id(topic_id).unwrap();

    // Warm: first publish+poll initialises internal scratch state.
    producer.publish_id(topic_id, &1u64).unwrap();
    let _ = client.poll();

    CountingAlloc::start_counting();
    for i in 2..=1000u64 {
        producer.publish_id(topic_id, &i).unwrap();
        let msg = client.poll().unwrap();
        assert_eq!(*msg.message, i);
    }
    let n = CountingAlloc::stop_counting();
    assert_eq!(n, 0, "poll allocated {n} times in steady state");
}

/// publish_id + poll_into cycle on a warmed client must not allocate in steady state.
#[test]
fn poll_into_steady_state_zero_allocations() {
    let (producer, ctx) = make_broker();
    let topic_id = producer.register_topic(ExactTopic::new("alloc-poll-into"));

    // Anchor the consumer cursor before subscribe.
    producer.publish_id(topic_id, &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe_id(topic_id).unwrap();

    // Warm: first publish+poll_into initialises internal scratch state.
    producer.publish_id(topic_id, &1u64).unwrap();
    let mut dst = MaybeUninit::<u64>::uninit();
    let _ = client.poll_into(&mut dst);

    CountingAlloc::start_counting();
    for i in 2..=1000u64 {
        producer.publish_id(topic_id, &i).unwrap();
        let returned_id = client.poll_into(&mut dst).unwrap();
        assert_eq!(returned_id, topic_id);
        assert_eq!(unsafe { dst.assume_init() }, i);
    }
    let n = CountingAlloc::stop_counting();
    assert_eq!(n, 0, "poll_into allocated {n} times in steady state");
}

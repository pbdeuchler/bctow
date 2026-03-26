# bctow

Buffalo Chicken Tender on White, now as a generic typed topic pub/sub library for Rust.

## What changed

- Messages are generic typed values, not byte slices, so the compiler ensures type safety across publish and subscribe.
- Topic ids are broker-branded, so a `TopicId` from one broker is rejected by another.
- Topic rings preallocate slot storage and copy typed values into those slots instead of allocating an `Arc` per message.
- Consumers copy into an internal scratch buffer with `poll()` or a caller-provided `MaybeUninit<T>` with `poll_into()`.

## Quick example

```rust
use bctow::{broker, BctowConfig};

fn main() -> Result<(), bctow::BctowError> {
    let (producer, ctx) = broker::<u64>(BctowConfig::default());

    let topic = producer.register_topic("events/sensor");
    producer.publish_id(topic, &42u64)?;

    let mut client = ctx.create_client();
    client.subscribe_id(topic)?;

    producer.publish("events/sensor", &43u64)?;

    let message = client.poll().unwrap();
    assert_eq!(*message.message, 43u64);
    Ok(())
}
```

Run the example with:

```bash
cargo run --all-features --example basic
```

## Design notes

- One producer, many consumers.
- One shared fixed-capacity ring per topic.
- No broker lock on steady-state `publish_id` after the producer caches the topic.
- No allocations in steady-state publish or receive paths.
- `publish` and `publish_id` return `Result<u64, BctowError>` where the `u64` is the slot sequence number.
- `ConsumerClient` supports `subscribe`/`subscribe_id`, `unsubscribe(TopicId)`, and `unsubscribe_by_name`.
- Both `Producer` and `ConsumerContext` expose `topic_name(TopicId) -> Option<String>`.
- Overflow policies: `DropOldest`, `DropNewest`, `SkipToLatest`.

## Variable-length byte payloads

`FixedBytes<N>` stores up to `N` bytes inline and is `Copy`, so it works as `T`
in `broker::<FixedBytes<N>>()`. This preserves byte-oriented semantics without
reintroducing dynamic slot sizing or per-message allocation.

```rust
use bctow::{broker, BctowConfig, FixedBytes};

let (producer, ctx) = broker::<FixedBytes<64>>(BctowConfig::default());
producer.publish("events/raw", &FixedBytes::<64>::new(b"hello").unwrap()).unwrap();
```

## Benchmarks

Criterion benchmarks live in `benches/pubsub.rs`. Run them with:

```bash
cargo bench --all-features
```

Run a specific group:

```bash
cargo bench --all-features -- publish_throughput
```

Benchmark groups:

| Group | What it measures |
|-------|-----------------|
| `publish_throughput` | Publish latency across message sizes (8B to 4096B) |
| `consumer_scaling` | Publish+poll with 1/2/4/8/16 consumers |
| `topic_scaling` | Round-robin publish+poll across 1/4/16/64 topics |
| `pathological` | Tiny ring overflow, 256-topic fan-out, subscribe churn, SkipToLatest overflow |
| `publish_path` | `publish_id` (cached ring) vs `publish` (string lookup) |

CI uses [Bencher](https://bencher.dev) to track the main branch baseline and detect regressions on PRs. Requires a `BENCHER_API_TOKEN` repository secret.

## Optional feature

- `metrics`: exposes `ConsumerContext::metrics()` with relaxed-atomic counters.

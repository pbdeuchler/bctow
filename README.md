# bctow

Buffalo Chicken Tender on White, now as a generic typed topic pub/sub library for Rust.

## What changed

- Messages are generic typed values, not byte slices, so the compiler ensures type safety across publish and subscribe.
- Topic ids are broker-branded, so a `TopicId` from one broker is rejected by another.
- Topic rings preallocate slot storage and copy typed values into those slots instead of allocating an `Arc` per message.
- Consumers copy into an internal scratch buffer with `poll()` or a caller-provided buffer with `poll_into()`.

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
cargo run --example basic
```

## Design notes

- One producer, many consumers.
- One shared fixed-capacity ring per topic.
- No broker lock on steady-state `publish_id` after the producer caches the topic.
- No allocations in steady-state publish or receive paths.
- Overflow policies: `DropOldest`, `DropNewest`, `SkipToLatest`.

## Optional feature

- `metrics`: exposes `ConsumerContext::metrics()` with relaxed-atomic counters.

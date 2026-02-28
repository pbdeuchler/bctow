# bctow

Buffalo Chicken Tender on White — a high-performance, in-process, topic-based pub/sub library for Rust.

## Key features

- **Single producer, multiple consumers** — one writer thread, any number of reader threads, no runtime mutex on the publish hot path.
- **Per-topic ring buffers** — slots pre-allocated at topic creation; steady-state publish cost is a single `Arc` allocation per message.
- **Lock-free reads** — consumers read from the ring using `Acquire`/`Release` atomics.
- **Per-client cursors** — each `ConsumerClient` tracks its own position; consumers never block each other.
- **Round-robin fairness** — `poll` rotates across subscribed topics so a high-throughput topic cannot starve others.
- **Three overflow policies** — `DropOldest` (default), `DropNewest`, `SkipToLatest`.
- **No async runtime required** — pure `std`, works in any threading model.

## Design constraints

- **One producer per broker.** `Producer` is `Send` but `!Sync`; the compiler prevents concurrent `publish` calls.
- **Bounded memory.** Ring buffer capacity is fixed at creation; slow consumers experience the configured overflow policy rather than back-pressuring the producer.
- **Topics created on first publish.** A `subscribe` call to a never-published topic returns `BctowError::TopicNotFound`.
- **New subscribers see only future messages.** Cursors start at `latest + 1`; historical messages within the ring window are not replayed.

## Quick example

```rust
use bctow::{BctowConfig, broker};

fn main() {
    let config = BctowConfig::default();

    // broker() returns a matched Producer + ConsumerContext pair.
    let (producer, ctx) = broker::<String>(config);

    // Publish to two topics — topics are created on first publish.
    for i in 0..4u32 {
        producer.publish("sensors/temperature", format!("temp={i}"));
        producer.publish("sensors/humidity",    format!("hum={i}"));
    }

    // Create a consumer client and subscribe.
    let mut client = ctx.create_client();
    client.subscribe("sensors/temperature").unwrap();
    client.subscribe("sensors/humidity").unwrap();

    // Publish more messages after subscribing.
    producer.publish("sensors/temperature", "temp=after".to_owned());
    producer.publish("sensors/humidity",    "hum=after".to_owned());

    // poll() returns (TopicId, Arc<T>) and rotates across topics for fairness.
    while let Some((topic_id, msg)) = client.poll() {
        println!("{topic_id:?}: {msg}");
    }

    // Unsubscribe at any time.
    client.unsubscribe_by_name("sensors/humidity");
}
```

Run the full annotated example with:

```bash
cargo run --all-features --example basic
```

## Overflow policies

| Policy         | Behaviour when consumer lags                          |
|----------------|-------------------------------------------------------|
| `DropOldest`   | Skip cursor to oldest available message (default)     |
| `DropNewest`   | Cursor stays in place; consumer becomes stuck and receives no further messages until re-subscribed |
| `SkipToLatest` | Jump cursor past latest message; skip entire backlog  |

## Optional features

- `metrics` — exposes `ConsumerContext::metrics()` returning `Metrics { messages_published, messages_delivered }`.

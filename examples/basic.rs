//! Basic bctow usage example.
//!
//! Demonstrates:
//!   - Creating a broker with a custom config
//!   - Publishing messages on multiple topics
//!   - Capturing the sequence number returned by publish
//!   - Using publish_id for hot-path publishing (skip per-call string interning)
//!   - Creating consumer clients from a ConsumerContext
//!   - Subscribing to topics
//!   - Polling for messages with topic_name resolution
//!   - Dynamic subscribe/unsubscribe
//!   - Round-robin fairness across subscribed topics

use bctow::{BctowConfig, OverflowPolicy, broker};

fn main() {
    // -------------------------------------------------------------------
    // 1. Build a broker.
    //
    // `broker()` returns a matched `(Producer<T>, ConsumerContext<T>)` pair
    // that share the same underlying ring-buffer state.  The Producer is
    // `Send` but NOT `Sync` — exactly one thread may call `publish` at a
    // time.  The ConsumerContext is freely `Clone` and can be handed to as
    // many consumer threads as needed.
    // -------------------------------------------------------------------
    let config = BctowConfig {
        ring_buffer_capacity: 16,          // 16 slots per topic
        overflow_policy: OverflowPolicy::DropOldest,
        ..BctowConfig::default()
    };

    let (producer, ctx) = broker::<String>(config);

    // -------------------------------------------------------------------
    // 2. Publish messages on multiple topics.
    //
    // Topics are created on first publish — no prior declaration needed.
    // `publish` returns the sequence number assigned to the message.
    // -------------------------------------------------------------------
    for i in 0..6u32 {
        let seq = producer.publish("sensors/temperature", format!("temp={i}"));
        if i == 0 {
            println!("First temperature message assigned sequence number: {seq}");
        }
        producer.publish("sensors/humidity", format!("hum={i}"));
    }

    // -------------------------------------------------------------------
    // 2b. publish_id — hot-path publishing without per-call string interning.
    //
    // Intern the topic string once to get a TopicId, then use publish_id
    // on every subsequent call.  This avoids the string lookup on each
    // publish and is the preferred pattern for high-throughput paths.
    // -------------------------------------------------------------------
    let temp_id = producer.intern_topic("sensors/temperature");
    let seq = producer.publish_id(temp_id, "temp=via-id".to_owned());
    println!("Published via publish_id (topic={temp_id:?}), sequence={seq}");

    // -------------------------------------------------------------------
    // 3. Create consumer clients.
    //
    // Each `ConsumerClient` maintains its own per-topic cursors and is
    // NOT Clone or Sync — it is owned by a single consumer thread.
    // -------------------------------------------------------------------
    let mut client_a = ctx.create_client();
    let mut client_b = ctx.create_client();

    // -------------------------------------------------------------------
    // 4. Publish more messages so the clients have something to read.
    // -------------------------------------------------------------------
    for i in 6..10u32 {
        producer.publish("sensors/temperature", format!("temp={i}"));
        producer.publish("sensors/humidity",    format!("hum={i}"));
    }

    // -------------------------------------------------------------------
    // 5. Subscribe to topics.
    //
    // `subscribe` resolves the topic string to a TopicId and records a
    // cursor starting at the current head of the ring at subscribe time.
    // Subscribing to a topic that has never been published to returns
    // `BctowError::TopicNotFound`.
    //
    // client_a follows both topics; client_b follows only temperature.
    // -------------------------------------------------------------------
    client_a.subscribe("sensors/temperature").expect("topic must exist");
    client_a.subscribe("sensors/humidity").expect("topic must exist");

    client_b.subscribe("sensors/temperature").expect("topic must exist");

    // -------------------------------------------------------------------
    // 6. Publish a few more messages after subscribing so both clients
    //    will receive them.
    // -------------------------------------------------------------------
    producer.publish("sensors/temperature", "temp=post-sub-0".to_owned());
    producer.publish("sensors/humidity",    "hum=post-sub-0".to_owned());
    producer.publish("sensors/temperature", "temp=post-sub-1".to_owned());
    producer.publish("sensors/humidity",    "hum=post-sub-1".to_owned());

    // -------------------------------------------------------------------
    // 7. Poll for messages.
    //
    // `poll` returns `Some((TopicId, Arc<T>))` for the next available
    // message across all subscribed topics, or `None` when the client is
    // fully caught up.
    //
    // Use `ctx.topic_name(id)` to resolve a TopicId back to its string for
    // display or logging — do not call this on the hot path.
    //
    // The polling order rotates across topics on each call so that a
    // high-throughput topic cannot starve a low-throughput one.
    // -------------------------------------------------------------------
    println!("\n=== client_a (temperature + humidity) ===");
    while let Some((topic_id, msg)) = client_a.poll() {
        let name = ctx.topic_name(topic_id).unwrap_or_else(|| format!("{topic_id:?}"));
        println!("  topic={name}  msg={msg}");
    }

    println!("\n=== client_b (temperature only) ===");
    while let Some((topic_id, msg)) = client_b.poll() {
        let name = ctx.topic_name(topic_id).unwrap_or_else(|| format!("{topic_id:?}"));
        println!("  topic={name}  msg={msg}");
    }

    // -------------------------------------------------------------------
    // 8. Round-robin fairness demonstration.
    //
    // Publish an equal number of messages to each topic, then drain with
    // a single client subscribed to both.  The messages will interleave
    // rather than being delivered in topic-first batches.
    // -------------------------------------------------------------------
    println!("\n=== round-robin fairness demonstration ===");

    let mut fair_client = ctx.create_client();
    fair_client.subscribe("sensors/temperature").expect("topic must exist");
    fair_client.subscribe("sensors/humidity").expect("topic must exist");

    for i in 0..4u32 {
        producer.publish("sensors/temperature", format!("fair-temp={i}"));
        producer.publish("sensors/humidity",    format!("fair-hum={i}"));
    }

    // Drain and track which topic was served on each call.  Because the
    // poll index rotates, consecutive messages will alternate topics.
    let mut prev_topic: Option<bctow::TopicId> = None;
    let mut alternations = 0usize;
    while let Some((topic_id, msg)) = fair_client.poll() {
        if let Some(prev) = prev_topic {
            if prev != topic_id {
                alternations += 1;
            }
        }
        prev_topic = Some(topic_id);
        let name = ctx.topic_name(topic_id).unwrap_or_else(|| format!("{topic_id:?}"));
        println!("  topic={name}  msg={msg}");
    }
    println!("  topic alternations: {alternations}  (> 0 confirms round-robin)");

    // -------------------------------------------------------------------
    // 9. Dynamic unsubscribe.
    //
    // Remove a subscription at any time.  The client's cursor for that
    // topic is discarded; a future re-subscribe starts at the current head.
    // -------------------------------------------------------------------
    println!("\n=== dynamic unsubscribe ===");

    let mut dyn_client = ctx.create_client();
    dyn_client.subscribe("sensors/temperature").expect("topic must exist");
    dyn_client.subscribe("sensors/humidity").expect("topic must exist");

    producer.publish("sensors/temperature", "before-unsub".to_owned());
    producer.publish("sensors/humidity",    "before-unsub".to_owned());

    // Read one message to show both subscriptions are active.
    if let Some((id, msg)) = dyn_client.poll() {
        let name = ctx.topic_name(id).unwrap_or_else(|| format!("{id:?}"));
        println!("  before unsubscribe: topic={name}  msg={msg}");
    }

    // Unsubscribe from humidity by name.
    dyn_client.unsubscribe_by_name("sensors/humidity");

    producer.publish("sensors/temperature", "after-unsub-temp".to_owned());
    producer.publish("sensors/humidity",    "after-unsub-hum".to_owned());

    // Only temperature messages should appear now.
    while let Some((id, msg)) = dyn_client.poll() {
        let name = ctx.topic_name(id).unwrap_or_else(|| format!("{id:?}"));
        println!("  after unsubscribe:  topic={name}  msg={msg}");
    }

    // -------------------------------------------------------------------
    // 10. Subscribing to a topic that has never been published to returns
    //     an error — topics are registered on first publish.
    // -------------------------------------------------------------------
    let mut error_client = ctx.create_client();
    let err = error_client.subscribe("does/not/exist");
    println!("\n=== expected error: {} ===", err.unwrap_err());
}

use std::thread;

use bctow::{
    broker, BctowConfig, BctowError, ConsumerClient, ConsumerContext, ExactTopic, OverflowPolicy,
    Producer, TopicId,
};

fn default_broker() -> (Producer, ConsumerContext) {
    broker(BctowConfig::default())
}

fn small_broker(
    capacity: usize,
    max_payload_size: usize,
    policy: OverflowPolicy,
) -> (Producer, ConsumerContext) {
    broker(BctowConfig {
        max_payload_size,
        ring_buffer_capacity: capacity,
        overflow_policy: policy,
    })
}

fn encode_u64(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn decode_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("payload must be 8 bytes"))
}

fn drain(client: &mut ConsumerClient) -> Vec<(TopicId, Vec<u8>)> {
    let mut out = Vec::new();
    while let Some(message) = client.poll() {
        out.push((message.topic_id, message.payload.to_vec()));
    }
    out
}

fn drain_u64(client: &mut ConsumerClient) -> Vec<(TopicId, u64)> {
    drain(client)
        .into_iter()
        .map(|(topic_id, payload)| (topic_id, decode_u64(&payload)))
        .collect()
}

#[test]
fn single_producer_single_consumer_order_preserved() {
    let (producer, ctx) = default_broker();
    producer.publish("ordered", encode_u64(0)).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("ordered").unwrap();

    for value in 1..=5 {
        producer.publish("ordered", encode_u64(value)).unwrap();
    }

    let values: Vec<u64> = drain_u64(&mut client)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn publish_id_path_uses_registered_topic() {
    let (producer, ctx) = default_broker();
    let topic_id = producer.register_topic(ExactTopic::new("events"));

    let seq1 = producer.publish_id(topic_id, encode_u64(10)).unwrap();
    let seq2 = producer.publish_id(topic_id, encode_u64(20)).unwrap();
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);

    let mut client = ctx.create_client();
    client.subscribe_id(topic_id).unwrap();

    producer.publish_id(topic_id, encode_u64(30)).unwrap();
    producer.publish_id(topic_id, encode_u64(40)).unwrap();

    let values: Vec<u64> = drain_u64(&mut client)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    assert_eq!(values, vec![30, 40]);
}

#[test]
fn publish_returns_increasing_sequence_numbers() {
    let (producer, _ctx) = default_broker();
    let seqs: Vec<u64> = (1..=5)
        .map(|value| producer.publish("seq-test", encode_u64(value)).unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

#[test]
fn single_producer_many_consumers_all_receive_independently() {
    const MSG_COUNT: u64 = 20;
    const NUM_CONSUMERS: usize = 4;

    let (producer, ctx) = default_broker();
    producer.publish("broadcast", encode_u64(0)).unwrap();

    let clients: Vec<_> = (0..NUM_CONSUMERS)
        .map(|_| {
            let mut client = ctx.create_client();
            client.subscribe("broadcast").unwrap();
            client
        })
        .collect();

    for value in 1..=MSG_COUNT {
        producer.publish("broadcast", encode_u64(value)).unwrap();
    }

    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            thread::spawn(move || {
                drain_u64(&mut client)
                    .into_iter()
                    .map(|(_, value)| value)
                    .collect::<Vec<u64>>()
            })
        })
        .collect();

    for handle in handles {
        assert_eq!(handle.join().unwrap(), (1..=MSG_COUNT).collect::<Vec<_>>());
    }
}

#[test]
fn cloned_context_creates_independent_clients() {
    let (producer, ctx) = default_broker();
    producer.publish("clone-test", encode_u64(0)).unwrap();

    let ctx2 = ctx.clone();
    let mut a = ctx.create_client();
    let mut b = ctx2.create_client();

    a.subscribe("clone-test").unwrap();
    b.subscribe("clone-test").unwrap();

    producer.publish("clone-test", encode_u64(1)).unwrap();
    producer.publish("clone-test", encode_u64(2)).unwrap();

    let a_values: Vec<u64> = drain_u64(&mut a)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    let b_values: Vec<u64> = drain_u64(&mut b)
        .into_iter()
        .map(|(_, value)| value)
        .collect();

    assert_eq!(a_values, vec![1, 2]);
    assert_eq!(b_values, vec![1, 2]);
}

#[test]
fn dynamic_subscribe_unsubscribe_works() {
    let (producer, ctx) = default_broker();
    producer.publish("a", b"a-prime").unwrap();
    producer.publish("b", b"b-prime").unwrap();

    let mut client = ctx.create_client();
    client.subscribe("a").unwrap();

    producer.publish("a", b"a1").unwrap();
    producer.publish("b", b"b1").unwrap();
    producer.publish("a", b"a2").unwrap();

    let phase1 = drain(&mut client);
    let phase1_payloads: Vec<Vec<u8>> = phase1.iter().map(|(_, payload)| payload.clone()).collect();
    assert_eq!(phase1_payloads, vec![b"a1".to_vec(), b"a2".to_vec()]);

    let id_a = phase1[0].0;
    client.subscribe("b").unwrap();

    producer.publish("a", b"a3").unwrap();
    producer.publish("b", b"b2").unwrap();
    let phase2 = drain(&mut client);
    assert!(phase2.iter().any(|(topic, _)| *topic == id_a));
    let id_b = phase2
        .iter()
        .find(|(topic, _)| *topic != id_a)
        .map(|(topic, _)| *topic)
        .unwrap();

    client.unsubscribe(id_a);
    producer.publish("a", b"a4").unwrap();
    producer.publish("b", b"b3").unwrap();

    let phase3 = drain(&mut client);
    assert!(phase3.iter().all(|(topic, _)| *topic == id_b));
    assert_eq!(phase3[0].1, b"b3");
}

#[test]
fn unsubscribe_by_name_unknown_topic_is_noop() {
    let (_producer, ctx) = default_broker();
    let mut client = ctx.create_client();
    client.unsubscribe_by_name("never-registered");
}

#[test]
fn overflow_drop_oldest_skips_to_oldest_available() {
    let (producer, ctx) = small_broker(4, 8, OverflowPolicy::DropOldest);
    producer.publish("lag", encode_u64(0)).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("lag").unwrap();

    for value in 1..=8 {
        producer.publish("lag", encode_u64(value)).unwrap();
    }

    let message = client.poll().unwrap();
    assert!(decode_u64(message.payload) >= 5);
}

#[test]
fn overflow_skip_to_latest_receives_only_future_messages() {
    let (producer, ctx) = small_broker(4, 8, OverflowPolicy::SkipToLatest);
    producer.publish("skip", encode_u64(0)).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("skip").unwrap();

    for value in 1..=8 {
        producer.publish("skip", encode_u64(value)).unwrap();
    }

    assert!(client.poll().is_none());

    producer.publish("skip", encode_u64(99)).unwrap();
    producer.publish("skip", encode_u64(100)).unwrap();

    let values: Vec<u64> = drain_u64(&mut client)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    assert_eq!(values, vec![99, 100]);
}

#[test]
fn overflow_drop_newest_leaves_consumer_stuck() {
    let (producer, ctx) = small_broker(4, 8, OverflowPolicy::DropNewest);
    producer.publish("drop", encode_u64(0)).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("drop").unwrap();

    for value in 1..=8 {
        producer.publish("drop", encode_u64(value)).unwrap();
    }

    for _ in 0..5 {
        assert!(client.poll().is_none());
    }
}

#[test]
fn payload_too_large_returns_error() {
    let (producer, _ctx) = small_broker(4, 4, OverflowPolicy::DropOldest);
    let err = producer.publish("topic", b"hello").unwrap_err();
    assert_eq!(err, BctowError::PayloadTooLarge { size: 5, max: 4 });
}

#[test]
fn subscribe_nonexistent_topic_returns_topic_not_found() {
    let (_producer, ctx) = default_broker();
    let mut client = ctx.create_client();
    let err = client.subscribe("missing").unwrap_err();
    assert!(matches!(err, BctowError::TopicNotFound(_)));
}

#[test]
fn foreign_topic_ids_are_rejected() {
    let (producer_a, _ctx_a) = default_broker();
    let topic_id = producer_a.register_topic("shared");

    let (producer_b, ctx_b) = default_broker();
    let err = producer_b.publish_id(topic_id, b"payload").unwrap_err();
    assert_eq!(err, BctowError::TopicBrokerMismatch(topic_id));

    let mut client = ctx_b.create_client();
    let err = client.subscribe_id(topic_id).unwrap_err();
    assert_eq!(err, BctowError::TopicBrokerMismatch(topic_id));
}

#[test]
fn poll_with_no_subscriptions_returns_none() {
    let (_producer, ctx) = default_broker();
    let mut client = ctx.create_client();
    assert!(client.poll().is_none());
}

#[test]
fn subscribe_twice_does_not_duplicate_messages() {
    let (producer, ctx) = default_broker();
    producer.publish("idem", encode_u64(0)).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("idem").unwrap();
    client.subscribe("idem").unwrap();

    producer.publish("idem", encode_u64(1)).unwrap();
    producer.publish("idem", encode_u64(2)).unwrap();

    let values: Vec<u64> = drain_u64(&mut client)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    assert_eq!(values, vec![1, 2]);
}

#[test]
fn poll_into_reports_small_buffers() {
    let (producer, ctx) = small_broker(4, 8, OverflowPolicy::DropOldest);
    producer.publish("topic", b"prime").unwrap();

    let mut client = ctx.create_client();
    client.subscribe("topic").unwrap();
    producer.publish("topic", b"payload").unwrap();

    let mut dst = [0u8; 4];
    let err = client.poll_into(&mut dst).unwrap_err();
    assert_eq!(
        err,
        BctowError::ReceiveBufferTooSmall {
            required: 7,
            available: 4
        }
    );
}

#[cfg(feature = "metrics")]
mod metrics_tests {
    use super::*;

    #[test]
    fn metrics_track_published_and_delivered_messages() {
        let (producer, ctx) = default_broker();
        producer.publish("metrics", b"prime").unwrap();

        let mut client = ctx.create_client();
        client.subscribe("metrics").unwrap();

        producer.publish("metrics", b"a").unwrap();
        producer.publish("metrics", b"b").unwrap();
        let delivered = drain(&mut client);

        let metrics = ctx.metrics();
        assert_eq!(metrics.messages_published, 3);
        assert_eq!(metrics.messages_delivered, delivered.len() as u64);
    }
}

#[test]
fn multithreaded_producer_and_consumers_no_panics() {
    const MSG_COUNT: u64 = 200;

    let (producer, ctx) = small_broker(512, 8, OverflowPolicy::DropOldest);
    producer.publish("shared", encode_u64(0)).unwrap();

    let clients: Vec<_> = (0..4)
        .map(|_| {
            let mut client = ctx.create_client();
            client.subscribe("shared").unwrap();
            client
        })
        .collect();

    let producer_handle = thread::spawn(move || {
        for value in 1..=MSG_COUNT {
            producer.publish("shared", encode_u64(value)).unwrap();
        }
    });

    producer_handle.join().unwrap();

    for mut client in clients {
        let values: Vec<u64> = drain_u64(&mut client)
            .into_iter()
            .map(|(_, value)| value)
            .collect();
        assert!(!values.is_empty());
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

#[test]
fn multithreaded_truly_concurrent_producer_and_consumers() {
    const MSG_COUNT: u64 = 500;

    let (producer, ctx) = small_broker(1024, 8, OverflowPolicy::DropOldest);
    producer.publish("concurrent", encode_u64(0)).unwrap();

    let clients: Vec<_> = (0..4)
        .map(|_| {
            let mut client = ctx.create_client();
            client.subscribe("concurrent").unwrap();
            client
        })
        .collect();

    let consumer_handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            thread::spawn(move || {
                let mut received = Vec::new();
                let mut attempts = 0usize;

                while received.len() < MSG_COUNT as usize && attempts < 100_000 {
                    if let Some(message) = client.poll() {
                        received.push(decode_u64(message.payload));
                    } else {
                        attempts += 1;
                        thread::yield_now();
                    }
                }

                received
            })
        })
        .collect();

    let producer_handle = thread::spawn(move || {
        for value in 1..=MSG_COUNT {
            producer.publish("concurrent", encode_u64(value)).unwrap();
        }
    });

    producer_handle.join().unwrap();

    for handle in consumer_handles {
        let values = handle.join().unwrap();
        assert!(!values.is_empty());
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

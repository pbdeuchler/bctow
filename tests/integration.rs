use std::mem::MaybeUninit;
use std::thread;

use bctow::{
    broker, BctowConfig, BctowError, ConsumerClient, ConsumerContext, ExactTopic, FixedBytes,
    OverflowPolicy, Producer, TopicId,
};

#[derive(Copy, Clone, Debug, PartialEq)]
struct SensorReading {
    sensor_id: u32,
    value: f64,
    timestamp: u64,
}

fn default_broker() -> (Producer<u64>, ConsumerContext<u64>) {
    broker::<u64>(BctowConfig::default())
}

fn small_broker(
    capacity: usize,
    policy: OverflowPolicy,
) -> (Producer<u64>, ConsumerContext<u64>) {
    broker::<u64>(BctowConfig {
        ring_buffer_capacity: capacity,
        overflow_policy: policy,
    })
}

fn drain(client: &mut ConsumerClient<u64>) -> Vec<(TopicId, u64)> {
    let mut out = Vec::new();
    while let Some(message) = client.poll() {
        out.push((message.topic_id, *message.message));
    }
    out
}

#[test]
fn single_producer_single_consumer_order_preserved() {
    let (producer, ctx) = default_broker();
    producer.publish("ordered", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("ordered").unwrap();

    for value in 1u64..=5 {
        producer.publish("ordered", &value).unwrap();
    }

    let values: Vec<u64> = drain(&mut client)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    assert_eq!(values, vec![1, 2, 3, 4, 5]);
}

#[test]
fn publish_id_path_uses_registered_topic() {
    let (producer, ctx) = default_broker();
    let topic_id = producer.register_topic(ExactTopic::new("events"));

    let seq1 = producer.publish_id(topic_id, &10u64).unwrap();
    let seq2 = producer.publish_id(topic_id, &20u64).unwrap();
    assert_eq!(seq1, 1);
    assert_eq!(seq2, 2);

    let mut client = ctx.create_client();
    client.subscribe_id(topic_id).unwrap();

    producer.publish_id(topic_id, &30u64).unwrap();
    producer.publish_id(topic_id, &40u64).unwrap();

    let values: Vec<u64> = drain(&mut client)
        .into_iter()
        .map(|(_, value)| value)
        .collect();
    assert_eq!(values, vec![30, 40]);
}

#[test]
fn publish_returns_increasing_sequence_numbers() {
    let (producer, _ctx) = default_broker();
    let seqs: Vec<u64> = (1u64..=5)
        .map(|value| producer.publish("seq-test", &value).unwrap())
        .collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
}

#[test]
fn single_producer_many_consumers_all_receive_independently() {
    const MSG_COUNT: u64 = 20;
    const NUM_CONSUMERS: usize = 4;

    let (producer, ctx) = default_broker();
    producer.publish("broadcast", &0u64).unwrap();

    let clients: Vec<_> = (0..NUM_CONSUMERS)
        .map(|_| {
            let mut client = ctx.create_client();
            client.subscribe("broadcast").unwrap();
            client
        })
        .collect();

    for value in 1u64..=MSG_COUNT {
        producer.publish("broadcast", &value).unwrap();
    }

    let handles: Vec<_> = clients
        .into_iter()
        .map(|mut client| {
            thread::spawn(move || {
                drain(&mut client)
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
    producer.publish("clone-test", &0u64).unwrap();

    let ctx2 = ctx.clone();
    let mut a = ctx.create_client();
    let mut b = ctx2.create_client();

    a.subscribe("clone-test").unwrap();
    b.subscribe("clone-test").unwrap();

    producer.publish("clone-test", &1u64).unwrap();
    producer.publish("clone-test", &2u64).unwrap();

    let a_values: Vec<u64> = drain(&mut a).into_iter().map(|(_, v)| v).collect();
    let b_values: Vec<u64> = drain(&mut b).into_iter().map(|(_, v)| v).collect();

    assert_eq!(a_values, vec![1, 2]);
    assert_eq!(b_values, vec![1, 2]);
}

#[test]
fn dynamic_subscribe_unsubscribe_works() {
    let (producer, ctx) = default_broker();
    producer.publish("a", &0u64).unwrap();
    producer.publish("b", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("a").unwrap();

    producer.publish("a", &1u64).unwrap();
    producer.publish("b", &10u64).unwrap();
    producer.publish("a", &2u64).unwrap();

    let phase1 = drain(&mut client);
    let phase1_values: Vec<u64> = phase1.iter().map(|(_, v)| *v).collect();
    assert_eq!(phase1_values, vec![1, 2]);

    let id_a = phase1[0].0;
    client.subscribe("b").unwrap();

    producer.publish("a", &3u64).unwrap();
    producer.publish("b", &20u64).unwrap();
    let phase2 = drain(&mut client);
    assert!(phase2.iter().any(|(topic, _)| *topic == id_a));
    let id_b = phase2
        .iter()
        .find(|(topic, _)| *topic != id_a)
        .map(|(topic, _)| *topic)
        .unwrap();

    client.unsubscribe(id_a);
    producer.publish("a", &4u64).unwrap();
    producer.publish("b", &30u64).unwrap();

    let phase3 = drain(&mut client);
    assert!(phase3.iter().all(|(topic, _)| *topic == id_b));
    assert_eq!(phase3[0].1, 30u64);
}

#[test]
fn unsubscribe_by_name_unknown_topic_is_noop() {
    let (_producer, ctx) = default_broker();
    let mut client = ctx.create_client();
    client.unsubscribe_by_name("never-registered");
}

#[test]
fn overflow_drop_oldest_skips_to_oldest_available() {
    let (producer, ctx) = small_broker(4, OverflowPolicy::DropOldest);
    producer.publish("lag", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("lag").unwrap();

    for value in 1u64..=8 {
        producer.publish("lag", &value).unwrap();
    }

    let message = client.poll().unwrap();
    assert!(*message.message >= 5u64);
}

#[test]
fn overflow_skip_to_latest_receives_only_future_messages() {
    let (producer, ctx) = small_broker(4, OverflowPolicy::SkipToLatest);
    producer.publish("skip", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("skip").unwrap();

    for value in 1u64..=8 {
        producer.publish("skip", &value).unwrap();
    }

    assert!(client.poll().is_none());

    producer.publish("skip", &99u64).unwrap();
    producer.publish("skip", &100u64).unwrap();

    let values: Vec<u64> = drain(&mut client).into_iter().map(|(_, v)| v).collect();
    assert_eq!(values, vec![99, 100]);
}

#[test]
fn overflow_drop_newest_leaves_consumer_stuck() {
    let (producer, ctx) = small_broker(4, OverflowPolicy::DropNewest);
    producer.publish("drop", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("drop").unwrap();

    for value in 1u64..=8 {
        producer.publish("drop", &value).unwrap();
    }

    for _ in 0..5 {
        assert!(client.poll().is_none());
    }
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
    let err = producer_b.publish_id(topic_id, &42u64).unwrap_err();
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
    producer.publish("idem", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("idem").unwrap();
    client.subscribe("idem").unwrap();

    producer.publish("idem", &1u64).unwrap();
    producer.publish("idem", &2u64).unwrap();

    let values: Vec<u64> = drain(&mut client).into_iter().map(|(_, v)| v).collect();
    assert_eq!(values, vec![1, 2]);
}

#[test]
fn poll_into_uses_caller_buffer() {
    let (producer, ctx) = small_broker(4, OverflowPolicy::DropOldest);
    producer.publish("topic", &0u64).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("topic").unwrap();
    producer.publish("topic", &42u64).unwrap();

    let mut dst = MaybeUninit::<u64>::uninit();
    let topic_id = client.poll_into(&mut dst).unwrap();
    assert_eq!(unsafe { dst.assume_init() }, 42u64);
    // topic_id should be valid
    let _ = topic_id;
}

#[cfg(feature = "metrics")]
mod metrics_tests {
    use super::*;

    #[test]
    fn metrics_track_published_and_delivered_messages() {
        let (producer, ctx) = default_broker();
        producer.publish("metrics", &0u64).unwrap();

        let mut client = ctx.create_client();
        client.subscribe("metrics").unwrap();

        producer.publish("metrics", &1u64).unwrap();
        producer.publish("metrics", &2u64).unwrap();
        let delivered = drain(&mut client);

        let metrics = ctx.metrics();
        assert_eq!(metrics.messages_published, 3);
        assert_eq!(metrics.messages_delivered, delivered.len() as u64);
    }
}

#[test]
fn multithreaded_producer_and_consumers_no_panics() {
    const MSG_COUNT: u64 = 200;

    let (producer, ctx) = small_broker(512, OverflowPolicy::DropOldest);
    producer.publish("shared", &0u64).unwrap();

    let clients: Vec<_> = (0..4)
        .map(|_| {
            let mut client = ctx.create_client();
            client.subscribe("shared").unwrap();
            client
        })
        .collect();

    let producer_handle = thread::spawn(move || {
        for value in 1u64..=MSG_COUNT {
            producer.publish("shared", &value).unwrap();
        }
    });

    producer_handle.join().unwrap();

    for mut client in clients {
        let values: Vec<u64> = drain(&mut client).into_iter().map(|(_, v)| v).collect();
        assert!(!values.is_empty());
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

#[test]
fn multithreaded_truly_concurrent_producer_and_consumers() {
    const MSG_COUNT: u64 = 500;

    let (producer, ctx) = small_broker(1024, OverflowPolicy::DropOldest);
    producer.publish("concurrent", &0u64).unwrap();

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
                        received.push(*message.message);
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
        for value in 1u64..=MSG_COUNT {
            producer.publish("concurrent", &value).unwrap();
        }
    });

    producer_handle.join().unwrap();

    for handle in consumer_handles {
        let values = handle.join().unwrap();
        assert!(!values.is_empty());
        assert!(values.windows(2).all(|pair| pair[0] < pair[1]));
    }
}

#[test]
fn custom_copy_struct_publish_and_receive() {
    let (producer, ctx) = broker::<SensorReading>(BctowConfig::default());

    let seed = SensorReading {
        sensor_id: 0,
        value: 0.0,
        timestamp: 0,
    };
    producer.publish("sensors", &seed).unwrap();

    let mut client = ctx.create_client();
    client.subscribe("sensors").unwrap();

    let readings = [
        SensorReading { sensor_id: 1, value: 23.5, timestamp: 1000 },
        SensorReading { sensor_id: 2, value: -7.1, timestamp: 2000 },
        SensorReading { sensor_id: 1, value: 24.0, timestamp: 3000 },
    ];

    for r in &readings {
        producer.publish("sensors", r).unwrap();
    }

    let received: Vec<SensorReading> = {
        let mut out = Vec::new();
        while let Some(msg) = client.poll() {
            out.push(*msg.message);
        }
        out
    };

    assert_eq!(received, readings);
}

#[test]
fn fixed_bytes_roundtrip_through_broker() {
    const N: usize = 64;
    let payloads: &[&[u8]] = &[b"hello", b"world", b"", b"exactly 64 bytes?", b"\x00\xFF\xAB"];

    let (producer, ctx) = broker::<FixedBytes<N>>(BctowConfig::default());

    // Publish a sentinel before the consumer subscribes to anchor the cursor.
    producer
        .publish("bytes", &FixedBytes::<N>::new(b"sentinel").unwrap())
        .unwrap();

    let mut client = ctx.create_client();
    client.subscribe("bytes").unwrap();

    for payload in payloads {
        producer
            .publish("bytes", &FixedBytes::<N>::new(payload).unwrap())
            .unwrap();
    }

    let received: Vec<Vec<u8>> = {
        let mut out = Vec::new();
        while let Some(msg) = client.poll() {
            out.push(msg.message.as_bytes().to_vec());
        }
        out
    };

    let expected: Vec<Vec<u8>> = payloads.iter().map(|p| p.to_vec()).collect();
    assert_eq!(received, expected);
}

#[test]
fn fixed_bytes_too_large_error() {
    let err = FixedBytes::<4>::new(b"hello").unwrap_err();
    assert_eq!(err, BctowError::MessageTooLarge { size: 5, max: 4 });
}

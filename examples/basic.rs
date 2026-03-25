use std::error::Error;

use bctow::{broker, BctowConfig, OverflowPolicy};

fn main() -> Result<(), Box<dyn Error>> {
    let config = BctowConfig {
        ring_buffer_capacity: 16,
        max_payload_size: 128,
        overflow_policy: OverflowPolicy::DropOldest,
    };

    let (producer, ctx) = broker(config);

    let temperature = producer.register_topic("sensors/temperature");
    let humidity = producer.register_topic("sensors/humidity");

    producer.publish_id(temperature, b"temp=20")?;
    producer.publish_id(humidity, b"hum=40")?;

    let mut client = ctx.create_client();
    client.subscribe_id(temperature)?;
    client.subscribe_id(humidity)?;

    producer.publish("sensors/temperature", b"temp=21")?;
    producer.publish("sensors/humidity", b"hum=41")?;

    while let Some(message) = client.poll() {
        let name = ctx
            .topic_name(message.topic_id)
            .unwrap_or_else(|| message.topic_id.to_string());
        println!("{name}: {}", String::from_utf8_lossy(message.payload));
    }

    Ok(())
}

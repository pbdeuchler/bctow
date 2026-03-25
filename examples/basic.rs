use std::error::Error;

use bctow::{broker, BctowConfig, OverflowPolicy};

fn main() -> Result<(), Box<dyn Error>> {
    let config = BctowConfig {
        ring_buffer_capacity: 16,
        overflow_policy: OverflowPolicy::DropOldest,
    };

    let (producer, ctx) = broker::<u64>(config);

    let temperature = producer.register_topic("sensors/temperature");
    let humidity = producer.register_topic("sensors/humidity");

    producer.publish_id(temperature, &20u64)?;
    producer.publish_id(humidity, &40u64)?;

    let mut client = ctx.create_client();
    client.subscribe_id(temperature)?;
    client.subscribe_id(humidity)?;

    producer.publish("sensors/temperature", &21u64)?;
    producer.publish("sensors/humidity", &41u64)?;

    while let Some(message) = client.poll() {
        let name = ctx
            .topic_name(message.topic_id)
            .unwrap_or_else(|| message.topic_id.to_string());
        println!("{name}: {}", message.message);
    }

    Ok(())
}

use std::error::Error;

use bctow::{broker, BctowConfig, OverflowPolicy};

#[derive(Copy, Clone, Debug)]
struct SensorReading {
    value: f32,
    unit: &'static str,
}

fn main() -> Result<(), Box<dyn Error>> {
    let config = BctowConfig {
        ring_buffer_capacity: 16,
        overflow_policy: OverflowPolicy::DropOldest,
    };

    let (producer, ctx) = broker::<SensorReading>(config);

    let temperature = producer.register_topic("sensors/temperature");
    let humidity = producer.register_topic("sensors/humidity");

    // Publish an initial reading on each topic before the consumer subscribes.
    // These are retained in the ring but the consumer cursor starts at "latest",
    // so they will not be delivered.
    producer.publish_id(temperature, &SensorReading { value: 20.0, unit: "°C" })?;
    producer.publish_id(humidity, &SensorReading { value: 40.0, unit: "%" })?;

    let mut client = ctx.create_client();
    client.subscribe_id(temperature)?;
    client.subscribe_id(humidity)?;

    // Publish updated readings after the consumer has subscribed.
    producer.publish_id(temperature, &SensorReading { value: 21.5, unit: "°C" })?;
    producer.publish_id(humidity, &SensorReading { value: 42.3, unit: "%" })?;

    while let Some(message) = client.poll() {
        let name = ctx
            .topic_name(message.topic_id)
            .unwrap_or_else(|| message.topic_id.to_string());
        println!("{name}: {:.1} {}", message.message.value, message.message.unit);
    }

    Ok(())
}

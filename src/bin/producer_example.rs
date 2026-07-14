// Copyright 2025 Confluent Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A minimal producer example — the write side of a two-terminal demo.
//!
//! Sends one message per second to a topic until you press Ctrl-C, printing
//! the partition and offset the broker assigned to each message. Pair it with
//! `consumer_example` running in another terminal (same `TOPIC` and
//! `BOOTSTRAP_SERVERS`) to watch messages flow end to end.
//!
//! Bootstrap servers and topic are read from the environment so the same
//! binary works against a local broker or a trivup cluster on ephemeral ports.
//!
//! Run with:
//!
//! ```sh
//! cargo run --bin producer_example
//! BOOTSTRAP_SERVERS=localhost:54908 TOPIC=demo-topic cargo run --bin producer_example
//! ```

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::{KafkaProducer, Producer, ProducerConfig, ProducerRecord};

/// Wall-clock milliseconds since the Unix epoch, stamped into each message so
/// the consumer side can see how fresh the record is.
fn now_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bootstrap =
        std::env::var("BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string());
    let topic = std::env::var("TOPIC").unwrap_or_else(|_| "demo-topic".to_string());

    // The producer is configured from a `bootstrap.servers` / `client.id` map,
    // mirroring Java's `new KafkaProducer(Properties, ...)`.
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), bootstrap.clone()),
        ("client.id".to_string(), "producer-example".to_string()),
    ]);
    let config = ProducerConfig::from_properties(&props)?;

    // Keys and values are plain `String`s; `StringSerializer` turns them into
    // the raw bytes that go on the wire.
    let producer = KafkaProducer::<String, String>::from_config(
        config,
        Box::new(StringSerializer),
        Box::new(StringSerializer),
    )?;

    println!("Producing to '{topic}' on {bootstrap}. Press Ctrl-C to stop.");

    let mut counter: u64 = 0;
    loop {
        tokio::select! {
            // Ctrl-C: stop producing and fall through to a clean flush + close.
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupt received, flushing and closing...");
                break;
            }
            // Otherwise, once a second, build and send one record.
            _ = tokio::time::sleep(Duration::from_secs(1)) => {
                counter += 1;
                let key = format!("key-{counter}");
                let value = format!("message #{counter} produced at {}", now_millis());

                // `send()` returns as soon as the record is buffered; awaiting the
                // returned future (`.get()`) waits for the broker's acknowledgement,
                // which carries the assigned partition and offset.
                let record = ProducerRecord::with_key(topic.clone(), Some(key), Some(value.clone()));
                match producer.send(record).await {
                    Ok(future) => match future.get().await {
                        Ok(metadata) => println!(
                            "sent {value:?} -> partition {} offset {}",
                            metadata.partition(),
                            metadata.offset(),
                        ),
                        Err(e) => eprintln!("delivery failed: {e}"),
                    },
                    Err(e) => eprintln!("send failed: {e}"),
                }
            }
        }
    }

    // Flush anything still buffered, then close (awaits in-flight sends).
    producer.flush().await?;
    producer.close().await?;
    println!("Producer closed.");
    Ok(())
}

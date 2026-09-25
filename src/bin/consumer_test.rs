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

//! A minimal smoke-test consumer application.
//!
//! Connects to a broker on `localhost:9092`, subscribes to `test-topic`,
//! and polls for records in a loop until the user interrupts with Ctrl-C.
//! After each non-empty poll it commits synchronously, and on shutdown it
//! performs a final synchronous commit and closes the consumer cleanly.
//!
//! Run with:
//!
//! ```sh
//! cargo run --bin consumer_test
//! ```

use std::collections::HashMap;
use std::io::Write;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::consumer::{ConsumerConfig, KafkaConsumer};

const BOOTSTRAP_SERVERS: &str = "localhost:9092";
const TOPIC: &str = "test-topic-consumer";
const GROUP_ID: &str = "consumer-test-group";
const POLL_TIMEOUT: Duration = Duration::from_millis(1000);

/// Wall-clock milliseconds since the Unix epoch, for tagging each printed
/// record so real-time delivery can be verified against the producer.
fn now_millis() -> u128 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

/// Wraps `poll()` to also report how long the call took.
async fn poll_with_timing(
    consumer: &mut Box<dyn confluent_kafka::consumer::Consumer<String, String>>,
) -> Result<(confluent_kafka::consumer::ConsumerRecords<String, String>, Duration), Error> {
    let started = Instant::now();
    let records = consumer.poll(POLL_TIMEOUT).await?;
    Ok((records, started.elapsed()))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // KIP-848 ("consumer") group protocol — the only protocol this client
    // supports today. The default ("classic") is rejected by the factory.
    let config = ConsumerConfig::new(&HashMap::from([(
        "bootstrap.servers".to_string(),
        BOOTSTRAP_SERVERS.to_string(),
    )]))?
    .set_client_id("consumer-test")
    .set_group_id(GROUP_ID)
    .set_group_protocol("consumer")
    .set_auto_offset_reset("earliest")
    // Commit explicitly after each batch instead of on a timer.
    .set_enable_auto_commit(false);

    let mut consumer =
        KafkaConsumer::new::<String, String>(config, Box::new(StringDeserializer), Box::new(StringDeserializer))?;

    consumer.subscribe_with_topics(vec![TOPIC.to_string()]).await?;
    println!("Subscribed to '{TOPIC}' on {BOOTSTRAP_SERVERS}. Press Ctrl-C to stop.");

    loop {
        tokio::select! {
            // Ctrl-C: stop the poll loop and fall through to clean shutdown.
            _ = tokio::signal::ctrl_c() => {
                println!("\nInterrupt received, shutting down...");
                break;
            }
            result = poll_with_timing(&mut consumer) => {
                match result {
                    Ok((records, elapsed)) => {
                        if records.is_empty() {
                            continue;
                        }
                        // One line per batch so it is obvious whether records
                        // arrive in real time or in bursts: how many records
                        // this poll() returned and how long the call took.
                        println!("[poll returned {} record(s) in {:?}]", records.count(), elapsed);
                        // Print and flush each record the moment it is
                        // consumed, so output appears immediately even when
                        // stdout is piped/redirected (block-buffered).
                        let stdout = std::io::stdout();
                        let mut out = stdout.lock();
                        for record in &records {
                            let _ = writeln!(
                                out,
                                "{} | {}-{} @ offset {}: key={:?} value={:?}",
                                now_millis(),
                                record.topic(),
                                record.partition(),
                                record.offset(),
                                record.key(),
                                record.value(),
                            );
                            let _ = out.flush();
                        }
                        // Commit the offsets of the batch we just processed.
                        if let Err(e) = consumer.commit_sync().await {
                            eprintln!("commit_sync failed: {e}");
                        }
                    }
                    Err(e) => {
                        eprintln!("poll failed: {e}");
                        // Back off briefly so a persistent error does not
                        // spin the loop at 100% CPU.
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            }
        }
    }

    // Final synchronous commit, then close the consumer (leaves the group).
    if let Err(e) = consumer.commit_sync().await {
        eprintln!("final commit_sync failed: {e}");
    }
    consumer.close().await?;
    println!("Consumer closed.");

    Ok(())
}

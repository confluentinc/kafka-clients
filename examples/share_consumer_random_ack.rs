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

//! Example: consuming from a Kafka share group (KIP-932) with randomized
//! acknowledgements.
//!
//! Like `share_consumer`, but instead of always accepting, each record is
//! acknowledged randomly:
//!
//!   * 5% [`AcknowledgeType::Reject`] — the record is not eligible for further
//!     delivery,
//!   * 15% [`AcknowledgeType::Release`] — the record goes back into the queue
//!     and will be delivered again (its `delivery_count` grows each time),
//!   * 5% [`AcknowledgeType::Renew`] — the record is still being processed; the
//!     acquisition lock is renewed so processing can continue,
//!   * 75% [`AcknowledgeType::Accept`] — processed successfully.
//!
//! Once a record's `delivery_count` reaches 4 it is always accepted, so a
//! record that keeps getting released does not bounce around forever.
//!
//! Because released records are redelivered, watching the `delivery_count`
//! climb across polls shows the share-group in-flight/redelivery mechanism at
//! work. Each line is printed as:
//!
//! ```text
//! topic[partition]: message - offset (delivery_count) - ack_type
//! ```
//!
//! Run it against a broker that has share groups enabled:
//!
//! ```sh
//! # defaults: bootstrap=localhost:9092, group=share-example, topic=foo
//! cargo run --example share_consumer_random_ack
//!
//! # or override via positional args: <bootstrap.servers> <group.id> <topic>
//! cargo run --example share_consumer_random_ack -- localhost:9092 my-group my-topic
//! ```
//!
//! Press Ctrl-C to stop; the consumer commits pending acknowledgements and
//! closes cleanly.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::{KafkaError, TopicIdPartition};
use confluent_kafka::consumer::{
    AcknowledgeType, AcknowledgementCommitCallback, KafkaShareConsumer, ShareConsumer, ShareConsumerConfig,
};
use rand::Rng;

/// A minimal UTF-8 string deserializer (see the `share_consumer` example).
struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
        Ok(String::from_utf8_lossy(data).into_owned())
    }
}

/// Callback fired once committed acknowledgements have been processed by the
/// broker. Runs on the consumer's own task while draining background events
/// inside `poll` / `commit_*` (see `consumer-threading.md` §31). Rejected and
/// released records are also part of the committed acknowledgement state, so
/// their offsets show up here too.
struct LoggingAckCallback;

#[async_trait]
impl AcknowledgementCommitCallback for LoggingAckCallback {
    async fn on_complete(&self, offsets: &HashMap<TopicIdPartition, HashSet<i64>>, error: Option<&KafkaError>) {
        for (tp, tp_offsets) in offsets {
            let mut sorted: Vec<i64> = tp_offsets.iter().copied().collect();
            sorted.sort_unstable();
            match error {
                None => println!("  [callback] committed {}[{}] offsets {sorted:?}", tp.topic(), tp.partition()),
                Some(err) => {
                    eprintln!(
                        "  [callback] commit FAILED {}[{}] offsets {sorted:?}: {err}",
                        tp.topic(),
                        tp.partition()
                    )
                },
            }
        }
    }
}

/// Pick an acknowledgement for a record, given how many times it has already
/// been delivered.
///
/// Once `delivery_count` reaches 4 the record is always `Accept`ed, so it stops
/// being redelivered. Otherwise the choice is random: 5% `Reject`, 15%
/// `Release`, 5% `Renew`, 75% `Accept`.
fn random_ack_type(delivery_count: Option<i16>) -> AcknowledgeType {
    // Stop retrying once the record has been delivered 4 times.
    if delivery_count.is_some_and(|c| c >= 4) {
        return AcknowledgeType::Accept;
    }
    let roll: f64 = rand::rng().random(); // uniform in [0.0, 1.0)
    if roll < 0.05 {
        AcknowledgeType::Reject
    } else if roll < 0.20 {
        AcknowledgeType::Release
    } else if roll < 0.25 {
        AcknowledgeType::Renew
    } else {
        AcknowledgeType::Accept
    }
}

#[tokio::main]
async fn main() -> Result<(), KafkaError> {
    // `RUST_LOG=info cargo run --example share_consumer_random_ack` for logs.
    env_logger::init();

    let mut args = std::env::args().skip(1);
    let bootstrap_servers = args.next().unwrap_or_else(|| "localhost:9092".to_string());
    let group_id = args.next().unwrap_or_else(|| "share-example".to_string());
    let topic = args.next().unwrap_or_else(|| "foo".to_string());

    let mut props = HashMap::new();
    props.insert("bootstrap.servers".to_string(), bootstrap_servers.clone());
    props.insert("group.id".to_string(), group_id.clone());
    // "explicit": we acknowledge every record ourselves before the next poll.
    props.insert("share.acknowledgement.mode".to_string(), "explicit".to_string());

    let config = ShareConsumerConfig::from_properties(&props)?;

    let mut consumer: KafkaShareConsumer<String, String> =
        KafkaShareConsumer::new(config, Box::new(StringDeserializer), Box::new(StringDeserializer))?;

    // Register a callback fired when committed acknowledgements complete.
    consumer.set_acknowledgement_commit_callback(Some(Arc::new(LoggingAckCallback)));

    consumer.subscribe(vec![topic.clone()]).await?;
    println!("Subscribed to '{topic}' in share group '{group_id}' at {bootstrap_servers}. Press Ctrl-C to stop.");

    loop {
        // Race a poll against Ctrl-C so we can shut down promptly.
        let records = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl-C received, shutting down...");
                break;
            }
            result = consumer.poll(Duration::from_millis(1000)) => result?,
        };

        for record in &records {
            let message = record.value().map(String::as_str).unwrap_or("<null>");
            // `delivery_count` is the number of times this record has been
            // delivered to a consumer in the group; `None` if the broker did
            // not report it.
            let raw_delivery_count = record.delivery_count();
            let delivery_count = raw_delivery_count.map(|c| c.to_string()).unwrap_or_else(|| "?".to_string());

            let ack_type = random_ack_type(raw_delivery_count);

            // topic[partition]: message - offset (delivery_count) - ack_type
            println!(
                "{}[{}]: {} - {} ({}) - {:?}",
                record.topic(),
                record.partition(),
                message,
                record.offset(),
                delivery_count,
                ack_type,
            );

            // `acknowledge_*` only updates local state; it is committed below.
            consumer.acknowledge_with_type(record, ack_type)?;
        }

        // Send the accumulated acknowledgement state to Kafka.
        if !records.is_empty() {
            for (tp, maybe_err) in consumer.commit_sync().await? {
                if let Some(err) = maybe_err {
                    eprintln!("commit failed for {tp:?}: {err}");
                }
            }
        }
    }

    // Close commits any pending acknowledgements and leaves the share group.
    consumer.close().await?;
    println!("Closed.");
    Ok(())
}

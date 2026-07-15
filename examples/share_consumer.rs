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

//! Example: consuming from a Kafka share group (KIP-932).
//!
//! Mirrors the "per-record acknowledgement (explicit acknowledgement)" example
//! from the Java `KafkaShareConsumer` javadoc, adapted to the async Rust API.
//!
//! A share group lets multiple consumers cooperatively consume the same
//! partitions: Kafka hands each record to exactly one consumer in the group and
//! tracks per-record delivery state on the broker (unlike a classic consumer
//! group, where each partition is owned by a single member). The consumer
//! *acknowledges* each record — `Accept`, `Release`, or `Reject` — and commits
//! those acknowledgements back to Kafka.
//!
//! Run it against a broker that has share groups enabled:
//!
//! ```sh
//! # defaults: bootstrap=localhost:9092, group=share-example, topic=foo
//! cargo run --example share_consumer
//!
//! # or override via positional args: <bootstrap.servers> <group.id> <topic>
//! cargo run --example share_consumer -- localhost:9092 my-group my-topic
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

/// A minimal UTF-8 string deserializer.
///
/// The Rust client takes deserializers explicitly (rather than by class name
/// via reflection like Java), so we supply our own. Bytes are borrowed from the
/// fetch buffer; the only allocation is the decoded `String` (see
/// `consumer-threading.md` §27).
struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
        Ok(String::from_utf8_lossy(data).into_owned())
    }
}

/// Callback invoked once the acknowledgements committed by `commit_sync` /
/// `commit_async` (or an implicit commit on `poll` / `close`) have been
/// processed by the broker.
///
/// It runs on the consumer's own task while draining the background-event
/// queue inside `poll` / `commit_*` (see `consumer-threading.md` §31), so it
/// never races the poll loop. `error` is `None` on success, or the failure
/// that means those offsets could not be committed and the records will be
/// delivered again.
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

#[tokio::main]
async fn main() -> Result<(), KafkaError> {
    // `RUST_LOG=info cargo run --example share_consumer` to see client logs.
    env_logger::init();

    let mut args = std::env::args().skip(1);
    let bootstrap_servers = args.next().unwrap_or_else(|| "localhost:9092".to_string());
    let group_id = args.next().unwrap_or_else(|| "share-example".to_string());
    let topic = args.next().unwrap_or_else(|| "foo".to_string());

    let mut props = HashMap::new();
    props.insert("bootstrap.servers".to_string(), bootstrap_servers.clone());
    props.insert("group.id".to_string(), group_id.clone());
    // "explicit": the application must acknowledge every record returned by a
    // poll before the next poll. The default, "implicit", auto-acknowledges all
    // delivered records on the next poll / commit instead.
    props.insert("share.acknowledgement.mode".to_string(), "explicit".to_string());

    let config = ShareConsumerConfig::from_properties(&props)?;

    let mut consumer: KafkaShareConsumer<String, String> =
        KafkaShareConsumer::new(config, Box::new(StringDeserializer), Box::new(StringDeserializer))?;

    // Register a callback fired when committed acknowledgements complete.
    consumer.set_acknowledgement_commit_callback(Some(Arc::new(LoggingAckCallback)));

    consumer.subscribe(vec![topic.clone()]).await?;
    println!("Subscribed to '{topic}' in share group '{group_id}' at {bootstrap_servers}. Press Ctrl-C to stop.");

    loop {
        // Race a poll against Ctrl-C so we can shut down promptly. `poll`
        // implicitly (re)joins the share group and keeps the member alive.
        let records = tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                println!("\nCtrl-C received, shutting down...");
                break;
            }
            result = consumer.poll(Duration::from_millis(1000)) => result?,
        };

        for record in &records {
            let key = record.key().map(String::as_str).unwrap_or("<null>");
            let value = record.value().map(String::as_str).unwrap_or("<null>");
            println!(
                "topic = {}, partition = {}, offset = {}, key = {}, value = {}",
                record.topic(),
                record.partition(),
                record.offset(),
                key,
                value,
            );

            // Explicit acknowledgement: mark each record processed. On a
            // permanent (e.g. semantic) error, `Reject` so it is not
            // redelivered; for a transient error, `Release` keeps it eligible
            // for another delivery attempt.
            match process(record.value()) {
                Ok(()) => consumer.acknowledge_with_type(record, AcknowledgeType::Accept)?,
                Err(err) => {
                    eprintln!("failed to process offset {}: {err}", record.offset());
                    consumer.acknowledge_with_type(record, AcknowledgeType::Reject)?;
                },
            }
        }

        // `acknowledge_*` only updates local state; `commit_sync` sends the new
        // acknowledgement state to Kafka and waits for the response. The
        // returned map reports any per-partition commit errors.
        if !records.is_empty() {
            for (tp, maybe_err) in consumer.commit_sync().await? {
                if let Some(err) = maybe_err {
                    eprintln!("commit failed for {tp:?}: {err}");
                }
            }
        }
    }

    // Close attempts to commit any pending acknowledgements and leaves the
    // share group.
    consumer.close().await?;
    println!("Closed.");
    Ok(())
}

/// Stand-in for the application's per-record processing.
fn process(_value: Option<&String>) -> Result<(), KafkaError> {
    Ok(())
}

// Copyright 2026 Confluent Inc.
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

//! Smoke test for `confluent-kafka` as distributed to users. `cargo xtask
//! package-check` and `cargo xtask package-smoke-test` compile this file as the
//! `main.rs` of a new project that depends on either the packaged `.crate` or
//! the version published to crates.io. The project is not part of any
//! workspace, so the test can use only the crate's public API.
//!
//! When `KAFKA_BOOTSTRAP_SERVERS` is unset, only the offline checks run: client
//! configuration parsing and a `MockProducer` round trip. When it is set, the
//! test also creates a topic, produces records, consumes them through the
//! KIP-848 consumer and deletes the topic, so that a defect in the network,
//! protocol or group-membership path is detected here rather than in a user's
//! application.

use std::collections::{BTreeSet, HashMap};
use std::process::ExitCode;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use confluent_kafka::admin::{Admin, AdminClient, AdminClientConfig, NewTopic};
use confluent_kafka::common::serialization::{StringDeserializer, StringSerializer};
use confluent_kafka::common::{Error, LocalIllegalStateError, TopicCollection};
use confluent_kafka::consumer::{Consumer, ConsumerConfig, GroupProtocol, KafkaConsumer};
use confluent_kafka::producer::{KafkaProducer, MockProducer, Producer, ProducerConfig, ProducerRecord};

const NUM_PARTITIONS: i32 = 3;
const NUM_RECORDS: usize = 100;
/// Maximum time for the consumer to join the group and read back every record.
const CONSUME_DEADLINE: Duration = Duration::from_secs(90);
/// Maximum time for the whole round trip, from creating the topic to closing the
/// consumer. Every client call is awaited within this limit, so a call that never
/// completes fails the smoke test instead of stalling the CI job.
const ROUND_TRIP_DEADLINE: Duration = Duration::from_secs(240);
/// Maximum time for deleting the topic and closing the admin client.
const CLEANUP_DEADLINE: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> ExitCode {
    if let Err(e) = offline().await {
        eprintln!("❌ smoke test (offline) failed: {e}");
        return ExitCode::FAILURE;
    }
    println!("✅ offline checks passed");

    let Ok(bootstrap_servers) = std::env::var("KAFKA_BOOTSTRAP_SERVERS") else {
        println!("KAFKA_BOOTSTRAP_SERVERS is not set: skipping the broker round trip");
        return ExitCode::SUCCESS;
    };
    match round_trip(&bootstrap_servers).await {
        Ok(()) => {
            println!("✅ broker round trip passed against {bootstrap_servers}");
            ExitCode::SUCCESS
        },
        Err(e) => {
            eprintln!("❌ smoke test (broker round trip against {bootstrap_servers}) failed: {e}");
            ExitCode::FAILURE
        },
    }
}

/// Checks that require no broker: the configuration of every client parses, and
/// a record sent through the `Producer` trait is recorded in `MockProducer`'s
/// history.
async fn offline() -> Result<(), Error> {
    let bootstrap = "localhost:9092".to_string();
    ProducerConfig::new(&HashMap::from([(
        ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        bootstrap.clone(),
    )]))?;
    ConsumerConfig::new(&HashMap::from([
        (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap.clone()),
        (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "smoke".to_string()),
        (
            ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(),
            GroupProtocol::Consumer.to_string(),
        ),
    ]))?;
    AdminClientConfig::new(&HashMap::from([(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        bootstrap,
    )]))?;

    let producer = MockProducer::<String, String>::with_auto_complete(true);
    let record = ProducerRecord::with_key("smoke".to_string(), Some("key".to_string()), Some("value".to_string()));
    producer.send(record).await?.get().await?;
    let history = producer.history();
    if history.len() != 1 || history[0].value() != Some(&"value".to_string()) {
        return Err(failure(format!(
            "MockProducer history contains {} records; expected exactly the one sent",
            history.len()
        )));
    }
    Ok(())
}

/// Creates a topic, produces [`NUM_RECORDS`] records to it, consumes them and
/// deletes the topic. The topic and group names are unique to each run, so
/// concurrent or repeated runs against the same broker do not interfere.
async fn round_trip(bootstrap_servers: &str) -> Result<(), Error> {
    let run_id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or_default();
    let topic = format!("confluent-kafka-smoke-{}-{run_id}", std::process::id());
    let group = format!("{topic}-group");

    let admin = AdminClient::create(AdminClientConfig::new(&HashMap::from([(
        AdminClientConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        bootstrap_servers.to_string(),
    )]))?)?;
    let result = tokio::time::timeout(
        ROUND_TRIP_DEADLINE,
        produce_and_consume(admin.as_ref(), bootstrap_servers, &topic, &group),
    )
    .await
    .unwrap_or_else(|_| Err(failure(format!("the round trip did not finish within {ROUND_TRIP_DEADLINE:?}"))));
    // Always delete the topic, but report the first failure that occurred.
    let cleanup = tokio::time::timeout(CLEANUP_DEADLINE, async {
        let delete = admin
            .delete_topics(TopicCollection::of_topic_names(vec![topic.clone()]))
            .all()
            .get()
            .await;
        admin.close().await;
        delete
    })
    .await
    .unwrap_or_else(|_| {
        Err(failure(format!(
            "deleting topic {topic} and closing the admin client did not finish within {CLEANUP_DEADLINE:?}"
        )))
    });
    result.and(cleanup)
}

async fn produce_and_consume(
    admin: &dyn Admin,
    bootstrap_servers: &str,
    topic: &str,
    group: &str,
) -> Result<(), Error> {
    admin
        .create_topics(&[NewTopic::with_num_partitions_replication_factor(
            topic,
            Some(NUM_PARTITIONS),
            Some(1),
        )])
        .all()
        .get()
        .await?;
    println!("created topic {topic} with {NUM_PARTITIONS} partitions");

    let expected: BTreeSet<(String, String)> =
        (0..NUM_RECORDS).map(|i| (format!("key-{i}"), format!("value-{i}"))).collect();

    let producer: KafkaProducer<String, String> = KafkaProducer::new(
        ProducerConfig::new(&HashMap::from([
            (
                ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                bootstrap_servers.to_string(),
            ),
            (ProducerConfig::ACKS_CONFIG.to_string(), "all".to_string()),
        ]))?,
        Box::new(StringSerializer::new()),
        Box::new(StringSerializer::new()),
    )?;
    let mut deliveries = Vec::with_capacity(NUM_RECORDS);
    for (key, value) in &expected {
        let record = ProducerRecord::with_key(topic.to_string(), Some(key.clone()), Some(value.clone()));
        deliveries.push(producer.send(record).await?);
    }
    for delivery in deliveries {
        delivery.get().await?;
    }
    producer.close().await?;
    println!("produced {NUM_RECORDS} records");

    let mut consumer = KafkaConsumer::new(
        ConsumerConfig::new(&HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                bootstrap_servers.to_string(),
            ),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), group.to_string()),
            (
                ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(),
                GroupProtocol::Consumer.to_string(),
            ),
            (ConsumerConfig::AUTO_OFFSET_RESET_CONFIG.to_string(), "earliest".to_string()),
        ]))?,
        Box::new(StringDeserializer::new()),
        Box::new(StringDeserializer::new()),
    )?;
    let consumed = consume_all(consumer.as_mut(), topic, expected.len()).await;
    let close = consumer.close().await;
    let consumed = consumed?;
    close?;

    if consumed != expected {
        let missing = expected.difference(&consumed).count();
        let unexpected = consumed.difference(&expected).count();
        return Err(failure(format!(
            "consumed records do not match those produced: {missing} missing, {unexpected} unexpected"
        )));
    }
    println!("consumed all {NUM_RECORDS} records through group {group}");
    Ok(())
}

async fn consume_all(
    consumer: &mut dyn Consumer<String, String>,
    topic: &str,
    count: usize,
) -> Result<BTreeSet<(String, String)>, Error> {
    consumer.subscribe_with_topics(vec![topic.to_string()]).await?;
    let deadline = Instant::now() + CONSUME_DEADLINE;
    let mut consumed = BTreeSet::new();
    while consumed.len() < count {
        if Instant::now() >= deadline {
            return Err(failure(format!(
                "consumed {} of {count} records within {CONSUME_DEADLINE:?}",
                consumed.len()
            )));
        }
        for record in consumer.poll(Duration::from_secs(1)).await? {
            // Every record is produced with a key and a value, so a record
            // missing either indicates data loss in the round trip.
            let (Some(key), Some(value)) = (record.key(), record.value()) else {
                return Err(failure(format!(
                    "consumed a record without a key or value at offset {} of partition {}",
                    record.offset(),
                    record.partition()
                )));
            };
            consumed.insert((key.clone(), value.clone()));
        }
    }
    consumer.commit_sync().await?;
    Ok(consumed)
}

/// Builds a smoke-test failure, reported through the crate's own error type.
fn failure(message: String) -> Error {
    Error::LocalIllegalState(LocalIllegalStateError::new(message))
}

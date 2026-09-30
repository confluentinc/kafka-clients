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

//! Produces a batch of records in a single transaction.
//!
//! Error handling follows the KIP-1050 error categories.

use std::collections::HashMap;
use std::process::ExitCode;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;

use uuid::Uuid;

const TOPIC_NAME: &str = "my-topic";
const TRANSACTIONAL_ID: &str = "my-transactional-id";
const NUM_RECORDS: usize = 100;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("transactional producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let producer = create_kafka_producer()?;
    println!("Kafka producer created successfully.");

    let result = produce_transactionally(&producer).await;
    let close_result = producer.close().await;
    result.and(close_result)
}

async fn produce_transactionally(producer: &KafkaProducer<String, String>) -> Result<(), Error> {
    producer.init_transactions().await?;
    println!("Transactions initialized for transactional.id={TRANSACTIONAL_ID}");

    let result = match produce_in_transaction(producer).await {
        // Abortable: abort so the caller may retry. A failed abort replaces `e`.
        Err(e) if matches!(e, Error::TransactionAbortable(_)) => {
            eprintln!("Aborting transaction: {e}");
            producer.abort_transaction().await.and(Err(e))
        },
        other => other,
    };

    match result {
        Ok(()) => {
            println!("Committed a transaction of {NUM_RECORDS} records to topic {TOPIC_NAME}");
            Ok(())
        },
        // Fatal: fix the configuration before restarting.
        Err(e) if e.is_invalid_configuration_error() => {
            eprintln!("Invalid configuration, shutting down: {e}");
            Err(e)
        },
        // Recoverable by closing this producer and creating a new one.
        Err(e) if e.is_application_recoverable_error() => {
            eprintln!("Application-recoverable error, closing the producer: {e}");
            Err(e)
        },
        // Includes a timed-out commit: it may still complete on the broker, so
        // retry it or close the producer, never abort.
        Err(e) if e.is_kafka_error() => {
            eprintln!("Kafka error, closing the producer: {e}");
            Err(e)
        },
        Err(e) => {
            eprintln!("Unhandled error: {e}");
            Err(e)
        },
    }
}

async fn produce_in_transaction(producer: &KafkaProducer<String, String>) -> Result<(), Error> {
    producer.begin_transaction()?;
    println!("Transaction begun.");

    for i in 0..NUM_RECORDS {
        let key = format!("key-{i}");
        let value = format!("value-{i}");

        println!("Producing record: key={key}, value={value}");
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(key), Some(value));
        // The future is not awaited: `commit_transaction` fails if any send failed.
        producer.send(record).await?;
    }
    println!("Sent {NUM_RECORDS} records, committing the transaction.");

    producer.commit_transaction().await
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn create_kafka_producer() -> Result<KafkaProducer<String, String>, Error> {
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        // Enables transactions and idempotence; must be unique per producer instance.
        (
            ProducerConfig::TRANSACTIONAL_ID_CONFIG.to_string(),
            TRANSACTIONAL_ID.to_string(),
        ),
        (
            ProducerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);

    println!("Kafka producer properties: {props:?}");

    let config = ProducerConfig::new(&props)?;
    KafkaProducer::new(config, Box::new(StringSerializer::new()), Box::new(StringSerializer::new()))
}

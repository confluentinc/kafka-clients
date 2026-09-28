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

    match produce_in_transaction(producer).await {
        Ok(()) => {
            println!("Committed a transaction of {NUM_RECORDS} records to topic {TOPIC_NAME}");
            Ok(())
        },
        Err(e) if is_unrecoverable(&e) => {
            // We can't recover from these errors, so close the producer and exit.
            eprintln!("Unrecoverable error, closing the producer: {e}");
            Err(e)
        },
        Err(e) if e.is_kafka_error() => {
            // For all other Kafka errors, just abort the transaction.
            eprintln!("Aborting transaction: {e}");
            producer.abort_transaction().await.and(Err(e))
        },
        // Not a Kafka error: nothing to abort, so it propagates unchanged.
        Err(e) => Err(e),
    }
}

async fn produce_in_transaction(producer: &KafkaProducer<String, String>) -> Result<(), Error> {
    producer.begin_transaction()?;
    println!("Transaction begun.");

    for i in 0..NUM_RECORDS {
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(i.to_string()), Some(i.to_string()));
        // The future is not awaited: `commit_transaction` fails if any send failed.
        producer.send(record).await?;
    }
    println!("Sent {NUM_RECORDS} records, committing the transaction.");

    producer.commit_transaction().await
}

fn is_unrecoverable(error: &Error) -> bool {
    matches!(error, Error::ProducerFenced(_))
        || error.is_out_of_order_sequence_error()
        || error.is_authorization_error()
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
    KafkaProducer::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
}

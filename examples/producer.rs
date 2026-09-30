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
const NUM_RECORDS: usize = 10;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let producer = create_kafka_producer()?;
    println!("Kafka producer created successfully.");

    let result = produce_records(&producer).await;
    let close_result = producer.close().await;
    result.and(close_result)
}

async fn produce_records(producer: &KafkaProducer<String, String>) -> Result<(), Error> {
    for i in 0..NUM_RECORDS {
        let key = format!("key-{i}");
        let value = format!("value-{i}");

        println!("Producing record: key={key}, value={value}");
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(key), Some(value));

        // Synchronous produce: `send` returns a future for the record's delivery,
        // and awaiting `get` blocks until the broker has acknowledged it (or the
        // delivery has failed) before the next record is sent.
        let future = producer.send(record).await?;
        match future.get().await {
            Ok(metadata) => println!(
                "Delivered record {i} to topic {}, partition {} at offset {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
            ),
            Err(error) => {
                eprintln!("Failed to deliver record {i}: {error}");
                return Err(error);
            },
        }
    }
    Ok(())
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn create_kafka_producer() -> Result<KafkaProducer<String, String>, Error> {
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (
            ProducerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);

    println!("Kafka producer properties: {props:?}");

    let config = ProducerConfig::new(&props)?;
    KafkaProducer::new(config, Box::new(StringSerializer), Box::new(StringSerializer))
}

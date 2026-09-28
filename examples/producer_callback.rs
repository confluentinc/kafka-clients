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
use std::sync::Arc;
use std::sync::Mutex;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::Callback;
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

    // Delivery callbacks run on the producer's background task, so the first
    // delivery error is recorded here and checked once `close()` has flushed
    // every outstanding record and invoked every callback.
    let first_delivery_error: Arc<Mutex<Option<Error>>> = Arc::new(Mutex::new(None));

    let result = produce_records(&producer, &first_delivery_error).await;
    let close_result = producer.close().await;

    let delivery_result = match first_delivery_error.lock().unwrap().take() {
        Some(e) => Err(e),
        None => Ok(()),
    };
    result.and(delivery_result).and(close_result)
}

async fn produce_records(
    producer: &KafkaProducer<String, String>,
    first_delivery_error: &Arc<Mutex<Option<Error>>>,
) -> Result<(), Error> {
    for i in 0..NUM_RECORDS {
        let key = format!("key-{i}");
        let value = format!("value-{i}");

        println!("Producing record: key={key}, value={value}");
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(key), Some(value));

        let callback = delivery_callback(i, Arc::clone(first_delivery_error));
        producer.send_with_callback(record, Some(callback)).await?;
    }
    Ok(())
}

fn delivery_callback(index: usize, first_delivery_error: Arc<Mutex<Option<Error>>>) -> Callback {
    // Per the `Callback` contract, on failure `metadata` is still `Some` but holds a
    // sentinel `-1` offset (and `-1` partition if none could be resolved), so `error`
    // must be checked first to tell success from failure.
    Box::new(move |metadata, error| match error {
        Some(error) => {
            eprintln!("Failed to deliver record {index}: {error}");
            first_delivery_error.lock().unwrap().get_or_insert_with(|| error.clone());
        },
        None => match metadata {
            Some(metadata) => println!(
                "Delivered record {index} to topic {}, partition {} at offset {}",
                metadata.topic(),
                metadata.partition(),
                metadata.offset()
            ),
            None => eprintln!("Record {index} completed with neither metadata nor error"),
        },
    })
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

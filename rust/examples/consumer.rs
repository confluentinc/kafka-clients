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
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::GroupProtocol;
use confluent_kafka::consumer::KafkaConsumer;

use uuid::Uuid;

const TOPIC_NAME: &str = "my-topic";
const GROUP_ID: &str = "my-group";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("consumer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let mut consumer = create_kafka_consumer()?;
    println!("Kafka consumer created successfully.");

    consumer.subscribe_with_topics(vec![TOPIC_NAME.to_string()]).await?;
    println!("Subscribed to topic: {TOPIC_NAME}");

    let result = consume_loop(&mut *consumer).await;
    let close_result = consumer.close().await;
    result.and(close_result)
}

async fn consume_loop(consumer: &mut dyn Consumer<String, String>) -> Result<(), Error> {
    loop {
        let records = match consumer.poll(Duration::from_secs(30)).await {
            Ok(records) => records,
            Err(e) if e.is_retriable_error() => {
                eprintln!("transient poll error, retrying: {e}");
                continue;
            },
            Err(Error::Wakeup(_)) => return Ok(()),
            Err(e) => return Err(e),
        };

        if records.is_empty() {
            println!("No records consumed in this poll interval.");
            return Ok(());
        }

        for record in records {
            println!("Consumed record: key={:?}, value={:?}", record.key(), record.value());
        }
        consumer.commit_sync().await?;
    }
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn create_kafka_consumer() -> Result<Box<dyn Consumer<String, String>>, Error> {
    let props = HashMap::from([
        (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (ConsumerConfig::GROUP_ID_CONFIG.to_string(), GROUP_ID.to_string()),
        (
            ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(),
            GroupProtocol::Consumer.to_string(),
        ),
        (
            ConsumerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);

    println!("Kafka consumer properties: {props:?}");

    let config = ConsumerConfig::new(&props)?;
    KafkaConsumer::new(config, Box::new(StringDeserializer::new()), Box::new(StringDeserializer::new()))
}

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

//! Consumes the JSON records `json_schema_producer` writes, validating them
//! against the JSON Schema fetched from Confluent Schema Registry through
//! Robert Yokota's `schema-registry-client` crate.
//!
//! The registry deserializer is wrapped in an implementation of this client's
//! [`Deserializer`] trait, [`SchemaRegistryJsonDeserializer`], so the consumer
//! is typed over the record type itself (`KafkaConsumer<String, User>`) and
//! `poll` returns decoded records, like any other deserializer.
//!
//! The deserializer takes the schema id from the payload prefix, fetches that
//! schema from the registry, validates the JSON against it, and `serde` then
//! maps the JSON onto the record type. Only the first record of each schema
//! reaches the registry: the registry client caches the schema by id, so
//! every later call completes without a network round trip.

use std::collections::HashMap;
use std::marker::PhantomData;
use std::process::ExitCode;
use std::sync::LazyLock;
use std::time::Duration;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::common::serialization::StringDeserializer;
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::GroupProtocol;
use confluent_kafka::consumer::KafkaConsumer;
use schema_registry_client::rest::client_config::ClientConfig;
use schema_registry_client::rest::schema_registry_client::Client;
use schema_registry_client::rest::schema_registry_client::SchemaRegistryClient;
use schema_registry_client::serdes::config::DeserializerConfig;
use schema_registry_client::serdes::json::JsonDeserializer;
use schema_registry_client::serdes::serde::SerdeFormat;
use schema_registry_client::serdes::serde::SerdeType;
use schema_registry_client::serdes::serde::SerializationContext;
use schema_registry_client::serdes::serde::SubjectNameStrategyType;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

const TOPIC_NAME: &str = "json-users";
const GROUP_ID: &str = "json-users-group";

/// The registry client the deserializer borrows. The [`Deserializer`] trait
/// object the consumer owns must be `'static`, so the client lives for the
/// whole program.
static REGISTRY: LazyLock<SchemaRegistryClient> =
    LazyLock::new(|| SchemaRegistryClient::new(ClientConfig::new(vec![schema_registry_url()])));

/// The Rust shape of a `User` record, filled from the JSON by property name;
/// it matches the schema `json_schema_producer` registers.
#[derive(Deserialize)]
struct User {
    name: String,
    favorite_number: i64,
    favorite_color: String,
}

/// A [`Deserializer`] that reads `T` from JSON in the Confluent wire format.
struct SchemaRegistryJsonDeserializer<T> {
    inner: JsonDeserializer<'static, SchemaRegistryClient>,
    serde_type: SerdeType,
    _record: PhantomData<fn() -> T>,
}

impl<T> SchemaRegistryJsonDeserializer<T> {
    fn new(serde_type: SerdeType, config: DeserializerConfig) -> Result<Self, Error> {
        let inner = JsonDeserializer::new(&*REGISTRY, None, config)
            .map_err(|e| Error::serialization(format!("cannot create the JSON deserializer: {e}")))?;
        Ok(Self { inner, serde_type, _record: PhantomData })
    }
}

impl<T: DeserializeOwned + 'static> Deserializer<T> for SchemaRegistryJsonDeserializer<T> {
    async fn deserialize<'a>(&'a self, topic: &'a str, data: &'a [u8]) -> Result<T, Error> {
        // The schema id travels in the payload prefix, so the record headers
        // are not consulted.
        let context = SerializationContext {
            topic: topic.to_string(),
            serde_type: self.serde_type,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        let value =
            self.inner.deserialize(&context, data).await.map_err(|e| {
                Error::serialization(format!("cannot deserialize the JSON record from topic {topic}: {e}"))
            })?;
        serde_json::from_value(value)
            .map_err(|e| Error::serialization(format!("the JSON record from topic {topic} does not fit the type: {e}")))
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("json schema consumer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let value_deserializer = SchemaRegistryJsonDeserializer::new(SerdeType::Value, deserializer_config())?;
    println!("Schema Registry client created for {}.", REGISTRY.config().base_urls.join(","));

    let mut consumer = create_kafka_consumer(value_deserializer)?;
    println!("Kafka consumer created successfully.");
    consumer.subscribe_with_topics(vec![TOPIC_NAME.to_string()]).await?;
    println!("Subscribed to topic: {TOPIC_NAME}");

    let handle = consumer.handle();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await; // needs tokio's "signal" feature
        handle.wakeup();
    });

    let result = consume_loop(&mut consumer).await;
    let close_result = consumer.close().await;
    result.and(close_result)
}

async fn consume_loop<C: Consumer<String, User>>(consumer: &mut C) -> Result<(), Error> {
    loop {
        // The value deserializer validates and decodes every record inside `poll`.
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
            let Some(user) = record.value() else {
                println!("Consumed tombstone: key={:?}", record.key());
                continue;
            };
            println!(
                "Consumed record: key={:?}, user {} likes {} and {} (partition {}, offset {})",
                record.key(),
                user.name,
                user.favorite_color,
                user.favorite_number,
                record.partition(),
                record.offset()
            );
        }
        consumer.commit_sync().await?;
    }
}

/// Reads with the schema registered under the `<topic>-value` subject, the
/// registry's conventional naming, and validates every record against the
/// writer's schema. This crate version defaults to the `Associated` strategy,
/// which first asks the registry for a topic association and only then falls
/// back to `Topic`: an extra round trip the example does not need.
fn deserializer_config() -> DeserializerConfig {
    DeserializerConfig {
        subject_name_strategy_type: SubjectNameStrategyType::Topic,
        validate: true,
        ..DeserializerConfig::default()
    }
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn schema_registry_url() -> String {
    std::env::var("SCHEMA_REGISTRY_URL").unwrap_or_else(|_| "http://localhost:8081".to_string())
}

fn create_kafka_consumer(
    value_deserializer: SchemaRegistryJsonDeserializer<User>,
) -> Result<KafkaConsumer<String, User>, Error> {
    let props = HashMap::from([
        (ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (ConsumerConfig::GROUP_ID_CONFIG.to_string(), GROUP_ID.to_string()),
        // A new group starts from the beginning of the topic, so the records
        // `json_schema_producer` wrote before this consumer joined are read too.
        (ConsumerConfig::AUTO_OFFSET_RESET_CONFIG.to_string(), "earliest".to_string()),
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
    KafkaConsumer::new(config, StringDeserializer::new(), value_deserializer)
}

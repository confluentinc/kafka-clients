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

//! Produces JSON records whose JSON Schema lives in Confluent Schema Registry,
//! using Robert Yokota's `schema-registry-client` crate for the registry
//! round trip.
//!
//! The registry serializer is wrapped in an implementation of this client's
//! [`Serializer`] trait, [`SchemaRegistryJsonSerializer`], so the producer is
//! typed over the record type itself (`KafkaProducer<String, User>`) and
//! serializes each value inside `send`, like any other serializer.
//!
//! JSON carries no schema of its own, so the serializer validates each record
//! against the schema before writing it: a record that does not fit fails in
//! `send`, before it reaches the broker. Only the first record reaches the
//! registry: the registry client caches the schema id, so every later call
//! completes without a network round trip.

use std::borrow::Cow;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::process::ExitCode;
use std::sync::LazyLock;

use confluent_kafka::common::Error;
use confluent_kafka::common::serialization::Serializer;
use confluent_kafka::common::serialization::StringSerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use confluent_kafka::producer::ProducerRecord;
use schema_registry_client::rest::client_config::ClientConfig;
use schema_registry_client::rest::models::Schema;
use schema_registry_client::rest::schema_registry_client::Client;
use schema_registry_client::rest::schema_registry_client::SchemaRegistryClient;
use schema_registry_client::serdes::config::SerializerConfig;
use schema_registry_client::serdes::json::JsonSerializer;
use schema_registry_client::serdes::serde::SerdeFormat;
use schema_registry_client::serdes::serde::SerdeType;
use schema_registry_client::serdes::serde::SerializationContext;
use schema_registry_client::serdes::serde::SubjectNameStrategyType;
use serde::Serialize;
use uuid::Uuid;

const TOPIC_NAME: &str = "json-users";
const NUM_RECORDS: usize = 10;
const COLORS: [&str; 3] = ["red", "green", "blue"];

/// The JSON Schema of the record values. The serializer registers it under
/// the subject `json-users-value` the first time it is used.
const USER_SCHEMA: &str = r#"{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User",
  "type": "object",
  "properties": {
    "name": {"type": "string"},
    "favorite_number": {"type": "integer"},
    "favorite_color": {"type": "string"}
  },
  "required": ["name", "favorite_number", "favorite_color"],
  "additionalProperties": false
}"#;

/// The registry client and the schema the serializer borrows. The
/// [`Serializer`] trait object the producer owns must be `'static`, so what
/// it borrows lives for the whole program.
static REGISTRY: LazyLock<SchemaRegistryClient> =
    LazyLock::new(|| SchemaRegistryClient::new(ClientConfig::new(vec![schema_registry_url()])));
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema::new(Some("JSON".to_string()), USER_SCHEMA.to_string()));

/// The Rust shape of a `User` record; `serde` maps its fields onto the
/// schema's properties.
#[derive(Debug, Serialize)]
struct User {
    name: String,
    favorite_number: i64,
    favorite_color: String,
}

/// A [`Serializer`] that writes `T` as JSON in the Confluent wire format: a
/// magic byte, the id the registry assigned to the schema, then the JSON
/// text.
struct SchemaRegistryJsonSerializer<T> {
    inner: JsonSerializer<'static, SchemaRegistryClient>,
    serde_type: SerdeType,
    _record: PhantomData<fn(&T)>,
}

impl<T> SchemaRegistryJsonSerializer<T> {
    fn new(schema: &'static Schema, serde_type: SerdeType, config: SerializerConfig) -> Result<Self, Error> {
        let inner = JsonSerializer::new(&*REGISTRY, Some(schema), None, config)
            .map_err(|e| Error::serialization(format!("cannot create the JSON serializer: {e}")))?;
        Ok(Self { inner, serde_type, _record: PhantomData })
    }
}

impl<T: Serialize + Sync> Serializer<T> for SchemaRegistryJsonSerializer<T> {
    async fn serialize<'a>(&'a self, topic: &'a str, data: Option<&'a T>) -> Result<Option<Cow<'a, [u8]>>, Error> {
        // A null value stays null, as Java's serializers recommend.
        let Some(data) = data else { return Ok(None) };
        let value = serde_json::to_value(data)
            .map_err(|e| Error::serialization(format!("cannot convert the record for topic {topic} to JSON: {e}")))?;
        // The schema id travels in the payload prefix, so the record
        // headers are left untouched.
        let context = SerializationContext {
            topic: topic.to_string(),
            serde_type: self.serde_type,
            serde_format: SerdeFormat::Json,
            headers: None,
        };
        self.inner
            .serialize(&context, value)
            .await
            .map(|bytes| Some(Cow::Owned(bytes)))
            .map_err(|e| Error::serialization(format!("cannot serialize the JSON record for topic {topic}: {e}")))
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("json schema producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let value_serializer = SchemaRegistryJsonSerializer::new(&SCHEMA, SerdeType::Value, serializer_config())?;
    println!("Schema Registry client created for {}.", REGISTRY.config().base_urls.join(","));

    let producer = create_kafka_producer(value_serializer)?;
    println!("Kafka producer created successfully.");

    let result = produce_records(&producer).await;
    let close_result = producer.close().await;
    result.and(close_result)
}

async fn produce_records(producer: &KafkaProducer<String, User>) -> Result<(), Error> {
    for (i, color) in COLORS.iter().cycle().take(NUM_RECORDS).enumerate() {
        let user = User {
            name: format!("user-{i}"),
            favorite_number: i64::try_from(i).expect("NUM_RECORDS fits in an i64"),
            favorite_color: (*color).to_string(),
        };
        println!("Producing record: key={}, value={user:?}", user.name);
        // The value serializer validates and encodes `user` inside `send`. The
        // first record registers the schema; later ones find its id in the
        // client's cache.
        let record = ProducerRecord::with_key(TOPIC_NAME.to_string(), Some(user.name.clone()), Some(user));

        // Synchronous produce: `send` returns a future for the record's delivery,
        // and awaiting `get` blocks until the broker has acknowledged it (or the
        // delivery has failed) before the next record is sent.
        let future = producer.send(record).await?;
        let metadata = future.get().await?;
        println!(
            "Delivered record {i} to topic {}, partition {} at offset {}",
            metadata.topic(),
            metadata.partition(),
            metadata.offset()
        );
    }
    Ok(())
}

/// Registers the schema on first use under the `<topic>-value` subject, the
/// registry's conventional naming, and validates every record against it.
/// This crate version defaults to the `Associated` strategy, which first asks
/// the registry for a topic association and only then falls back to `Topic`:
/// an extra round trip the example does not need.
fn serializer_config() -> SerializerConfig {
    SerializerConfig {
        subject_name_strategy_type: SubjectNameStrategyType::Topic,
        validate: true,
        ..SerializerConfig::default()
    }
}

fn bootstrap_servers() -> String {
    std::env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or_else(|_| "localhost:9092".to_string())
}

fn schema_registry_url() -> String {
    std::env::var("SCHEMA_REGISTRY_URL").unwrap_or_else(|_| "http://localhost:8081".to_string())
}

fn create_kafka_producer(
    value_serializer: SchemaRegistryJsonSerializer<User>,
) -> Result<KafkaProducer<String, User>, Error> {
    let props = HashMap::from([
        (ProducerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(), bootstrap_servers()),
        (
            ProducerConfig::CLIENT_ID_CONFIG.to_string(),
            format!("client-{}", Uuid::new_v4()),
        ),
    ]);

    println!("Kafka producer properties: {props:?}");

    let config = ProducerConfig::new(&props)?;
    KafkaProducer::new(config, StringSerializer::new(), value_serializer)
}

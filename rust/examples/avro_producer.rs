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

//! Produces Avro records whose schema lives in Confluent Schema Registry,
//! using Robert Yokota's `schema-registry-client` crate for the registry
//! round trip.
//!
//! The registry serializer is wrapped in an implementation of this client's
//! [`Serializer`] trait, [`SchemaRegistryAvroSerializer`], so the producer is
//! typed over the record type itself (`KafkaProducer<String, User>`) and
//! serializes each value inside `send`, like any other serializer.
//!
//! The registry serializer is `async`, because it may call the registry to
//! register or look up the schema, while the [`Serializer`] trait is
//! synchronous so it can run inline on the send path. The wrapper bridges the
//! two with `tokio::task::block_in_place`, which needs the multi-thread
//! runtime `#[tokio::main]` starts. Only the first record waits on the
//! network: the registry client caches the schema id, so every later call
//! completes without blocking.

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
use schema_registry_client::serdes::avro::AvroSerializer;
use schema_registry_client::serdes::config::SerializerConfig;
use schema_registry_client::serdes::serde::SerdeFormat;
use schema_registry_client::serdes::serde::SerdeType;
use schema_registry_client::serdes::serde::SerializationContext;
use schema_registry_client::serdes::serde::SubjectNameStrategyType;
use serde::Serialize;
use tokio::runtime::Handle;

use uuid::Uuid;

const TOPIC_NAME: &str = "avro-users";
const NUM_RECORDS: usize = 10;
const COLORS: [&str; 3] = ["red", "green", "blue"];

/// The Avro schema of the record values. The serializer registers it under
/// the subject `avro-users-value` the first time it is used.
const USER_SCHEMA: &str = r#"{
  "type": "record",
  "name": "User",
  "namespace": "io.confluent.examples",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "favorite_number", "type": "long"},
    {"name": "favorite_color", "type": "string"}
  ]
}"#;

/// The registry client and the schema the serializer borrows. The
/// [`Serializer`] trait object the producer owns must be `'static`, so what
/// it borrows lives for the whole program.
static REGISTRY: LazyLock<SchemaRegistryClient> =
    LazyLock::new(|| SchemaRegistryClient::new(ClientConfig::new(vec![schema_registry_url()])));
static SCHEMA: LazyLock<Schema> = LazyLock::new(|| Schema::new(Some("AVRO".to_string()), USER_SCHEMA.to_string()));

/// The Rust shape of a `User` record; `serde` maps its fields onto the
/// schema's.
#[derive(Debug, Serialize)]
struct User {
    name: String,
    favorite_number: i64,
    favorite_color: String,
}

/// A [`Serializer`] that writes `T` as Avro in the Confluent wire format: a
/// magic byte, the id the registry assigned to the schema, then the Avro
/// binary payload.
struct SchemaRegistryAvroSerializer<T> {
    inner: AvroSerializer<'static, SchemaRegistryClient>,
    serde_type: SerdeType,
    /// The runtime the registry calls run on, captured at construction.
    runtime: Handle,
    _record: PhantomData<fn(&T)>,
}

impl<T> SchemaRegistryAvroSerializer<T> {
    /// Must be called inside the Tokio runtime the producer runs on.
    fn new(schema: &'static Schema, serde_type: SerdeType, config: SerializerConfig) -> Result<Self, Error> {
        let inner = AvroSerializer::new(&*REGISTRY, Some(schema), None, config)
            .map_err(|e| Error::serialization(format!("cannot create the Avro serializer: {e}")))?;
        Ok(Self { inner, serde_type, runtime: Handle::current(), _record: PhantomData })
    }
}

impl<T: Serialize> Serializer<T> for SchemaRegistryAvroSerializer<T> {
    fn serialize(&self, topic: &str, data: Option<&T>) -> Result<Option<Vec<u8>>, Error> {
        // A null value stays null, as Java's serializers recommend.
        let Some(data) = data else { return Ok(None) };
        // The schema id travels in the payload prefix, so the record
        // headers are left untouched.
        let context = SerializationContext {
            topic: topic.to_string(),
            serde_type: self.serde_type,
            serde_format: SerdeFormat::Avro,
            headers: None,
        };
        // Blocks this worker thread until the registry answers, while
        // `block_in_place` hands its other tasks to the rest of the runtime.
        tokio::task::block_in_place(|| self.runtime.block_on(self.inner.serialize_ser(&context, data)))
            .map(Some)
            .map_err(|e| Error::serialization(format!("cannot serialize the Avro record for topic {topic}: {e}")))
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("avro producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let value_serializer = SchemaRegistryAvroSerializer::new(&SCHEMA, SerdeType::Value, serializer_config())?;
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
        // The value serializer encodes `user` inside `send`. The first record
        // registers the schema; later ones find its id in the client's cache.
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
/// registry's conventional naming. This crate version defaults to the
/// `Associated` strategy, which first asks the registry for a topic
/// association and only then falls back to `Topic`: an extra round trip the
/// example does not need.
fn serializer_config() -> SerializerConfig {
    SerializerConfig {
        subject_name_strategy_type: SubjectNameStrategyType::Topic,
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
    value_serializer: SchemaRegistryAvroSerializer<User>,
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
    KafkaProducer::new(config, Box::new(StringSerializer::new()), Box::new(value_serializer))
}

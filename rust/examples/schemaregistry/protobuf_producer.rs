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

//! Produces Protobuf records whose schema lives in Confluent Schema Registry,
//! using Robert Yokota's `schema-registry-client` crate for the registry
//! round trip.
//!
//! The registry serializer is wrapped in an implementation of this client's
//! [`Serializer`] trait, [`SchemaRegistryProtobufSerializer`], so the producer
//! is typed over the message type itself (`KafkaProducer<String, User>`) and
//! serializes each value inside `send`, like any other serializer.
//!
//! The registry stores a Protobuf schema as a file descriptor. A project
//! normally generates the message struct and its descriptor from a `.proto`
//! file with `prost-build` and `prost-reflect-build` in a build script. To
//! stay self-contained, this example writes both by hand for this schema:
//!
//! ```proto
//! syntax = "proto3";
//! package io.confluent.examples;
//!
//! message User {
//!   string name = 1;
//!   int64 favorite_number = 2;
//!   string favorite_color = 3;
//! }
//! ```
//!
//! Only the first record reaches the registry: the registry client caches the
//! schema id, so every later call completes without a network round trip.

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
use prost_reflect::prost_types::field_descriptor_proto::{Label, Type};
use prost_reflect::prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use prost_reflect::{DescriptorPool, MessageDescriptor, ReflectMessage};
use schema_registry_client::rest::client_config::ClientConfig;
use schema_registry_client::rest::schema_registry_client::Client;
use schema_registry_client::rest::schema_registry_client::SchemaRegistryClient;
use schema_registry_client::serdes::config::SerializerConfig;
use schema_registry_client::serdes::protobuf::ProtobufSerializer;
use schema_registry_client::serdes::serde::SerdeFormat;
use schema_registry_client::serdes::serde::SerdeType;
use schema_registry_client::serdes::serde::SerializationContext;
use schema_registry_client::serdes::serde::SubjectNameStrategyType;
use uuid::Uuid;

const TOPIC_NAME: &str = "protobuf-users";
const NUM_RECORDS: usize = 10;
const COLORS: [&str; 3] = ["red", "green", "blue"];

/// The registry client the serializer borrows. The [`Serializer`] trait
/// object the producer owns must be `'static`, so the client lives for the
/// whole program.
static REGISTRY: LazyLock<SchemaRegistryClient> =
    LazyLock::new(|| SchemaRegistryClient::new(ClientConfig::new(vec![schema_registry_url()])));

/// The `User` message, as `prost-build` would generate it from the schema.
#[derive(Clone, PartialEq, prost::Message)]
struct User {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(int64, tag = "2")]
    favorite_number: i64,
    #[prost(string, tag = "3")]
    favorite_color: String,
}

/// The descriptor of `User`, as `prost-reflect-build` would generate it:
/// built once, then cloned cheaply for each record.
static USER_DESCRIPTOR: LazyLock<MessageDescriptor> = LazyLock::new(|| {
    let field = |name: &str, number, field_type: Type| FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(field_type as i32),
        ..FieldDescriptorProto::default()
    };
    let file = FileDescriptorProto {
        name: Some("user.proto".to_string()),
        package: Some("io.confluent.examples".to_string()),
        syntax: Some("proto3".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("User".to_string()),
            field: vec![
                field("name", 1, Type::String),
                field("favorite_number", 2, Type::Int64),
                field("favorite_color", 3, Type::String),
            ],
            ..DescriptorProto::default()
        }],
        ..FileDescriptorProto::default()
    };
    DescriptorPool::from_file_descriptor_set(FileDescriptorSet { file: vec![file] })
        .expect("the User descriptor is valid")
        .get_message_by_name("io.confluent.examples.User")
        .expect("the descriptor declares User")
});

impl ReflectMessage for User {
    fn descriptor(&self) -> MessageDescriptor {
        USER_DESCRIPTOR.clone()
    }
}

/// A [`Serializer`] that writes `T` as Protobuf in the Confluent wire format:
/// a magic byte, the id the registry assigned to the schema, the indexes of
/// the message type within its file, then the Protobuf binary payload.
struct SchemaRegistryProtobufSerializer<T> {
    inner: ProtobufSerializer<'static, SchemaRegistryClient>,
    serde_type: SerdeType,
    _record: PhantomData<fn(&T)>,
}

impl<T> SchemaRegistryProtobufSerializer<T> {
    fn new(serde_type: SerdeType, config: SerializerConfig) -> Result<Self, Error> {
        let inner = ProtobufSerializer::new(&*REGISTRY, None, config)
            .map_err(|e| Error::serialization(format!("cannot create the Protobuf serializer: {e}")))?;
        Ok(Self { inner, serde_type, _record: PhantomData })
    }
}

impl<T: ReflectMessage + Sync> Serializer<T> for SchemaRegistryProtobufSerializer<T> {
    async fn serialize<'a>(&'a self, topic: &'a str, data: Option<&'a T>) -> Result<Option<Cow<'a, [u8]>>, Error> {
        // A null value stays null, as Java's serializers recommend.
        let Some(data) = data else { return Ok(None) };
        // The schema id travels in the payload prefix, so the record
        // headers are left untouched.
        let context = SerializationContext {
            topic: topic.to_string(),
            serde_type: self.serde_type,
            serde_format: SerdeFormat::Protobuf,
            headers: None,
        };
        // The schema comes from the message's own descriptor.
        self.inner
            .serialize(&context, data)
            .await
            .map(|bytes| Some(Cow::Owned(bytes)))
            .map_err(|e| Error::serialization(format!("cannot serialize the Protobuf record for topic {topic}: {e}")))
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("protobuf producer failed: {e}");
            ExitCode::FAILURE
        },
    }
}

async fn run() -> Result<(), Error> {
    let value_serializer = SchemaRegistryProtobufSerializer::new(SerdeType::Value, serializer_config())?;
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
    value_serializer: SchemaRegistryProtobufSerializer<User>,
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

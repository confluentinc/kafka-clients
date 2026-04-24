// Copyright 2025 Confluent Inc.
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

//! A factory abstraction over the three producer backends used by the
//! multilanguage integration tests.
//!
//! Test bodies are written generically over `<F: ProducerBackendFactory>`,
//! and the [`crate::common::multilanguage_test`] macro instantiates each
//! body once per backend (rust / python / c). There is no dynamic
//! dispatch — the trait carries an associated `Producer` type so each
//! instantiation gets the concrete producer for its backend.
//!
//! See `design/history/MILESTONE-6/DESIGN-multilanguage-tests.md`.

use std::collections::HashMap;

use confluent_kafka::common::KafkaError;
use confluent_kafka::common::serialization::ByteArraySerializer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
use tonic::transport::Channel;

use crate::common::multilanguage_producer::MultilanguageProducer;

/// Abstraction over a `Producer<Vec<u8>, Vec<u8>>` source.
///
/// Implementors construct a producer of their associated `Producer` type
/// from a flat `HashMap<String, String>` of config properties (the same
/// shape Python's `KafkaProducer(config: dict)` accepts and
/// `ProducerConfig::from_properties` parses). Each impl corresponds to
/// one backend in the multilanguage test matrix.
///
/// The map is taken by value because the gRPC factories forward it into
/// a proto `CreateProducerRequest`, which consumes the map.
#[allow(async_fn_in_trait)]
pub trait ProducerBackendFactory {
    /// The concrete producer type this backend constructs.
    type Producer: Producer<Vec<u8>, Vec<u8>> + Send;

    /// Construct a producer from the given config properties.
    async fn create(&self, config: HashMap<String, String>) -> Result<Self::Producer, KafkaError>;

    /// Short backend label used in test names and log messages.
    fn name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// RustNative — drives KafkaProducer<Vec<u8>, Vec<u8>> directly in-process.
// ---------------------------------------------------------------------------

/// Backend that constructs a native Rust `KafkaProducer<Vec<u8>, Vec<u8>>`.
/// This is the baseline against which the python and c backends are
/// compared.
pub struct RustNativeFactory;

impl ProducerBackendFactory for RustNativeFactory {
    type Producer = KafkaProducer<Vec<u8>, Vec<u8>>;

    async fn create(&self, config: HashMap<String, String>) -> Result<Self::Producer, KafkaError> {
        let producer_config = ProducerConfig::from_properties(&config)?;
        KafkaProducer::from_config(producer_config, Box::new(ByteArraySerializer), Box::new(ByteArraySerializer))
    }

    fn name(&self) -> &'static str {
        "rust"
    }
}

// ---------------------------------------------------------------------------
// PythonGrpc / CGrpc — both wrap a MultilanguageProducer pointed at their
// respective gRPC backend container.
// ---------------------------------------------------------------------------

/// Backend that drives bindings/python/producer.py through a gRPC server
/// running in the `confluent-kafka-rust/python-grpc-server:dev` Docker
/// image.
pub struct PythonGrpcFactory {
    channel: Channel,
}

impl PythonGrpcFactory {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }
}

impl ProducerBackendFactory for PythonGrpcFactory {
    type Producer = MultilanguageProducer;

    async fn create(&self, config: HashMap<String, String>) -> Result<Self::Producer, KafkaError> {
        MultilanguageProducer::new(self.channel.clone(), rewrite_for_container(config), "python").await
    }

    fn name(&self) -> &'static str {
        "python"
    }
}

/// Backend that drives the public C FFI through a gRPC server running in
/// the `confluent-kafka-rust/c-grpc-server:dev` Docker image (a thin
/// grpc++ wrapper that calls `kafka_producer_*`).
pub struct CGrpcFactory {
    channel: Channel,
}

impl CGrpcFactory {
    pub fn new(channel: Channel) -> Self {
        Self { channel }
    }
}

impl ProducerBackendFactory for CGrpcFactory {
    type Producer = MultilanguageProducer;

    async fn create(&self, config: HashMap<String, String>) -> Result<Self::Producer, KafkaError> {
        MultilanguageProducer::new(self.channel.clone(), rewrite_for_container(config), "c").await
    }

    fn name(&self) -> &'static str {
        "c"
    }
}

/// Rewrite the `bootstrap.servers` entry so it's reachable from inside the
/// gRPC client container.
///
/// The Rust test process holds host addresses like `127.0.0.1:32781` (the
/// random host port testcontainers mapped to the broker container). Inside
/// a sibling client container, `127.0.0.1` is the container itself; the
/// reachable name for the host is `host.docker.internal`, exposed via the
/// `--add-host=host.docker.internal:host-gateway` flag attached by the
/// backend pool.
fn rewrite_for_container(mut config: HashMap<String, String>) -> HashMap<String, String> {
    if let Some(bootstrap) = config.get("bootstrap.servers").cloned() {
        let rewritten = bootstrap
            .split(',')
            .map(|hp| {
                let trimmed = hp.trim();
                if let Some(port) = trimmed.strip_prefix("127.0.0.1:") {
                    format!("host.docker.internal:{port}")
                } else if let Some(port) = trimmed.strip_prefix("localhost:") {
                    format!("host.docker.internal:{port}")
                } else {
                    trimmed.to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(",");
        config.insert("bootstrap.servers".to_string(), rewritten);
    }
    config
}

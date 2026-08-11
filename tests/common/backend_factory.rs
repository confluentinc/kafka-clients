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

//! A factory abstraction over the backends used by the multilanguage
//! integration tests (native Rust, Python sync, Python asyncio, C).
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
use confluent_kafka::common::serialization::{ByteArrayDeserializer, ByteArraySerializer};
use confluent_kafka::consumer::Consumer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::new_consumer;
use confluent_kafka::producer::KafkaProducer;
use confluent_kafka::producer::Producer;
use confluent_kafka::producer::ProducerConfig;
#[cfg(feature = "multilanguage-tests")]
use tonic::transport::Channel;

use crate::common::admin_backend::{AdminBackend, RustNativeAdmin};
#[cfg(feature = "multilanguage-tests")]
use crate::common::multilanguage_admin::MultilanguageAdmin;
#[cfg(feature = "multilanguage-tests")]
use crate::common::multilanguage_consumer::MultilanguageConsumer;
#[cfg(feature = "multilanguage-tests")]
use crate::common::multilanguage_producer::MultilanguageProducer;

/// Abstraction over an [`AdminBackend`] source.
///
/// Shaped like [`ProducerBackendFactory`] (an associated type, static dispatch)
/// rather than like [`ConsumerBackendFactory`]: `AdminBackend`'s methods are
/// `async fn` in the trait, which is not dyn-compatible, so there is no
/// `Box<dyn AdminBackend>` to hand back. Test bodies are written generically
/// over `<F: AdminBackendFactory>` and quadruplicated by
/// [`crate::multilanguage_admin_test`].
#[allow(async_fn_in_trait)]
pub trait AdminBackendFactory {
    /// The concrete admin backend this factory constructs.
    type Admin: AdminBackend;

    /// Construct a network-backed admin client from the given config
    /// properties.
    async fn create(&self, config: HashMap<String, String>) -> Result<Self::Admin, KafkaError>;

    /// Construct a broker-less mock admin client with `num_brokers` brokers
    /// (Java `MockAdminClient.create().numBrokers(n).build()`).
    async fn create_mock(&self, num_brokers: i32) -> Result<Self::Admin, KafkaError>;

    /// Short backend label used in test names and log messages.
    fn name(&self) -> &'static str;

    /// Whether this backend needs the container-internal bootstrap addresses
    /// (true for the gRPC backends). See
    /// [`ProducerBackendFactory::needs_container_bootstrap`].
    fn needs_container_bootstrap(&self) -> bool {
        false
    }
}

/// Abstraction over a `Box<dyn Consumer<Vec<u8>, Vec<u8>>>` source.
///
/// Unlike [`ProducerBackendFactory`] (which uses an associated type for static
/// dispatch), the consumer trait is dispatched dynamically as
/// `Box<dyn Consumer>` everywhere (see `consumer-threading.md` §2), so every
/// backend simply yields a boxed consumer. Test bodies are written generically
/// over `<F: ConsumerBackendFactory>` and triplicated by
/// [`crate::multilanguage_consumer_test`].
#[allow(async_fn_in_trait)]
pub trait ConsumerBackendFactory {
    /// Construct a consumer from the given config properties.
    async fn create(&self, config: HashMap<String, String>) -> Result<Box<dyn Consumer<Vec<u8>, Vec<u8>>>, KafkaError>;

    /// Short backend label used in test names and log messages.
    fn name(&self) -> &'static str;

    /// Whether this backend needs the container-internal bootstrap addresses
    /// (true for the gRPC backends). See [`ProducerBackendFactory::needs_container_bootstrap`].
    fn needs_container_bootstrap(&self) -> bool {
        false
    }
}

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

    /// Whether this backend needs the container-internal bootstrap
    /// addresses (true for the gRPC backends, since the broker's
    /// host-loopback addresses aren't reachable from inside their
    /// containers). Tests pick between
    /// [`TestContext::bootstrap_servers`] and
    /// [`TestContext::container_bootstrap_servers`] based on this.
    fn needs_container_bootstrap(&self) -> bool {
        false
    }
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

impl AdminBackendFactory for RustNativeFactory {
    type Admin = RustNativeAdmin;

    async fn create(&self, config: HashMap<String, String>) -> Result<Self::Admin, KafkaError> {
        RustNativeAdmin::from_config(&config)
    }

    async fn create_mock(&self, num_brokers: i32) -> Result<Self::Admin, KafkaError> {
        Ok(RustNativeAdmin::mock(num_brokers))
    }

    fn name(&self) -> &'static str {
        "rust"
    }
}

impl ConsumerBackendFactory for RustNativeFactory {
    async fn create(&self, config: HashMap<String, String>) -> Result<Box<dyn Consumer<Vec<u8>, Vec<u8>>>, KafkaError> {
        let consumer_config = ConsumerConfig::from_properties(&config)?;
        new_consumer::<Vec<u8>, Vec<u8>>(
            consumer_config,
            Box::new(ByteArrayDeserializer),
            Box::new(ByteArrayDeserializer),
        )
    }

    fn name(&self) -> &'static str {
        "rust"
    }
}

// ---------------------------------------------------------------------------
// PythonGrpc / CGrpc — both wrap a MultilanguageProducer pointed at their
// respective gRPC backend container. Gated on multilanguage-tests since
// they pull in tonic + the proto crate.
// ---------------------------------------------------------------------------

#[cfg(feature = "multilanguage-tests")]
mod grpc_backends {
    use super::*;

    /// Backend that drives bindings/python/producer.py through a gRPC
    /// server running in the
    /// `confluent-kafka-rust/python-grpc-server:dev` Docker image.
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
            // Caller is responsible for passing a container-reachable
            // bootstrap — see TestContext::container_bootstrap_servers.
            MultilanguageProducer::new(self.channel.clone(), config, "python").await
        }

        fn name(&self) -> &'static str {
            "python"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    impl ConsumerBackendFactory for PythonGrpcFactory {
        async fn create(
            &self,
            config: HashMap<String, String>,
        ) -> Result<Box<dyn Consumer<Vec<u8>, Vec<u8>>>, KafkaError> {
            Ok(Box::new(
                MultilanguageConsumer::new(self.channel.clone(), config, "python").await?,
            ))
        }

        fn name(&self) -> &'static str {
            "python"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    impl AdminBackendFactory for PythonGrpcFactory {
        type Admin = MultilanguageAdmin;

        async fn create(&self, config: HashMap<String, String>) -> Result<Self::Admin, KafkaError> {
            MultilanguageAdmin::new(self.channel.clone(), config, "python").await
        }

        async fn create_mock(&self, num_brokers: i32) -> Result<Self::Admin, KafkaError> {
            MultilanguageAdmin::new_mock(self.channel.clone(), num_brokers, "python").await
        }

        fn name(&self) -> &'static str {
            "python"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    /// Backend that drives the *asyncio-native* bindings/python client
    /// (`AsyncKafkaProducer` / `AsyncKafkaConsumer`) through a gRPC server
    /// running in the `confluent-kafka-rust/python-async-grpc-server:dev`
    /// Docker image. Identical wiring to [`PythonGrpcFactory`] — only the
    /// backend label (used in logs) differs.
    pub struct PythonAsyncGrpcFactory {
        channel: Channel,
    }

    impl PythonAsyncGrpcFactory {
        pub fn new(channel: Channel) -> Self {
            Self { channel }
        }
    }

    impl ProducerBackendFactory for PythonAsyncGrpcFactory {
        type Producer = MultilanguageProducer;

        async fn create(&self, config: HashMap<String, String>) -> Result<Self::Producer, KafkaError> {
            MultilanguageProducer::new(self.channel.clone(), config, "python_async").await
        }

        fn name(&self) -> &'static str {
            "python_async"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    impl ConsumerBackendFactory for PythonAsyncGrpcFactory {
        async fn create(
            &self,
            config: HashMap<String, String>,
        ) -> Result<Box<dyn Consumer<Vec<u8>, Vec<u8>>>, KafkaError> {
            Ok(Box::new(
                MultilanguageConsumer::new(self.channel.clone(), config, "python_async").await?,
            ))
        }

        fn name(&self) -> &'static str {
            "python_async"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    impl AdminBackendFactory for PythonAsyncGrpcFactory {
        type Admin = MultilanguageAdmin;

        async fn create(&self, config: HashMap<String, String>) -> Result<Self::Admin, KafkaError> {
            MultilanguageAdmin::new(self.channel.clone(), config, "python_async").await
        }

        async fn create_mock(&self, num_brokers: i32) -> Result<Self::Admin, KafkaError> {
            MultilanguageAdmin::new_mock(self.channel.clone(), num_brokers, "python_async").await
        }

        fn name(&self) -> &'static str {
            "python_async"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    /// Backend that drives the public C FFI through a gRPC server running
    /// in the `confluent-kafka-rust/c-grpc-server:dev` Docker image (a
    /// thin grpc++ wrapper that calls `kafka_producer_*`).
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
            MultilanguageProducer::new(self.channel.clone(), config, "c").await
        }

        fn name(&self) -> &'static str {
            "c"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    impl ConsumerBackendFactory for CGrpcFactory {
        async fn create(
            &self,
            config: HashMap<String, String>,
        ) -> Result<Box<dyn Consumer<Vec<u8>, Vec<u8>>>, KafkaError> {
            Ok(Box::new(MultilanguageConsumer::new(self.channel.clone(), config, "c").await?))
        }

        fn name(&self) -> &'static str {
            "c"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }

    impl AdminBackendFactory for CGrpcFactory {
        type Admin = MultilanguageAdmin;

        async fn create(&self, config: HashMap<String, String>) -> Result<Self::Admin, KafkaError> {
            MultilanguageAdmin::new(self.channel.clone(), config, "c").await
        }

        async fn create_mock(&self, num_brokers: i32) -> Result<Self::Admin, KafkaError> {
            MultilanguageAdmin::new_mock(self.channel.clone(), num_brokers, "c").await
        }

        fn name(&self) -> &'static str {
            "c"
        }

        fn needs_container_bootstrap(&self) -> bool {
            true
        }
    }
}

#[cfg(feature = "multilanguage-tests")]
#[allow(unused_imports)] // Used only by the `integration` test binary
pub use grpc_backends::{CGrpcFactory, PythonAsyncGrpcFactory, PythonGrpcFactory};

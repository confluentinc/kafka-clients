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

//! The public entry point for constructing a [`Consumer`].
//!
//! Translated from `org.apache.kafka.clients.consumer.KafkaConsumer`.

use crate::common::Error;
use crate::common::serialization::Deserializer;
use crate::consumer::{Consumer, ConsumerConfig, GroupProtocol, async_kafka_consumer};

/// The public entry point for constructing a [`Consumer`].
///
/// Translates `org.apache.kafka.clients.consumer.KafkaConsumer`, whose public
/// constructors are the only way a Java user obtains a consumer. Java's class
/// is a thin delegating wrapper: its constructor calls
/// `ConsumerDelegateCreator.create(config, keyDeserializer, valueDeserializer)`
/// (`KafkaConsumer.java:615`), which switches on `group.protocol` and returns
/// either an `AsyncKafkaConsumer` or a `ClassicKafkaConsumer`
/// (`ConsumerDelegateCreator.java:57-66`); its 55 other methods forward
/// verbatim to that delegate.
///
/// Rust keeps only the constructor. The delegate is returned directly as
/// `Box<dyn Consumer<K, V>>` rather than being stored in a wrapper that
/// re-forwards every method, because [`Consumer`] is dyn compatible — so the
/// caller holds exactly what Java's wrapper would have held, and the 55
/// forwarding bodies carry no behavior to translate. `ConsumerDelegate` and
/// `ConsumerDelegateCreator` are out of scope per `consumer-threading.md` §20
/// ("collapses to direct `Box::new(AsyncKafkaConsumer)`"); this type is where
/// that collapsed creator lives.
pub struct KafkaConsumer;

impl KafkaConsumer {
    /// Constructs a new [`Consumer`] from a configuration and explicit
    /// key/value [`Deserializer`]s.
    ///
    /// For `group.protocol=consumer` (KIP-848), this returns
    /// `Box::new(AsyncKafkaConsumer::new(...)?)` — the production consumer
    /// built end-to-end with `SubscriptionState`, `ConsumerMetadata`,
    /// `NetworkClient`, every `RequestManager`, and a single bg task
    /// (`ConsumerNetworkThread`). For `group.protocol=classic`, returns
    /// [`Error::unsupported_version`] per `consumer-threading.md` §20
    /// (classic protocol deferred to a later milestone).
    ///
    /// Java passes deserializers via `ConsumerConfig` reflection; Rust takes
    /// them as explicit `Box<dyn>` parameters (Phase 1 decision not to
    /// translate reflection machinery). The consumer wraps them in
    /// `Arc<Deserializers<K, V>>` internally for sharing with
    /// `Fetcher`/`FetchCollector` (Shape B).
    ///
    /// `MockConsumer` (Phase 3) does NOT come through this factory — it has
    /// its own constructor. The factory is for the production consumer only.
    // `new` deliberately does not return `Self`: Java's `KafkaConsumer`
    // constructor hands back the delegate that `ConsumerDelegateCreator` chose
    // (`KafkaConsumer.java:615`), and Rust returns it as `Box<dyn Consumer<K, V>>`
    // rather than re-wrapping it in a type whose only content is 55 forwarding
    // methods. See the type-level comment above.
    #[allow(clippy::new_ret_no_self)]
    pub fn new<K, V>(
        config: ConsumerConfig,
        key_deserializer: Box<dyn Deserializer<K>>,
        value_deserializer: Box<dyn Deserializer<V>>,
    ) -> Result<Box<dyn Consumer<K, V>>, Error>
    where
        K: Send + Sync + 'static,
        V: Send + Sync + 'static,
    {
        // Phase 12 commit (4/N) wires the `GroupProtocol::Consumer` arm to
        // the production constructor at
        // [`async_kafka_consumer::AsyncKafkaConsumer::new`], which translates
        // the Java primary constructor at `AsyncKafkaConsumer.java:285-518`
        // end-to-end. The ctor builds the full dependency closure
        // (`SubscriptionState`, `ConsumerMetadata`, `NetworkClient` +
        // PLAINTEXT `ChannelBuilder`, every `RequestManager`,
        // `ApplicationEventHandler`, `ConsumerNetworkThread` bg task) and
        // hands off to `AsyncKafkaConsumer::with_components` so the
        // Phase-11 test seam is preserved. `Box<dyn Consumer<K, V>>` is
        // returned so the dispatch surface stays object-safe (Consumer
        // trait surface check at `tests/consumer/trait_surface_check.rs`).
        //
        // `GroupProtocol::Classic` remains an `unsupported_version` error
        // per `consumer-threading.md` §20 (classic protocol deferred to a
        // later milestone).
        let protocol = GroupProtocol::of(config.group_protocol())?;
        match protocol {
            GroupProtocol::Consumer => Ok(Box::new(async_kafka_consumer::AsyncKafkaConsumer::<K, V>::new(
                config,
                key_deserializer,
                value_deserializer,
            )?)),
            GroupProtocol::Classic => Err(Error::unsupported_version(
                "Classic group protocol is not yet supported in this client; \
                 set group.protocol=consumer (KIP-848).",
            )),
        }
    }
}

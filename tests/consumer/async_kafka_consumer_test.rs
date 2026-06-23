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

//! Unit-level smoke tests for the production `AsyncKafkaConsumer` ctor
//! reachable via the `new_consumer` factory (Phase 12 commit (4/N)).
//!
//! These tests deliberately do NOT require a live broker — they target
//! a closed localhost port. The Kafka client's metadata bootstrap path
//! does not synchronously connect, so the ctor succeeds even when the
//! peer refuses connection. This exercises:
//!
//! 1. The `new_consumer::<K, V>(...)` factory wires the
//!    `GroupProtocol::Consumer` arm through to
//!    `AsyncKafkaConsumer::new(config, kd, vd)`.
//! 2. The full RequestManagers + bg-task scaffold builds and spawns
//!    without panic.
//! 3. `Box<dyn Consumer<K, V>>` dispatch works for sync accessors
//!    (`client_id`).
//! 4. `close().await` cleanly tears down the bg task with no broker.
//!
//! Docker-free coverage; the integration tests in
//! `tests/integration/consumer_test.rs` exercise the same path against
//! a real broker.

use std::collections::HashMap;

use confluent_kafka::common::Errors;
use confluent_kafka::common::KafkaError;
use confluent_kafka::common::serialization::Deserializer;
use confluent_kafka::consumer::ConsumerConfig;
use confluent_kafka::consumer::new_consumer;

/// Local string deserializer for tests — `tests/integration` uses a
/// similar inline impl pending a shared `StringDeserializer` in
/// `common::serialization`. Returns the input bytes as a UTF-8 `String`.
struct StringDeserializer;

impl Deserializer<String> for StringDeserializer {
    fn deserialize(&self, _topic: &str, data: &[u8]) -> Result<String, KafkaError> {
        String::from_utf8(data.to_vec()).map_err(|e| KafkaError::serialization(format!("invalid utf-8: {}", e)))
    }
}

/// Build a `ConsumerConfig` pointing at a closed-on-purpose localhost
/// port. `127.0.0.1:1` is reliably-refused on every CI runner.
fn make_smoke_config() -> ConsumerConfig {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "127.0.0.1:1".to_string()),
        ("group.id".to_string(), "smoke-test-group".to_string()),
        ("group.protocol".to_string(), "consumer".to_string()),
        ("client.id".to_string(), "smoke-test-consumer".to_string()),
        ("auto.offset.reset".to_string(), "earliest".to_string()),
        ("enable.auto.commit".to_string(), "false".to_string()),
    ]);
    ConsumerConfig::from_properties(&props).expect("smoke-test config should validate")
}

/// Smoke: the production ctor succeeds against a refuses-connection
/// broker (metadata.bootstrap is non-connecting), trait dispatch works
/// for `client_id()`, and `close().await` cleanly tears down the bg
/// task. Covers commit (4/N) of Phase 12.
#[tokio::test(flavor = "multi_thread")]
async fn new_consumer_builds_and_closes_against_refused_broker() {
    let mut consumer =
        new_consumer::<String, String>(make_smoke_config(), Box::new(StringDeserializer), Box::new(StringDeserializer))
            .expect("new_consumer should succeed even without a reachable broker");

    // Trait dispatch through `Box<dyn Consumer<K, V>>`. `client_id` is
    // the Phase-11-translated sync accessor.
    assert_eq!(
        consumer.client_id(),
        "smoke-test-consumer",
        "client_id() should round-trip the config value",
    );

    // Bg-task graceful shutdown: signal close + drop the join handle.
    // Should NOT hang even though we never reached a broker.
    consumer
        .close()
        .await
        .expect("close should succeed against a never-connected broker");
}

/// Smoke: `GroupProtocol::Classic` continues to return
/// `KafkaError::unsupported_version` after the Phase-12 factory swap.
/// Regression guard for the classic-protocol gate at
/// `src/consumer/mod.rs`.
#[tokio::test(flavor = "multi_thread")]
async fn new_consumer_rejects_classic_group_protocol() {
    let props = HashMap::from([
        ("bootstrap.servers".to_string(), "127.0.0.1:1".to_string()),
        ("group.protocol".to_string(), "classic".to_string()),
    ]);
    let config = ConsumerConfig::from_properties(&props).expect("config should validate");

    let result = new_consumer::<String, String>(config, Box::new(StringDeserializer), Box::new(StringDeserializer));

    let err = result.err().expect("classic protocol must be rejected");
    assert_eq!(
        err.error(),
        Errors::UnsupportedVersion,
        "expected UnsupportedVersion, got: {:?}",
        err
    );
}

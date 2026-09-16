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

//! Translated from `org.apache.kafka.clients.consumer.ConsumerConfigTest`.
//!
//! Skipped tests:
//! - `testOverrideClientId`, `testOverrideEnableAutoCommit` — exercise
//!   Java's `postProcessParsedConfig`, which is deferred to a later phase
//!   per PLAN.md "Cross-field validation".
//! - `testAppendDeserializerToConfig`,
//!   `testAppendDeserializerToConfigWithException` — exercise the Java
//!   static `appendDeserializerToConfig` helper that mutates a typed
//!   `Map<String, Object>`. Rust code typically sets deserializers via the
//!   fluent setters on `ConsumerConfig`; there is no equivalent function
//!   to test.
//! - `testRemoteAssignorWithClassicGroupProtocol`,
//!   `testUnsupportedConfigsWithConsumerGroupProtocol` — cross-field
//!   validation, deferred (consumer-threading.md §20: classic-protocol-only
//!   keys are accepted silently for now).
//! - `testValidateConfigPropertiesFile` — reads `config/consumer.properties`
//!   from the Apache Kafka source tree; not part of the Rust client.

use std::collections::HashMap;

use confluent_kafka::consumer::ConsumerConfig;

/// Java's `ConsumerConfigTest` seeds `key.deserializer` / `value.deserializer`
/// into every property map (`ConsumerConfigTest.java:60`) because Java's
/// `ConfigDef` defines them with no default, so `new ConsumerConfig(props)`
/// throws without them. Rust takes the deserializers as constructor arguments
/// to `AsyncKafkaConsumer::new` instead, and `ConsumerConfig` holds no
/// deserializer state at all, so the two keys are simply absent here.
fn base_props() -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert(
        ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
        "localhost:9092".to_string(),
    );
    props
}

/// Translated from
/// `ConsumerConfigTest.ensureDefaultThrowOnUnsupportedStableFlagToFalse`.
#[test]
fn ensure_default_throw_on_unsupported_stable_flag_to_false() {
    let config = ConsumerConfig::new(&base_props()).unwrap();
    assert!(!config.throw_on_fetch_stable_offset_unsupported());
}

/// Translated from `ConsumerConfigTest.testDefaultPartitionAssignor`.
///
/// `partition.assignment.strategy` is a classic-protocol-only key in Java.
/// Per consumer-threading.md §20 we accept it silently and store it
/// untyped (as a list of class-name strings). Its default is empty in the
/// Rust struct (Java's default is `[RangeAssignor, CooperativeStickyAssignor]`,
/// but the actual values are class objects which we don't translate).
#[test]
fn test_default_partition_assignor_is_accepted() {
    let config = ConsumerConfig::new(&base_props()).unwrap();
    // Default is empty in the Rust struct; the actual class-name list is
    // tracked silently for forward compatibility.
    assert!(config.partition_assignment_strategy().is_empty());
}

/// Translated from `ConsumerConfigTest.testInvalidGroupInstanceId`.
///
/// Asserts that the `group.instance.id` empty-string check is enforced.
#[test]
fn test_invalid_group_instance_id() {
    let mut props = base_props();
    props.insert(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG.to_string(), String::new());
    let err = ConsumerConfig::new(&props).unwrap_err();
    assert!(
        err.message().contains(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG),
        "error message should mention the failing config key, got: {}",
        err.message()
    );
}

/// Translated from `ConsumerConfigTest.testInvalidSecurityProtocol`.
#[test]
fn test_invalid_security_protocol() {
    let mut props = base_props();
    props.insert(ConsumerConfig::SECURITY_PROTOCOL_CONFIG.to_string(), "abc".to_string());
    let err = ConsumerConfig::new(&props).unwrap_err();
    assert!(
        err.message().contains(ConsumerConfig::SECURITY_PROTOCOL_CONFIG),
        "error message should mention the failing config key, got: {}",
        err.message()
    );
}

/// Translated from `ConsumerConfigTest.testCaseInsensitiveSecurityProtocol`.
#[test]
fn test_case_insensitive_security_protocol() {
    let mut props = base_props();
    props.insert(ConsumerConfig::SECURITY_PROTOCOL_CONFIG.to_string(), "sasl_ssl".to_string());
    let config = ConsumerConfig::new(&props).unwrap();
    // A lowercase value is parsed case-insensitively into the canonical
    // `SecurityProtocol`, mirroring Java's `SecurityProtocol.forName` and the
    // producer's `testCaseInsensitiveSecurityProtocol`. The accessor returns
    // the canonical uppercase name.
    assert_eq!(config.security_protocol(), "SASL_SSL");
}

/// Translated from `ConsumerConfigTest.testDefaultConsumerGroupConfig`.
///
/// Java default for `group.protocol` is `"classic"` (per Kafka 4.2;
/// `ConsumerConfig.DEFAULT_GROUP_PROTOCOL`); `group.remote.assignor` is
/// `null`.
#[test]
fn test_default_consumer_group_config() {
    let config = ConsumerConfig::new(&base_props()).unwrap();
    assert_eq!(config.group_protocol(), "classic");
    assert_eq!(config.group_remote_assignor(), None);
}

/// Translated from `ConsumerConfigTest.testRemoteAssignorConfig`.
#[test]
fn test_remote_assignor_config() {
    let mut props = base_props();
    let remote_assignor_name = "SomeAssignor";
    let protocol = "consumer";
    props.insert(
        ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG.to_string(),
        remote_assignor_name.to_string(),
    );
    props.insert(ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), protocol.to_string());
    let config = ConsumerConfig::new(&props).unwrap();
    assert_eq!(config.group_protocol(), protocol);
    assert_eq!(config.group_remote_assignor(), Some(remote_assignor_name));
}

/// Translated from `ConsumerConfigTest.testDefaultMetadataRecoveryStrategy`.
#[test]
fn test_default_metadata_recovery_strategy() {
    let config = ConsumerConfig::new(&base_props()).unwrap();
    assert_eq!(config.metadata_recovery_strategy(), "rebootstrap");
}

/// Translated from `ConsumerConfigTest.testInvalidMetadataRecoveryStrategy`.
#[test]
fn test_invalid_metadata_recovery_strategy() {
    let mut props = base_props();
    props.insert(ConsumerConfig::METADATA_RECOVERY_STRATEGY_CONFIG.to_string(), "abc".to_string());
    let err = ConsumerConfig::new(&props).unwrap_err();
    assert!(
        err.message().contains(ConsumerConfig::METADATA_RECOVERY_STRATEGY_CONFIG),
        "error message should mention the failing config key, got: {}",
        err.message()
    );
}

/// Translated from `ConsumerConfigTest.testProtocolConfigValidation`.
///
/// Java uses `@ParameterizedTest @CsvSource(...)`; here we iterate over the
/// same inputs.
#[test]
fn test_protocol_config_validation() {
    let cases: &[(&str, bool)] = &[
        ("consumer", true),
        ("classic", true),
        ("Consumer", true),
        ("Classic", true),
        ("invalid", false),
    ];
    for (protocol, is_valid) in cases {
        let mut props = base_props();
        props.insert(ConsumerConfig::GROUP_PROTOCOL_CONFIG.to_string(), protocol.to_string());
        if *is_valid {
            let config = ConsumerConfig::new(&props).unwrap();
            assert_eq!(config.group_protocol(), *protocol);
        } else {
            let err = ConsumerConfig::new(&props).unwrap_err();
            assert!(
                err.message().contains(ConsumerConfig::GROUP_PROTOCOL_CONFIG),
                "case {protocol}: {}",
                err.message()
            );
        }
    }
}

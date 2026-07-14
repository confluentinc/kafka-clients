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

//! Share-consumer configuration.
//!
//! Translates `org.apache.kafka.clients.consumer.ShareConsumerConfig`
//! (Apache Kafka 4.2). Java's `ShareConsumerConfig extends ConsumerConfig`
//! and overrides `preProcessParsedConfig` to reject a set of consumer configs
//! that are not supported by share groups. Rust's [`ConsumerConfig`] is not
//! subclassable, so `ShareConsumerConfig` is a thin newtype that performs the
//! unsupported-config validation up-front and then delegates parsing to
//! [`ConsumerConfig::from_properties`].

use std::collections::HashMap;

use crate::common::KafkaError;
use crate::consumer::consumer_config::ConsumerConfig;

/// The share-consumer configuration.
///
/// Wraps a [`ConsumerConfig`] and rejects the configuration keys that are not
/// supported for a share group (Java `ShareConsumerConfig.SHARE_GROUP_UNSUPPORTED_CONFIGS`).
pub struct ShareConsumerConfig {
    inner: ConsumerConfig,
}

impl ShareConsumerConfig {
    /// A list of configuration keys not supported for a SHARE consumer.
    ///
    /// Java: `ShareConsumerConfig.SHARE_GROUP_UNSUPPORTED_CONFIGS`.
    pub(crate) const SHARE_GROUP_UNSUPPORTED_CONFIGS: [&'static str; 10] = [
        ConsumerConfig::AUTO_OFFSET_RESET_CONFIG,
        ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG,
        ConsumerConfig::GROUP_INSTANCE_ID_CONFIG,
        ConsumerConfig::ISOLATION_LEVEL_CONFIG,
        ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
        ConsumerConfig::INTERCEPTOR_CLASSES_CONFIG,
        ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG,
        ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG,
        ConsumerConfig::GROUP_PROTOCOL_CONFIG,
        ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG,
    ];

    /// Parses a string-typed property map into a `ShareConsumerConfig`.
    ///
    /// Mirrors Java's `new ShareConsumerConfig(Map<String, Object>)`: the
    /// unsupported-config pre-process check (`checkUnsupportedConfigsPreProcess`)
    /// runs first, then the properties are parsed via
    /// [`ConsumerConfig::from_properties`].
    ///
    /// # Errors
    ///
    /// Returns [`KafkaError::illegal_argument`] (the Rust analog of Java's
    /// `ConfigException`) if any share-group-unsupported config is present, or
    /// if [`ConsumerConfig::from_properties`] fails.
    pub fn from_properties(props: &HashMap<String, String>) -> Result<Self, KafkaError> {
        Self::check_unsupported_configs_pre_process(props)?;
        Ok(Self { inner: ConsumerConfig::from_properties(props)? })
    }

    /// Java: `checkUnsupportedConfigsPreProcess`.
    fn check_unsupported_configs_pre_process(props: &HashMap<String, String>) -> Result<(), KafkaError> {
        let invalid_configs: Vec<&str> = Self::SHARE_GROUP_UNSUPPORTED_CONFIGS
            .iter()
            .copied()
            .filter(|name| props.contains_key(*name))
            .collect();
        if !invalid_configs.is_empty() {
            return Err(KafkaError::illegal_argument(format!(
                "{} cannot be set when using a share group.",
                invalid_configs.join(", ")
            )));
        }
        Ok(())
    }

    /// Borrows the underlying [`ConsumerConfig`].
    pub fn as_consumer_config(&self) -> &ConsumerConfig {
        &self.inner
    }

    /// Consumes this wrapper, returning the underlying [`ConsumerConfig`].
    pub fn into_consumer_config(self) -> ConsumerConfig {
        self.inner
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java `ShareConsumerConfigTest.testUnsupportedShareConsumerConfigs`.
    #[test]
    fn test_unsupported_share_consumer_configs() {
        verify_unsupported_share_consumer_config(ConsumerConfig::AUTO_OFFSET_RESET_CONFIG, "earliest");
        verify_unsupported_share_consumer_config(ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG, "true");
        verify_unsupported_share_consumer_config(ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, "1");
        verify_unsupported_share_consumer_config(ConsumerConfig::ISOLATION_LEVEL_CONFIG, "read_committed");
        verify_unsupported_share_consumer_config(
            ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.StickyAssignor",
        );
        verify_unsupported_share_consumer_config(
            ConsumerConfig::INTERCEPTOR_CLASSES_CONFIG,
            "org.apache.kafka.clients.consumer.ConsumerInterceptor",
        );
        verify_unsupported_share_consumer_config(ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG, "3000");
        verify_unsupported_share_consumer_config(ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        verify_unsupported_share_consumer_config(ConsumerConfig::GROUP_PROTOCOL_CONFIG, "classic");
        verify_unsupported_share_consumer_config(ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, "null");
    }

    fn verify_unsupported_share_consumer_config(key: &str, value: &str) {
        let mut props = HashMap::new();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), "1".to_string());
        props.insert(
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            "localhost:9092".to_string(),
        );
        props.insert(key.to_string(), value.to_string());
        match ShareConsumerConfig::from_properties(&props) {
            Ok(_) => panic!("expected ConfigException for unsupported key {key}"),
            Err(err) => assert!(
                err.to_string().contains("cannot be set when using a share group"),
                "unexpected error: {err}"
            ),
        }
    }

    /// A property map with no unsupported keys parses successfully.
    #[test]
    fn test_supported_configs_parse() {
        let mut props = HashMap::new();
        props.insert(ConsumerConfig::GROUP_ID_CONFIG.to_string(), "group-id".to_string());
        props.insert(
            ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
            "localhost:9092".to_string(),
        );
        props.insert(
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG.to_string(),
            "explicit".to_string(),
        );
        let config = match ShareConsumerConfig::from_properties(&props) {
            Ok(c) => c,
            Err(err) => panic!("config should parse: {err}"),
        };
        assert_eq!(config.as_consumer_config().group_id(), Some("group-id"));
    }
}

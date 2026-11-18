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

//! The configuration of a share consumer.
//!
//! Translated from `org.apache.kafka.clients.consumer.ShareConsumerConfig`.

use std::collections::HashMap;
use std::ops::Deref;

use crate::common::Error;
use crate::common::config::abstract_config::{AbstractConfig, ConfigMap, string_originals};
use crate::consumer::ConsumerConfig;

/// `interceptor.classes`.
const INTERCEPTOR_CLASSES_CONFIG: &str = "interceptor.classes";

/// A list of configuration keys not supported for SHARE consumer.
const SHARE_GROUP_UNSUPPORTED_CONFIGS: [&str; 10] = [
    ConsumerConfig::AUTO_OFFSET_RESET_CONFIG,
    ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG,
    ConsumerConfig::GROUP_INSTANCE_ID_CONFIG,
    ConsumerConfig::ISOLATION_LEVEL_CONFIG,
    ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
    // Java's `ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG`. The key takes a list
    // of classes, so `ConsumerConfig` does not define it (CLAUDE.md §2), but
    // the check reads the originals, where a user can still set it.
    INTERCEPTOR_CLASSES_CONFIG,
    ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG,
    ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG,
    ConsumerConfig::GROUP_PROTOCOL_CONFIG,
    ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG,
];

/// The consumer configuration behavior specific to share groups.
///
/// Java's class is package-private and extends `ConsumerConfig`, overriding
/// two hooks: `preProcessParsedConfig` rejects the keys a share group does
/// not support, and `checkUnsupportedConfigsPostProcess` does nothing, so the
/// share keys `ConsumerConfig` rejects under either group protocol are
/// accepted. Rust has no subclass: this type holds the `ConsumerConfig` the
/// two hooks built and dereferences to it, which is how the share consumer
/// reads it, as Java's reads its `ConsumerConfig` parameter.
#[derive(Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumerConfig")]
pub(crate) struct ShareConsumerConfig {
    config: ConsumerConfig,
}

impl ShareConsumerConfig {
    /// Java's `ShareConsumerConfig(Map)`: parses `props` as
    /// [`ConsumerConfig::new`] does, with the two share-group hooks.
    ///
    /// # Errors
    ///
    /// The errors of [`ConsumerConfig::new`] other than an unsupported key for
    /// the `group.protocol`, and `Error::Config` with
    /// `"<keys joined by ", "> cannot be set when using a share group."` if
    /// any of the keys a share group does not support is set, checked before
    /// anything is parsed.
    #[doc(alias = "org.apache.kafka.clients.consumer.ShareConsumerConfig#ShareConsumerConfig")]
    pub(crate) fn new(props: &HashMap<String, String>) -> Result<Self, Error> {
        let config = ConsumerConfig::build_with_hooks(
            string_originals(props),
            None,
            "ShareConsumerConfig",
            Self::pre_process_parsed_config,
            Self::check_unsupported_configs_post_process,
        )?;
        Ok(Self { config })
    }

    /// The configuration as the `ConsumerConfig` it extends in Java.
    pub(crate) fn into_consumer_config(self) -> ConsumerConfig {
        self.config
    }

    /// Java's `preProcessParsedConfig` override.
    fn pre_process_parsed_config(parsed_values: ConfigMap) -> Result<ConfigMap, Error> {
        Self::check_unsupported_configs_pre_process(&parsed_values)?;
        Ok(parsed_values)
    }

    /// Java's `checkUnsupportedConfigsPreProcess`: a key is set when it is in
    /// the originals, whatever its value.
    fn check_unsupported_configs_pre_process(parsed_values: &ConfigMap) -> Result<(), Error> {
        let invalid_configs: Vec<&str> = SHARE_GROUP_UNSUPPORTED_CONFIGS
            .into_iter()
            .filter(|config_name| parsed_values.contains_key(*config_name))
            .collect();
        if !invalid_configs.is_empty() {
            return Err(Error::config_message(format!(
                "{} cannot be set when using a share group.",
                invalid_configs.join(", ")
            )));
        }
        Ok(())
    }

    /// Java's `checkUnsupportedConfigsPostProcess` override, which does
    /// nothing.
    fn check_unsupported_configs_post_process(_config: &AbstractConfig) -> Result<(), Error> {
        Ok(())
    }
}

impl Deref for ShareConsumerConfig {
    type Target = ConsumerConfig;

    // Java's upcast of a `ShareConsumerConfig` to its `ConsumerConfig`.
    fn deref(&self) -> &ConsumerConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Java's `verifyUnsupportedShareConsumerConfig`. Java passes the
    /// deserializer classes too; the Rust configuration has no class keys
    /// (CLAUDE.md §2), and `bootstrap.servers` is required here.
    fn verify_unsupported_share_consumer_config(extra_config: (&str, &str)) {
        let mut props = HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "localhost:9092".to_string(),
            ),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "1".to_string()),
        ]);
        props.insert(extra_config.0.to_string(), extra_config.1.to_string());
        let error = ShareConsumerConfig::new(&props).unwrap_err();
        assert!(matches!(error, Error::Config(_)), "{error:?}");
        assert_eq!(
            format!("{} cannot be set when using a share group.", extra_config.0),
            error.message()
        );
    }

    #[test]
    fn test_unsupported_share_consumer_configs() {
        verify_unsupported_share_consumer_config((ConsumerConfig::AUTO_OFFSET_RESET_CONFIG, "earliest"));
        verify_unsupported_share_consumer_config((ConsumerConfig::ENABLE_AUTO_COMMIT_CONFIG, "true"));
        verify_unsupported_share_consumer_config((ConsumerConfig::GROUP_INSTANCE_ID_CONFIG, "1"));
        verify_unsupported_share_consumer_config((ConsumerConfig::ISOLATION_LEVEL_CONFIG, "read_committed"));
        verify_unsupported_share_consumer_config((
            ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
            "org.apache.kafka.clients.consumer.StickyAssignor",
        ));
        verify_unsupported_share_consumer_config((
            INTERCEPTOR_CLASSES_CONFIG,
            "org.apache.kafka.clients.consumer.ConsumerInterceptor",
        ));
        verify_unsupported_share_consumer_config((ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG, "3000"));
        verify_unsupported_share_consumer_config((ConsumerConfig::HEARTBEAT_INTERVAL_MS_CONFIG, "3000"));
        verify_unsupported_share_consumer_config((ConsumerConfig::GROUP_PROTOCOL_CONFIG, "classic"));
        verify_unsupported_share_consumer_config((ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG, "null"));
    }

    // The tests below are not in `ShareConsumerConfigTest`.

    fn base_props() -> HashMap<String, String> {
        HashMap::from([
            (
                ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG.to_string(),
                "localhost:9092".to_string(),
            ),
            (ConsumerConfig::GROUP_ID_CONFIG.to_string(), "group".to_string()),
        ])
    }

    /// Every unsupported key set is listed, in Java's list order, and an empty
    /// value counts as set (`containsKey`, unlike `ConsumerConfig`'s
    /// post-process check, which skips a blank value).
    #[test]
    fn test_unsupported_configs_are_listed_in_order() {
        let mut props = base_props();
        props.insert(ConsumerConfig::GROUP_REMOTE_ASSIGNOR_CONFIG.to_string(), String::new());
        props.insert(ConsumerConfig::ISOLATION_LEVEL_CONFIG.to_string(), "read_committed".to_string());
        props.insert(ConsumerConfig::AUTO_OFFSET_RESET_CONFIG.to_string(), "earliest".to_string());
        assert_eq!(
            "auto.offset.reset, isolation.level, group.remote.assignor cannot be set when using a share group.",
            ShareConsumerConfig::new(&props).unwrap_err().message()
        );
    }

    /// The check runs before parsing, as Java's `preProcessParsedConfig`
    /// does: an unsupported key with an invalid value fails as unsupported.
    #[test]
    fn test_unsupported_check_runs_before_parsing() {
        let mut props = base_props();
        props.insert(
            ConsumerConfig::SESSION_TIMEOUT_MS_CONFIG.to_string(),
            "not-a-number".to_string(),
        );
        assert_eq!(
            "session.timeout.ms cannot be set when using a share group.",
            ShareConsumerConfig::new(&props).unwrap_err().message()
        );
    }

    /// The share keys, which `ConsumerConfig` rejects under either group
    /// protocol, are accepted: Java overrides `checkUnsupportedConfigsPostProcess`
    /// to do nothing. Their validators still apply.
    #[test]
    fn test_share_keys_are_accepted_and_validated() {
        let mut props = base_props();
        props.insert(
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG.to_string(),
            "explicit".to_string(),
        );
        props.insert(
            ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG.to_string(),
            "record_limit".to_string(),
        );
        let config = ShareConsumerConfig::new(&props).unwrap();
        assert_eq!(
            Some("explicit"),
            config.get_string(ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG).unwrap()
        );
        assert_eq!(
            Some("record_limit"),
            config.get_string(ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG).unwrap()
        );
        assert!(ConsumerConfig::new(&props).is_err());

        props.insert(
            ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG.to_string(),
            "IMPLICIT".to_string(),
        );
        assert_eq!(
            "Invalid value IMPLICIT for configuration share.acknowledgement.mode: Invalid value `IMPLICIT` for \
             configuration share.acknowledgement.mode. The value must either be 'implicit' or 'explicit'.",
            ShareConsumerConfig::new(&props).unwrap_err().message()
        );
    }

    /// The rest of `ConsumerConfig`'s post-processing still runs: the
    /// generated `client.id`, and the defaults of the share keys.
    #[test]
    fn test_consumer_config_post_processing_still_applies() {
        let config = ShareConsumerConfig::new(&base_props()).unwrap();
        assert!(config.client_id().starts_with("consumer-group-"), "{}", config.client_id());
        assert_eq!(
            Some("implicit"),
            config.get_string(ConsumerConfig::SHARE_ACKNOWLEDGEMENT_MODE_CONFIG).unwrap()
        );
        assert_eq!(
            Some("BATCH_OPTIMIZED"),
            config.get_string(ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG).unwrap()
        );
        assert_eq!("group", config.into_consumer_config().group_id().unwrap());
    }
}

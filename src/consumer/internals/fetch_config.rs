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

//! Immutable bundle of fetch-related consumer configuration.
//!
//! Translated from
//! `org.apache.kafka.clients.consumer.internals.FetchConfig`. Mirrors the
//! Java class: public final fields (`pub` in Rust) read directly by the
//! consumer fetch path, no getters.

#![allow(dead_code)]

use std::fmt;

use crate::common::IsolationLevel;
use crate::common::KafkaError;
use crate::consumer::consumer_config::ConsumerConfig;

/// Immutable bundle of fetch settings derived from [`ConsumerConfig`].
///
/// Corresponds to `org.apache.kafka.clients.consumer.internals.FetchConfig`.
#[derive(Clone, Debug)]
pub(crate) struct FetchConfig {
    /// `fetch.min.bytes`.
    pub min_bytes: i32,
    /// `fetch.max.bytes`.
    pub max_bytes: i32,
    /// `fetch.max.wait.ms`.
    pub max_wait_ms: i32,
    /// `max.partition.fetch.bytes`.
    pub fetch_size: i32,
    /// `max.poll.records`.
    pub max_poll_records: i32,
    /// `check.crcs`.
    pub check_crcs: bool,
    /// `client.rack`.
    pub client_rack_id: String,
    /// `isolation.level`.
    pub isolation_level: IsolationLevel,
}

impl FetchConfig {
    /// Constructs a `FetchConfig` with explicit values.
    ///
    /// Translates the 8-arg Java constructor.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        min_bytes: i32,
        max_bytes: i32,
        max_wait_ms: i32,
        fetch_size: i32,
        max_poll_records: i32,
        check_crcs: bool,
        client_rack_id: impl Into<String>,
        isolation_level: IsolationLevel,
    ) -> Self {
        Self {
            min_bytes,
            max_bytes,
            max_wait_ms,
            fetch_size,
            max_poll_records,
            check_crcs,
            client_rack_id: client_rack_id.into(),
            isolation_level,
        }
    }

    /// Constructs a `FetchConfig` by pulling values from a [`ConsumerConfig`].
    ///
    /// Translates the `FetchConfig(ConsumerConfig)` Java constructor.
    ///
    /// # Errors
    ///
    /// Returns an error if `isolation.level` is not one of `read_uncommitted`
    /// or `read_committed`.
    pub(crate) fn from_consumer_config(config: &ConsumerConfig) -> Result<Self, KafkaError> {
        let isolation_level = match config.isolation_level.as_str() {
            "read_uncommitted" => IsolationLevel::ReadUncommitted,
            "read_committed" => IsolationLevel::ReadCommitted,
            other => {
                return Err(KafkaError::illegal_argument(format!(
                    "Invalid value '{other}' for configuration isolation.level: must be one of \
                     'read_uncommitted' or 'read_committed'"
                )));
            },
        };
        Ok(Self {
            min_bytes: config.fetch_min_bytes,
            max_bytes: config.fetch_max_bytes,
            max_wait_ms: config.fetch_max_wait_ms,
            fetch_size: config.max_partition_fetch_bytes,
            max_poll_records: config.max_poll_records,
            check_crcs: config.check_crcs,
            client_rack_id: config.client_rack.clone(),
            isolation_level,
        })
    }
}

impl fmt::Display for FetchConfig {
    /// Matches Java's `toString()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FetchConfig{{minBytes={}, maxBytes={}, maxWaitMs={}, fetchSize={}, \
             maxPollRecords={}, checkCrcs={}, clientRackId='{}', isolationLevel={}}}",
            self.min_bytes,
            self.max_bytes,
            self.max_wait_ms,
            self.fetch_size,
            self.max_poll_records,
            self.check_crcs,
            self.client_rack_id,
            self.isolation_level,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from
    /// `FetchConfigTest.newFetchConfigFromValues` — explicit-argument constructor
    /// using the default constants exposed on `ConsumerConfig`.
    #[test]
    fn test_basic_from_explicit_values() {
        let cfg = FetchConfig::new(
            1,                // DEFAULT_FETCH_MIN_BYTES
            50 * 1024 * 1024, // DEFAULT_FETCH_MAX_BYTES
            500,              // DEFAULT_FETCH_MAX_WAIT_MS
            1024 * 1024,      // DEFAULT_MAX_PARTITION_FETCH_BYTES
            500,              // DEFAULT_MAX_POLL_RECORDS
            true,             // check_crcs
            "",               // DEFAULT_CLIENT_RACK
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(1, cfg.min_bytes);
        assert_eq!(50 * 1024 * 1024, cfg.max_bytes);
        assert_eq!(500, cfg.max_wait_ms);
        assert_eq!(1024 * 1024, cfg.fetch_size);
        assert_eq!(500, cfg.max_poll_records);
        assert!(cfg.check_crcs);
        assert_eq!("", cfg.client_rack_id);
        assert_eq!(IsolationLevel::ReadUncommitted, cfg.isolation_level);
    }

    /// Translated from
    /// `FetchConfigTest.newFetchConfigFromConsumerConfig`. Just exercises the
    /// `from_consumer_config` constructor with default settings.
    #[test]
    fn test_basic_from_consumer_config() {
        let consumer_config = ConsumerConfig::default();
        let fetch_config = FetchConfig::from_consumer_config(&consumer_config).unwrap();
        // Same as the explicit-values test — defaults agree.
        assert_eq!(1, fetch_config.min_bytes);
        assert_eq!(50 * 1024 * 1024, fetch_config.max_bytes);
        assert_eq!(500, fetch_config.max_wait_ms);
        assert_eq!(1024 * 1024, fetch_config.fetch_size);
        assert_eq!(500, fetch_config.max_poll_records);
        assert!(fetch_config.check_crcs);
        assert_eq!("", fetch_config.client_rack_id);
        assert_eq!(IsolationLevel::ReadUncommitted, fetch_config.isolation_level);
    }

    /// `from_consumer_config` honors a non-default isolation level.
    #[test]
    fn test_from_consumer_config_read_committed() {
        let consumer_config =
            ConsumerConfig { isolation_level: "read_committed".to_string(), ..ConsumerConfig::default() };
        let fetch_config = FetchConfig::from_consumer_config(&consumer_config).unwrap();
        assert_eq!(IsolationLevel::ReadCommitted, fetch_config.isolation_level);
    }

    /// `from_consumer_config` rejects unknown isolation levels.
    #[test]
    fn test_from_consumer_config_rejects_unknown_isolation_level() {
        let consumer_config =
            ConsumerConfig { isolation_level: "not_a_level".to_string(), ..ConsumerConfig::default() };
        let err = FetchConfig::from_consumer_config(&consumer_config).unwrap_err();
        let msg = err.message();
        assert!(msg.contains("isolation.level"), "{msg}");
        assert!(msg.contains("not_a_level"), "{msg}");
    }

    /// `Display` produces a Java-shaped `toString` representation.
    #[test]
    fn test_display_matches_java_to_string() {
        let cfg = FetchConfig::new(1, 1024, 500, 256, 500, true, "rack-a", IsolationLevel::ReadCommitted);
        let s = cfg.to_string();
        // Spot-check the expected substring shape — full equality would
        // tie the test to formatting whitespace.
        assert!(s.contains("FetchConfig{"));
        assert!(s.contains("minBytes=1"));
        assert!(s.contains("maxBytes=1024"));
        assert!(s.contains("clientRackId='rack-a'"));
        assert!(s.contains("isolationLevel=read_committed"));
    }
}

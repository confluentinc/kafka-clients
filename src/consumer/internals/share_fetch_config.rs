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

//! Static configuration for fetching records for share consumers (KIP-932).
//!
//! Corresponds to `org.apache.kafka.clients.consumer.internals.ShareFetchConfig`.

// Phase 1 (M9) translates the share wire/session layer; the share consumer that
// constructs this config lands in a later phase.
#![allow(dead_code)]

use crate::common::IsolationLevel;
use crate::common::KafkaError;
use crate::consumer::consumer_config::ConsumerConfig;

use crate::consumer::internals::consumer_utils::configured_isolation_level;
use crate::consumer::internals::share_acquire_mode::ShareAcquireMode;

/// Represents the static configuration for fetching records from Kafka for
/// share consumers. It bundles the immutable settings that were presented at
/// the time the share consumer was created for later use by share-consumer
/// related classes.
///
/// This is similar to [`super::fetch_config::FetchConfig`] but specifically
/// designed for share-consumer use cases.
///
/// Corresponds to
/// `org.apache.kafka.clients.consumer.internals.ShareFetchConfig`.
#[derive(Clone, Debug)]
pub(crate) struct ShareFetchConfig {
    /// Minimum number of bytes the broker should accumulate before responding.
    pub(crate) min_bytes: i32,
    /// Maximum number of bytes the broker should return.
    pub(crate) max_bytes: i32,
    /// Maximum time in milliseconds the broker should block waiting for data.
    pub(crate) max_wait_ms: i32,
    /// Maximum number of bytes to fetch per partition.
    pub(crate) fetch_size: i32,
    /// Maximum number of records returned in a single poll.
    pub(crate) max_poll_records: i32,
    /// Whether to check record CRCs.
    pub(crate) check_crcs: bool,
    /// The rack id of the client.
    pub(crate) client_rack_id: String,
    /// The isolation level for reads.
    pub(crate) isolation_level: IsolationLevel,
    /// The acquire mode controlling fetch behavior.
    pub(crate) share_acquire_mode: ShareAcquireMode,
}

impl ShareFetchConfig {
    /// Constructs a new [`ShareFetchConfig`] using explicitly provided values.
    /// This is provided so tests can exercise different scenarios rather than
    /// going through the hassle of constructing a [`ConsumerConfig`].
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
        share_acquire_mode: ShareAcquireMode,
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
            share_acquire_mode,
        }
    }

    /// Constructs a new [`ShareFetchConfig`] using values from the given
    /// [`ConsumerConfig`].
    ///
    /// Translates the `ShareFetchConfig(ConsumerConfig)` Java constructor.
    ///
    /// # Errors
    ///
    /// Returns an error if the configured isolation level or share acquire mode
    /// is invalid.
    pub(crate) fn from_consumer_config(config: &ConsumerConfig) -> Result<Self, KafkaError> {
        Ok(Self {
            min_bytes: config.fetch_min_bytes,
            max_bytes: config.fetch_max_bytes,
            max_wait_ms: config.fetch_max_wait_ms,
            fetch_size: config.max_partition_fetch_bytes,
            max_poll_records: config.max_poll_records,
            check_crcs: config.check_crcs,
            client_rack_id: config.client_rack.clone(),
            isolation_level: configured_isolation_level(config)?,
            share_acquire_mode: ShareAcquireMode::of(&config.share_acquire_mode)?,
        })
    }
}

impl std::fmt::Display for ShareFetchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ShareFetchConfig{{minBytes={}, maxBytes={}, maxWaitMs={}, fetchSize={}, \
             maxPollRecords={}, checkCrcs={}, clientRackId='{}', isolationLevel={}, shareAcquireMode={}}}",
            self.min_bytes,
            self.max_bytes,
            self.max_wait_ms,
            self.fetch_size,
            self.max_poll_records,
            self.check_crcs,
            self.client_rack_id,
            self.isolation_level,
            self.share_acquire_mode
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::consumer_config::ConsumerConfig;

    #[test]
    fn test_explicit_constructor() {
        let cfg = ShareFetchConfig::new(
            1,
            100,
            500,
            1024,
            500,
            true,
            "",
            IsolationLevel::ReadUncommitted,
            ShareAcquireMode::BatchOptimized,
        );
        assert_eq!(cfg.min_bytes, 1);
        assert_eq!(cfg.max_bytes, 100);
        assert_eq!(cfg.max_wait_ms, 500);
        assert_eq!(cfg.fetch_size, 1024);
        assert_eq!(cfg.max_poll_records, 500);
        assert!(cfg.check_crcs);
        assert_eq!(cfg.share_acquire_mode, ShareAcquireMode::BatchOptimized);
    }

    #[test]
    fn test_from_consumer_config_defaults() {
        let config = ConsumerConfig::default();
        let cfg = ShareFetchConfig::from_consumer_config(&config).expect("share fetch config");
        assert_eq!(cfg.min_bytes, ConsumerConfig::DEFAULT_FETCH_MIN_BYTES);
        assert_eq!(cfg.max_bytes, ConsumerConfig::DEFAULT_FETCH_MAX_BYTES);
        assert_eq!(cfg.max_wait_ms, ConsumerConfig::DEFAULT_FETCH_MAX_WAIT_MS);
        assert_eq!(cfg.fetch_size, ConsumerConfig::DEFAULT_MAX_PARTITION_FETCH_BYTES);
        assert_eq!(cfg.max_poll_records, ConsumerConfig::DEFAULT_MAX_POLL_RECORDS);
        assert_eq!(cfg.share_acquire_mode, ShareAcquireMode::BatchOptimized);
    }
}

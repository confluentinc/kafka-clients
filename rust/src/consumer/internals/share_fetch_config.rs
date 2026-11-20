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

//! Immutable bundle of share-fetch configuration.
//!
//! Translated from `org.apache.kafka.clients.consumer.internals.ShareFetchConfig`.
//! Mirrors the Java class: public final fields (`pub` in Rust, on a
//! crate-private type) read directly by the share fetch path, no getters, as
//! [`FetchConfig`](super::FetchConfig) does.

use std::fmt;

use crate::common::Error;
use crate::common::IsolationLevel;
use crate::consumer::ConsumerConfig;
use crate::consumer::internals::ConsumerUtils;
use crate::consumer::internals::ShareAcquireMode;

/// The static configuration for fetching records from Kafka for share
/// consumers. It bundles the immutable settings that were presented at the
/// time the share consumer was created for later use by share consumer
/// related types.
///
/// This is similar to [`FetchConfig`](super::FetchConfig) but specifically
/// designed for share consumer use cases.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchConfig")]
pub(crate) struct ShareFetchConfig {
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
    /// `share.acquire.mode`.
    pub share_acquire_mode: ShareAcquireMode,
}

impl ShareFetchConfig {
    /// Constructs a `ShareFetchConfig` using explicitly provided values. This
    /// is provided for tests that want to exercise different scenarios with
    /// specific configuration values rather than going through a
    /// [`ConsumerConfig`].
    ///
    /// Translates the 9-argument Java constructor.
    #[expect(clippy::too_many_arguments)]
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "in Java only the tests call this constructor")
    )]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchConfig#ShareFetchConfig")]
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

    /// Constructs a `ShareFetchConfig` using values from the given consumer
    /// configuration:
    ///
    /// - `min_bytes`: `fetch.min.bytes`
    /// - `max_bytes`: `fetch.max.bytes`
    /// - `max_wait_ms`: `fetch.max.wait.ms`
    /// - `fetch_size`: `max.partition.fetch.bytes`
    /// - `max_poll_records`: `max.poll.records`
    /// - `check_crcs`: `check.crcs`
    /// - `client_rack_id`: `client.rack`
    /// - `isolation_level`: `isolation.level`
    /// - `share_acquire_mode`: `share.acquire.mode`
    ///
    /// Translates the `ShareFetchConfig(ConsumerConfig)` Java constructor.
    ///
    /// # Errors
    ///
    /// `isolation.level` or `share.acquire.mode` does not name a known value
    /// (Java's `IllegalArgumentException` from `ConsumerUtils#configuredIsolationLevel`
    /// and `ShareAcquireMode#of`). A `ConsumerConfig` built from properties has
    /// validated both already.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the RequestManagers share supplier calls it from Milestone 18 P6 on"
        )
    )]
    #[doc(alias = "org.apache.kafka.clients.consumer.internals.ShareFetchConfig#ShareFetchConfig")]
    pub(crate) fn with_consumer_config(config: &ConsumerConfig) -> Result<Self, Error> {
        Ok(Self {
            min_bytes: config.fetch_min_bytes,
            max_bytes: config.fetch_max_bytes,
            max_wait_ms: config.fetch_max_wait_ms,
            fetch_size: config.max_partition_fetch_bytes,
            max_poll_records: config.max_poll_records,
            check_crcs: config.check_crcs,
            client_rack_id: config.client_rack.clone(),
            isolation_level: ConsumerUtils::configured_isolation_level(config)?,
            share_acquire_mode: ShareAcquireMode::of(config.get_string(ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG)?)?,
        })
    }
}

impl fmt::Display for ShareFetchConfig {
    /// Matches Java's `toString()`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ShareFetchConfig{{minBytes={}, maxBytes={}, maxWaitMs={}, fetchSize={}, maxPollRecords={}, \
             checkCrcs={}, clientRackId='{}', isolationLevel={}, shareAcquireMode={}}}",
            self.min_bytes,
            self.max_bytes,
            self.max_wait_ms,
            self.fetch_size,
            self.max_poll_records,
            self.check_crcs,
            self.client_rack_id,
            self.isolation_level,
            self.share_acquire_mode,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::consumer::ShareConsumerConfig;

    /// The explicit-values constructor keeps every value as given.
    #[test]
    fn test_new_from_explicit_values() {
        let config = ShareFetchConfig::new(
            2,
            3,
            4,
            5,
            6,
            false,
            "rack-a",
            IsolationLevel::ReadCommitted,
            ShareAcquireMode::RecordLimit,
        );
        assert_eq!(2, config.min_bytes);
        assert_eq!(3, config.max_bytes);
        assert_eq!(4, config.max_wait_ms);
        assert_eq!(5, config.fetch_size);
        assert_eq!(6, config.max_poll_records);
        assert!(!config.check_crcs);
        assert_eq!("rack-a", config.client_rack_id);
        assert_eq!(IsolationLevel::ReadCommitted, config.isolation_level);
        assert_eq!(ShareAcquireMode::RecordLimit, config.share_acquire_mode);
    }

    fn props(entries: &[(&str, &str)]) -> HashMap<String, String> {
        [(ConsumerConfig::BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")]
            .iter()
            .chain(entries)
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    /// Translation-checklist R3: every key the Java `ShareFetchConfig(ConsumerConfig)`
    /// constructor reads is set to a non-default value, in the configuration
    /// the share consumer builds (`ShareConsumerConfig`), and each lands in
    /// its field. `isolation.level` cannot be set on a share consumer
    /// (`ShareConsumerConfig#checkUnsupportedConfigsPreProcess`), so it is the
    /// default here and is covered by the next test.
    #[test]
    fn test_with_consumer_config_reads_every_key() {
        let share_config = ShareConsumerConfig::new(&props(&[
            (ConsumerConfig::FETCH_MIN_BYTES_CONFIG, "11"),
            (ConsumerConfig::FETCH_MAX_BYTES_CONFIG, "12000"),
            (ConsumerConfig::FETCH_MAX_WAIT_MS_CONFIG, "13"),
            (ConsumerConfig::MAX_PARTITION_FETCH_BYTES_CONFIG, "14000"),
            (ConsumerConfig::MAX_POLL_RECORDS_CONFIG, "15"),
            (ConsumerConfig::CHECK_CRCS_CONFIG, "false"),
            (ConsumerConfig::CLIENT_RACK_CONFIG, "rack-b"),
            (ConsumerConfig::SHARE_ACQUIRE_MODE_CONFIG, "RECORD_LIMIT"),
        ]))
        .unwrap();
        let config = ShareFetchConfig::with_consumer_config(&share_config).unwrap();
        assert_eq!(11, config.min_bytes);
        assert_eq!(12000, config.max_bytes);
        assert_eq!(13, config.max_wait_ms);
        assert_eq!(14000, config.fetch_size);
        assert_eq!(15, config.max_poll_records);
        assert!(!config.check_crcs);
        assert_eq!("rack-b", config.client_rack_id);
        // `ShareAcquireMode#of` is case-insensitive.
        assert_eq!(ShareAcquireMode::RecordLimit, config.share_acquire_mode);
    }

    /// `isolation.level` is read through `ConsumerUtils#configuredIsolationLevel`,
    /// and `share.acquire.mode` defaults to `batch_optimized`.
    #[test]
    fn test_with_consumer_config_isolation_level_and_default_acquire_mode() {
        let consumer_config =
            ConsumerConfig::new(&props(&[(ConsumerConfig::ISOLATION_LEVEL_CONFIG, "read_uncommitted")])).unwrap();
        let config = ShareFetchConfig::with_consumer_config(&consumer_config).unwrap();
        assert_eq!(IsolationLevel::ReadUncommitted, config.isolation_level);
        assert_eq!(ShareAcquireMode::BatchOptimized, config.share_acquire_mode);
        assert_eq!(500, config.max_poll_records);
        assert!(config.check_crcs);
    }

    /// `Display` matches Java's `toString()` exactly.
    #[test]
    fn test_display_matches_java_to_string() {
        let config = ShareFetchConfig::new(
            1,
            1024,
            500,
            256,
            500,
            true,
            "rack-a",
            IsolationLevel::ReadCommitted,
            ShareAcquireMode::BatchOptimized,
        );
        assert_eq!(
            "ShareFetchConfig{minBytes=1, maxBytes=1024, maxWaitMs=500, fetchSize=256, maxPollRecords=500, \
             checkCrcs=true, clientRackId='rack-a', isolationLevel=read_committed, shareAcquireMode=batch_optimized}",
            config.to_string()
        );
    }
}

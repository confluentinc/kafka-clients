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

//! The result of `Admin::create_topics`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.CreateTopicsResult`.

use std::collections::HashMap;

use crate::admin::Config;
use crate::common::{Error, KafkaFuture, Uuid};

/// Sentinel used when the broker did not return partition/replication metadata.
pub(crate) const UNKNOWN: i32 = -1;

/// Topic metadata and configuration returned per created topic.
///
/// Corresponds to `CreateTopicsResult.TopicMetadataAndConfig`. Carries either
/// the metadata or a stored error; the accessors surface the error (mirroring
/// Java's `ensureSuccess`, which rethrows the stored `ApiException`).
#[derive(Clone, Debug)]
pub struct TopicMetadataAndConfig {
    error: Option<Error>,
    topic_id: Uuid,
    num_partitions: i32,
    replication_factor: i32,
    config: Option<Config>,
}

impl TopicMetadataAndConfig {
    /// Creates a successful metadata-and-config holder.
    pub fn new(topic_id: Uuid, num_partitions: i32, replication_factor: i32, config: Config) -> Self {
        Self { error: None, topic_id, num_partitions, replication_factor, config: Some(config) }
    }

    /// Creates a holder representing a failure; every accessor returns the
    /// error.
    pub fn with_error(error: Error) -> Self {
        Self {
            error: Some(error),
            topic_id: Uuid::zero(),
            num_partitions: UNKNOWN,
            replication_factor: UNKNOWN,
            config: None,
        }
    }

    fn ensure_success(&self) -> Result<(), Error> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// The topic id, or the stored error.
    pub fn topic_id(&self) -> Result<Uuid, Error> {
        self.ensure_success()?;
        Ok(self.topic_id)
    }

    /// The number of partitions, or the stored error.
    pub fn num_partitions(&self) -> Result<i32, Error> {
        self.ensure_success()?;
        Ok(self.num_partitions)
    }

    /// The replication factor, or the stored error.
    pub fn replication_factor(&self) -> Result<i32, Error> {
        self.ensure_success()?;
        Ok(self.replication_factor)
    }

    /// The topic config, or the stored error.
    pub fn config(&self) -> Result<Config, Error> {
        self.ensure_success()?;
        // Only `None` when an exception is present, already handled above.
        Ok(self.config.clone().expect("config present on success"))
    }
}

/// The result of `Admin::create_topics`.
///
/// Corresponds to `org.apache.kafka.clients.admin.CreateTopicsResult`.
#[derive(Clone, Debug)]
pub struct CreateTopicsResult {
    futures: HashMap<String, KafkaFuture<TopicMetadataAndConfig>>,
}

impl CreateTopicsResult {
    /// Creates a result from a map of topic name to per-topic future.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<TopicMetadataAndConfig>>) -> Self {
        Self { futures }
    }

    /// Return a map from topic names to futures, which can be used to check the
    /// status of individual topic creations.
    pub fn values(&self) -> HashMap<String, KafkaFuture<()>> {
        self.futures
            .iter()
            .map(|(name, future)| (name.clone(), future.then_apply(|_| ())))
            .collect()
    }

    /// Return a future which succeeds if all the topic creations succeed.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }

    /// Returns a future that provides topic configs for the topic when the
    /// request completes.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    pub fn config(&self, topic: &str) -> KafkaFuture<Config> {
        self.future_for(topic).then_apply_try(|tmac| tmac.config())
    }

    /// Returns a future that provides the topic id for the topic.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    pub fn topic_id(&self, topic: &str) -> KafkaFuture<Uuid> {
        self.future_for(topic).then_apply_try(|tmac| tmac.topic_id())
    }

    /// Returns a future that provides the number of partitions for the topic.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    pub fn num_partitions(&self, topic: &str) -> KafkaFuture<i32> {
        self.future_for(topic).then_apply_try(|tmac| tmac.num_partitions())
    }

    /// Returns a future that provides the replication factor for the topic.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    pub fn replication_factor(&self, topic: &str) -> KafkaFuture<i32> {
        self.future_for(topic).then_apply_try(|tmac| tmac.replication_factor())
    }

    /// The raw per-topic futures, keyed by topic name.
    ///
    /// Java has no equivalent accessor because Java callers hold the per-key
    /// `KafkaFuture`s and inspect each one through `values()` / `topicId(topic)`
    /// / `config(topic)` etc. The C FFI cannot: it has to flatten the whole
    /// batch into one handle carrying a value *and* an error per key
    /// (`PLAN-bindings.md` D2), which needs the `TopicMetadataAndConfig` future
    /// itself — not the `KafkaFuture<Void>` that `values()` maps it to, nor four
    /// separate `then_apply_try` views of the same source. Crate-internal, so
    /// the public surface still matches Java exactly.
    // Only the `ffi` feature consumes this outside tests; without it the method
    // is dead code and `#![deny(warnings)]` would fail the build.
    #[cfg_attr(not(feature = "ffi"), allow(dead_code))]
    pub(crate) fn futures(&self) -> &HashMap<String, KafkaFuture<TopicMetadataAndConfig>> {
        &self.futures
    }

    fn future_for(&self, topic: &str) -> &KafkaFuture<TopicMetadataAndConfig> {
        self.futures
            .get(topic)
            .unwrap_or_else(|| panic!("Topic {topic} was not part of the createTopics request"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::ConfigEntry;
    use crate::common::kafka_future::KafkaFutureImpl;

    fn config() -> Config {
        Config::new([ConfigEntry::new("k".to_string(), Some("v".to_string()))])
    }

    #[tokio::test]
    async fn values_and_all_succeed() {
        let handle: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
        let mut futures = HashMap::new();
        futures.insert("t".to_string(), handle.future());
        let result = CreateTopicsResult::new(futures);

        handle.complete(TopicMetadataAndConfig::new(Uuid::new(1, 2), 3, 2, config()));

        assert_eq!(result.values()["t"].get().await.unwrap(), ());
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.topic_id("t").get().await.unwrap(), Uuid::new(1, 2));
        assert_eq!(result.num_partitions("t").get().await.unwrap(), 3);
        assert_eq!(result.replication_factor("t").get().await.unwrap(), 2);
        assert_eq!(result.config("t").get().await.unwrap().get("k").unwrap().value(), Some("v"));
    }

    /// `futures()` exposes the raw per-key `TopicMetadataAndConfig` futures the
    /// FFI needs to flatten a batch, keyed exactly like `values()`.
    #[tokio::test]
    async fn futures_exposes_raw_per_key_futures() {
        let ok: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
        let bad: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
        let mut futures = HashMap::new();
        futures.insert("ok".to_string(), ok.future());
        futures.insert("bad".to_string(), bad.future());
        let result = CreateTopicsResult::new(futures);

        ok.complete(TopicMetadataAndConfig::new(Uuid::new(1, 2), 3, 2, config()));
        bad.complete_with_error(Error::local_illegal_argument("nope"));

        let raw = result.futures();
        assert_eq!(raw.len(), 2);
        assert_eq!(
            raw.keys().cloned().collect::<std::collections::BTreeSet<_>>(),
            result.values().keys().cloned().collect::<std::collections::BTreeSet<_>>()
        );
        // Unlike `values()` (mapped to `KafkaFuture<()>`), the raw future carries
        // the metadata itself.
        let metadata = raw["ok"].get().await.unwrap();
        assert_eq!(metadata.num_partitions().unwrap(), 3);
        // Per-key errors stay per-key.
        assert!(matches!(raw["bad"].get().await, Err(Error::LocalIllegalArgument(_))));
    }

    #[tokio::test]
    async fn accessors_surface_stored_error() {
        let handle: KafkaFutureImpl<TopicMetadataAndConfig> = KafkaFutureImpl::new();
        let mut futures = HashMap::new();
        futures.insert("t".to_string(), handle.future());
        let result = CreateTopicsResult::new(futures);

        handle.complete(TopicMetadataAndConfig::with_error(Error::local_illegal_state("unsupported")));

        assert!(matches!(result.config("t").get().await, Err(Error::LocalIllegalState(_))));
        assert!(matches!(result.topic_id("t").get().await, Err(Error::LocalIllegalState(_))));
        // values()/all() still succeed (they only observe completion, not the
        // metadata accessors' stored error).
        assert_eq!(result.all().get().await.unwrap(), ());
    }
}

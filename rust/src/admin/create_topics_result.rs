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

use std::collections::HashMap;

use crate::admin::Config;
use crate::common::{Error, KafkaFuture, Uuid};

/// Topic metadata and configuration returned per created topic.
///
/// Carries either the metadata or a stored error; the accessors return the
/// stored error.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig")]
pub struct TopicMetadataAndConfig {
    error: Option<Error>,
    topic_id: Uuid,
    num_partitions: i32,
    replication_factor: i32,
    config: Option<Config>,
}

impl TopicMetadataAndConfig {
    /// Creates a successful metadata-and-config holder.
    ///
    /// # Arguments
    ///
    /// - `topic_id` - the topic id.
    /// - `num_partitions` - the number of partitions of the topic.
    /// - `replication_factor` - the replication factor of the topic.
    /// - `config` - the topic config.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#TopicMetadataAndConfig")]
    pub fn new(topic_id: Uuid, num_partitions: i32, replication_factor: i32, config: Config) -> Self {
        Self { error: None, topic_id, num_partitions, replication_factor, config: Some(config) }
    }

    /// Creates a holder representing a failure; every accessor returns the
    /// error.
    ///
    /// # Arguments
    ///
    /// - `error` - the error every accessor returns.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#TopicMetadataAndConfig")]
    pub fn with_error(error: Error) -> Self {
        Self {
            error: Some(error),
            topic_id: Uuid::zero(),
            num_partitions: CreateTopicsResult::UNKNOWN,
            replication_factor: CreateTopicsResult::UNKNOWN,
            config: None,
        }
    }

    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#ensureSuccess")]
    fn ensure_success(&self) -> Result<(), Error> {
        match &self.error {
            Some(e) => Err(e.clone()),
            None => Ok(()),
        }
    }

    /// Returns the topic id, or the stored error.
    ///
    /// # Errors
    ///
    /// The error the topic creation failed with, if any.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#topicId")]
    pub fn topic_id(&self) -> Result<Uuid, Error> {
        self.ensure_success()?;
        Ok(self.topic_id)
    }

    /// Returns the number of partitions, or the stored error.
    ///
    /// # Errors
    ///
    /// The error the topic creation failed with, if any.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#numPartitions")]
    pub fn num_partitions(&self) -> Result<i32, Error> {
        self.ensure_success()?;
        Ok(self.num_partitions)
    }

    /// Returns the replication factor, or the stored error.
    ///
    /// # Errors
    ///
    /// The error the topic creation failed with, if any.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#replicationFactor")]
    pub fn replication_factor(&self) -> Result<i32, Error> {
        self.ensure_success()?;
        Ok(self.replication_factor)
    }

    /// Returns the topic config, or the stored error.
    ///
    /// # Errors
    ///
    /// The error the topic creation failed with, if any.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult$TopicMetadataAndConfig#config")]
    pub fn config(&self) -> Result<Config, Error> {
        self.ensure_success()?;
        // Only `None` when an exception is present, already handled above.
        Ok(self.config.clone().expect("config present on success"))
    }
}

/// The result of `Admin::create_topics`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult")]
pub struct CreateTopicsResult {
    futures: HashMap<String, KafkaFuture<TopicMetadataAndConfig>>,
}

impl CreateTopicsResult {
    /// Sentinel used when the broker did not return partition/replication metadata.
    pub(crate) const UNKNOWN: i32 = -1;

    /// Creates a result from a map of topic name to per-topic future.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#CreateTopicsResult")]
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<TopicMetadataAndConfig>>) -> Self {
        Self { futures }
    }

    /// Returns a map from topic names to futures, which can be used to check the
    /// status of individual topic creations.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#values")]
    pub fn values(&self) -> HashMap<String, KafkaFuture<()>> {
        self.futures
            .iter()
            .map(|(name, future)| (name.clone(), future.then_apply(|_| Ok(()))))
            .collect()
    }

    /// Returns a future which succeeds if all the topic creations succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }

    /// Returns a future that provides topic configs for the topic when the
    /// request completes.
    ///
    /// If broker version doesn't support replication factor in the response, the future
    /// completes with an error answering `Error::is_unsupported_version_error`. If broker
    /// returned an error for topic configs, it completes with the appropriate error, for
    /// example one answering `Error::is_topic_authorization_error` if user does not have
    /// permission to describe topic configs. Note that the values for the type and documentation
    /// fields will be `None`.
    ///
    /// # Arguments
    ///
    /// - `topic` - the topic name.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#config")]
    pub fn config(&self, topic: &str) -> KafkaFuture<Config> {
        self.future_for(topic).then_apply(|tmac: TopicMetadataAndConfig| tmac.config())
    }

    /// Returns a future that provides the topic id for the topic when the request completes.
    ///
    /// If broker version doesn't support replication factor in the response, the future
    /// completes with an error answering `Error::is_unsupported_version_error`. If broker
    /// returned an error for topic configs, it completes with the appropriate error, for
    /// example one answering `Error::is_topic_authorization_error` if user does not have
    /// permission to describe topic configs.
    ///
    /// # Arguments
    ///
    /// - `topic` - the topic name.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#topicId")]
    pub fn topic_id(&self, topic: &str) -> KafkaFuture<Uuid> {
        self.future_for(topic)
            .then_apply(|tmac: TopicMetadataAndConfig| tmac.topic_id())
    }

    /// Returns a future that provides the number of partitions for the topic when the request
    /// completes.
    ///
    /// If broker version doesn't support replication factor in the response, the future
    /// completes with an error answering `Error::is_unsupported_version_error`. If broker
    /// returned an error for topic configs, it completes with the appropriate error, for
    /// example one answering `Error::is_topic_authorization_error` if user does not have
    /// permission to describe topic configs.
    ///
    /// # Arguments
    ///
    /// - `topic` - the topic name.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#numPartitions")]
    pub fn num_partitions(&self, topic: &str) -> KafkaFuture<i32> {
        self.future_for(topic)
            .then_apply(|tmac: TopicMetadataAndConfig| tmac.num_partitions())
    }

    /// Returns a future that provides the replication factor for the topic when the request
    /// completes.
    ///
    /// If broker version doesn't support replication factor in the response, the future
    /// completes with an error answering `Error::is_unsupported_version_error`. If broker
    /// returned an error for topic configs, it completes with the appropriate error, for
    /// example one answering `Error::is_topic_authorization_error` if user does not have
    /// permission to describe topic configs.
    ///
    /// # Arguments
    ///
    /// - `topic` - the topic name.
    ///
    /// # Panics
    ///
    /// Panics if `topic` was not part of the original request.
    #[doc(alias = "org.apache.kafka.clients.admin.CreateTopicsResult#replicationFactor")]
    pub fn replication_factor(&self, topic: &str) -> KafkaFuture<i32> {
        self.future_for(topic)
            .then_apply(|tmac: TopicMetadataAndConfig| tmac.replication_factor())
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
    use crate::common::internals::KafkaFutureImpl;

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

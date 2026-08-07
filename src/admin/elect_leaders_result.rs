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

//! The result of `Admin::elect_leaders`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ElectLeadersResult`.

use std::collections::HashMap;

use crate::common::{KafkaError, KafkaFuture, TopicPartition};

/// The result of `Admin::elect_leaders`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ElectLeadersResult`.
#[derive(Clone, Debug)]
pub struct ElectLeadersResult {
    election_future: KafkaFuture<HashMap<TopicPartition, Option<KafkaError>>>,
}

impl ElectLeadersResult {
    /// Creates a result wrapping the election future.
    pub(crate) fn new(election_future: KafkaFuture<HashMap<TopicPartition, Option<KafkaError>>>) -> Self {
        Self { election_future }
    }

    /// Get a future for the topic partitions for which a leader election was
    /// attempted. If the election succeeded then the value for a topic
    /// partition will be `None`; otherwise the election failed and the value
    /// will be `Some(error)`.
    ///
    /// Mirrors `partitions()`.
    pub fn partitions(&self) -> KafkaFuture<HashMap<TopicPartition, Option<KafkaError>>> {
        self.election_future.clone()
    }

    /// Return a future which succeeds if all the topic elections succeed.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        self.election_future
            .then_apply_try(|topic_partitions| match topic_partitions.into_values().flatten().next() {
                Some(error) => Err(error),
                None => Ok(()),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    #[tokio::test]
    async fn all_succeeds_when_no_partition_has_error() {
        let h: KafkaFutureImpl<HashMap<TopicPartition, Option<KafkaError>>> = KafkaFutureImpl::new();
        let result = ElectLeadersResult::new(h.future());
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), None);
        map.insert(TopicPartition::new("t", 1), None);
        h.complete(map);
        assert_eq!(result.all().get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn all_fails_when_any_partition_has_error() {
        let h: KafkaFutureImpl<HashMap<TopicPartition, Option<KafkaError>>> = KafkaFutureImpl::new();
        let result = ElectLeadersResult::new(h.future());
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), None);
        map.insert(
            TopicPartition::new("t", 1),
            Some(KafkaError::new(Errors::ClusterAuthorizationFailed)),
        );
        h.complete(map);
        let err = result.all().get().await.unwrap_err();
        assert_eq!(err.error(), Errors::ClusterAuthorizationFailed);
    }

    #[tokio::test]
    async fn partitions_exposes_per_partition_result() {
        let h: KafkaFutureImpl<HashMap<TopicPartition, Option<KafkaError>>> = KafkaFutureImpl::new();
        let result = ElectLeadersResult::new(h.future());
        let mut map = HashMap::new();
        map.insert(
            TopicPartition::new("t", 0),
            Some(KafkaError::new(Errors::ClusterAuthorizationFailed)),
        );
        h.complete(map);
        let partitions = result.partitions().get().await.unwrap();
        assert!(partitions.get(&TopicPartition::new("t", 0)).unwrap().is_some());
    }
}

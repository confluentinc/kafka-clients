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

//! The result of `Admin::alter_partition_reassignments`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult`.

use std::collections::HashMap;

use crate::common::{KafkaFuture, TopicPartition};

/// The result of `Admin::alter_partition_reassignments`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.AlterPartitionReassignmentsResult`.
#[derive(Clone, Debug)]
pub struct AlterPartitionReassignmentsResult {
    futures: HashMap<TopicPartition, KafkaFuture<()>>,
}

impl AlterPartitionReassignmentsResult {
    /// Creates a result from the per-partition futures.
    pub(crate) fn new(futures: HashMap<TopicPartition, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Return a map from partitions to futures which can be used to check the
    /// status of the reassignment.
    ///
    /// Mirrors `values()`.
    pub fn values(&self) -> &HashMap<TopicPartition, KafkaFuture<()>> {
        &self.futures
    }

    /// Return a future which succeeds only if all the reassignments were
    /// successfully initiated.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::kafka_future::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    #[tokio::test]
    async fn all_succeeds_when_each_completes() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h.future());
        let result = AlterPartitionReassignmentsResult::new(map);
        h.complete(());
        assert_eq!(result.all().get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn all_fails_if_one_fails() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h.future());
        let result = AlterPartitionReassignmentsResult::new(map);
        h.complete_exceptionally(Error::new(Errors::InvalidReplicaAssignment));
        assert!(result.all().get().await.is_err());
    }
}

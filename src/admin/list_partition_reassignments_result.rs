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

//! The result of `Admin::list_partition_reassignments`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.ListPartitionReassignmentsResult`.

use std::collections::HashMap;

use crate::admin::PartitionReassignment;
use crate::common::{KafkaFuture, TopicPartition};

/// The result of `Admin::list_partition_reassignments`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.ListPartitionReassignmentsResult`.
#[derive(Clone, Debug)]
pub struct ListPartitionReassignmentsResult {
    future: KafkaFuture<HashMap<TopicPartition, PartitionReassignment>>,
}

impl ListPartitionReassignmentsResult {
    /// Creates a result wrapping the reassignments future.
    pub(crate) fn new(future: KafkaFuture<HashMap<TopicPartition, PartitionReassignment>>) -> Self {
        Self { future }
    }

    /// Return a future which yields a map containing each partition's
    /// reassignments.
    ///
    /// Mirrors `reassignments()`.
    pub fn reassignments(&self) -> KafkaFuture<HashMap<TopicPartition, PartitionReassignment>> {
        self.future.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;

    #[tokio::test]
    async fn reassignments_exposes_the_map() {
        let h: KafkaFutureImpl<HashMap<TopicPartition, PartitionReassignment>> = KafkaFutureImpl::new();
        let result = ListPartitionReassignmentsResult::new(h.future());
        let mut map = HashMap::new();
        map.insert(
            TopicPartition::new("t", 0),
            PartitionReassignment::new(vec![1, 2], vec![2], vec![1]),
        );
        h.complete(map);
        let reassignments = result.reassignments().get().await.unwrap();
        assert_eq!(reassignments.get(&TopicPartition::new("t", 0)).unwrap().replicas(), &[1, 2]);
    }
}

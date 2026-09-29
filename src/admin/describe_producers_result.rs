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

//! The result of `Admin::describe_producers`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeProducersResult`.

use std::collections::HashMap;

use crate::admin::ProducerState;
use crate::common::{Error, KafkaFuture, TopicPartition};

/// The producer state of a single partition.
///
/// Corresponds to `DescribeProducersResult.PartitionProducerState`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartitionProducerState {
    active_producers: Vec<ProducerState>,
}

impl PartitionProducerState {
    /// Creates a new `PartitionProducerState` from the active producers.
    pub fn new(active_producers: Vec<ProducerState>) -> Self {
        Self { active_producers }
    }

    /// The active producers for this partition.
    pub fn active_producers(&self) -> &[ProducerState] {
        &self.active_producers
    }
}

impl std::fmt::Display for PartitionProducerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PartitionProducerState(activeProducers={:?})", self.active_producers)
    }
}

/// The result of `Admin::describe_producers`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeProducersResult`.
#[derive(Clone, Debug)]
pub struct DescribeProducersResult {
    futures: HashMap<TopicPartition, KafkaFuture<PartitionProducerState>>,
}

impl DescribeProducersResult {
    /// Creates a result from the per-partition futures.
    pub(crate) fn new(futures: HashMap<TopicPartition, KafkaFuture<PartitionProducerState>>) -> Self {
        Self { futures }
    }

    /// Returns the future for a specific partition.
    ///
    /// Mirrors `DescribeProducersResult.partitionResult`. Returns an error if
    /// the partition was not included in the request, mirroring Java's
    /// `IllegalArgumentException`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::local_illegal_argument`] if `partition` was not requested.
    pub fn partition_result(&self, partition: &TopicPartition) -> Result<KafkaFuture<PartitionProducerState>, Error> {
        self.futures.get(partition).cloned().ok_or_else(|| {
            Error::local_illegal_argument(format!("Topic partition {partition} was not included in the request"))
        })
    }

    /// Returns a future yielding a map of every requested partition's producer
    /// state. Fails if any partition's request fails.
    ///
    /// Mirrors `DescribeProducersResult.all`.
    pub fn all(&self) -> KafkaFuture<HashMap<TopicPartition, PartitionProducerState>> {
        KafkaFuture::join_map(self.futures.iter().map(|(tp, f)| (tp.clone(), f.clone())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;

    fn state(producer_id: i64) -> PartitionProducerState {
        PartitionProducerState::new(vec![ProducerState::new(producer_id, 1, 0, 0, None, None)])
    }

    #[tokio::test]
    async fn all_collects_every_partition() {
        let h0: KafkaFutureImpl<PartitionProducerState> = KafkaFutureImpl::new();
        let h1: KafkaFutureImpl<PartitionProducerState> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h0.future());
        map.insert(TopicPartition::new("t", 1), h1.future());
        let result = DescribeProducersResult::new(map);
        h0.complete(state(10));
        h1.complete(state(20));
        let all = result.all().get().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[&TopicPartition::new("t", 0)].active_producers()[0].producer_id(), 10);
    }

    #[test]
    fn partition_result_errors_for_unknown_partition() {
        let result = DescribeProducersResult::new(HashMap::new());
        let err = result.partition_result(&TopicPartition::new("t", 0)).unwrap_err();
        assert!(err.message().contains("was not included in the request"));
    }
}

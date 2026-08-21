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

//! The result of `Admin::list_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListOffsetsResult`.

use std::collections::HashMap;

use crate::common::{Error, KafkaFuture, TopicPartition};

/// The result of `Admin::list_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListOffsetsResult`.
#[derive(Clone, Debug)]
pub struct ListOffsetsResult {
    futures: HashMap<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>,
}

impl ListOffsetsResult {
    /// Creates a result from the per-partition futures.
    pub(crate) fn new(futures: HashMap<TopicPartition, KafkaFuture<ListOffsetsResultInfo>>) -> Self {
        Self { futures }
    }

    /// Return a future which can be used to check the result for a given
    /// partition.
    ///
    /// Mirrors `partitionResult(TopicPartition)`.
    ///
    /// # Errors
    ///
    /// Returns an error (invalid argument) if the offset for `partition` was not
    /// attempted, mirroring Java's `IllegalArgumentException`.
    pub fn partition_result(&self, partition: &TopicPartition) -> Result<KafkaFuture<ListOffsetsResultInfo>, Error> {
        self.futures.get(partition).cloned().ok_or_else(|| {
            Error::local_illegal_argument(format!("List Offsets for partition \"{partition}\" was not attempted"))
        })
    }

    /// Return a future which succeeds only if offsets for all specified
    /// partitions have been successfully retrieved.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<HashMap<TopicPartition, ListOffsetsResultInfo>> {
        KafkaFuture::join_map(self.futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

/// Per-partition offset information returned by `Admin::list_offsets`.
///
/// Corresponds to `ListOffsetsResult.ListOffsetsResultInfo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListOffsetsResultInfo {
    offset: i64,
    timestamp: i64,
    leader_epoch: Option<i32>,
}

impl ListOffsetsResultInfo {
    /// Creates a new result info.
    pub fn new(offset: i64, timestamp: i64, leader_epoch: Option<i32>) -> Self {
        Self { offset, timestamp, leader_epoch }
    }

    /// The offset.
    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// The timestamp associated with the offset.
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// The leader epoch associated with the offset, if known.
    pub fn leader_epoch(&self) -> Option<i32> {
        self.leader_epoch
    }
}

impl std::fmt::Display for ListOffsetsResultInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListOffsetsResultInfo(offset={}, timestamp={}, leaderEpoch={:?})",
            self.offset, self.timestamp, self.leader_epoch
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;

    #[tokio::test]
    async fn partition_result_returns_future_or_errors() {
        let h: KafkaFutureImpl<ListOffsetsResultInfo> = KafkaFutureImpl::new();
        let tp = TopicPartition::new("t", 0);
        let mut map = HashMap::new();
        map.insert(tp.clone(), h.future());
        let result = ListOffsetsResult::new(map);
        h.complete(ListOffsetsResultInfo::new(10, -1, Some(5)));
        assert_eq!(result.partition_result(&tp).unwrap().get().await.unwrap().offset(), 10);
        let err = result.partition_result(&TopicPartition::new("unknown", 0)).unwrap_err();
        assert!(err.message().contains("was not attempted"));
    }

    #[tokio::test]
    async fn all_collects_every_partition() {
        let h0: KafkaFutureImpl<ListOffsetsResultInfo> = KafkaFutureImpl::new();
        let h1: KafkaFutureImpl<ListOffsetsResultInfo> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h0.future());
        map.insert(TopicPartition::new("t", 1), h1.future());
        let result = ListOffsetsResult::new(map);
        h0.complete(ListOffsetsResultInfo::new(10, -1, Some(5)));
        h1.complete(ListOffsetsResultInfo::new(20, -1, None));
        let all = result.all().get().await.unwrap();
        assert_eq!(all.get(&TopicPartition::new("t", 0)).unwrap().offset(), 10);
        assert_eq!(all.get(&TopicPartition::new("t", 1)).unwrap().leader_epoch(), None);
    }
}

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

//! The result of `Admin::list_consumer_group_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult`.

use std::collections::HashMap;

use crate::common::{Error, KafkaFuture, TopicPartition};
use crate::consumer::OffsetAndMetadata;

/// A map of topic partitions to their committed offset and metadata. A `None`
/// value indicates the group has no committed offset for that partition
/// (Java's `null` value in the map).
pub type GroupOffsets = HashMap<TopicPartition, Option<OffsetAndMetadata>>;

/// The result of `Admin::list_consumer_group_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult`.
#[derive(Clone, Debug)]
pub struct ListConsumerGroupOffsetsResult {
    futures: HashMap<String, KafkaFuture<GroupOffsets>>,
}

impl ListConsumerGroupOffsetsResult {
    /// Creates a result from the per-group-id futures.
    ///
    /// Java's constructor takes a `Map<CoordinatorKey, KafkaFuture<...>>` and
    /// re-keys it by group id; the caller (the admin client) performs the
    /// `CoordinatorKey` → id conversion before calling this, so the map is
    /// already keyed by group id here.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<GroupOffsets>>) -> Self {
        Self { futures }
    }

    /// Returns a future which yields a map of topic partitions to
    /// `OffsetAndMetadata` for the single requested group. A `None` value
    /// indicates the group has no committed offset for that partition.
    ///
    /// Mirrors `partitionsToOffsetAndMetadata()`.
    ///
    /// # Errors
    ///
    /// Returns an error (Java's `IllegalStateException`) if offsets from
    /// multiple groups were requested — use
    /// [`partitions_to_offset_and_metadata_for_group`](Self::partitions_to_offset_and_metadata_for_group)
    /// instead.
    pub fn partitions_to_offset_and_metadata(&self) -> Result<KafkaFuture<GroupOffsets>, Error> {
        if self.futures.len() != 1 {
            return Err(Error::illegal_state(
                "Offsets from multiple consumer groups were requested. Use \
                 partitionsToOffsetAndMetadata(groupId) instead to get future for a specific group.",
            ));
        }
        Ok(self.futures.values().next().expect("len checked to be 1").clone())
    }

    /// Returns a future which yields a map of topic partitions to
    /// `OffsetAndMetadata` for the specified group. A `None` value indicates
    /// the group has no committed offset for that partition.
    ///
    /// Mirrors `partitionsToOffsetAndMetadata(String groupId)`.
    ///
    /// # Errors
    ///
    /// Returns an error (Java's `IllegalArgumentException`) if offsets for the
    /// given group were not requested.
    pub fn partitions_to_offset_and_metadata_for_group(
        &self,
        group_id: &str,
    ) -> Result<KafkaFuture<GroupOffsets>, Error> {
        self.futures.get(group_id).cloned().ok_or_else(|| {
            Error::illegal_argument(format!("Offsets for consumer group '{group_id}' were not requested."))
        })
    }

    /// Returns a future which yields all group offsets, if requests for all the
    /// groups succeed.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<HashMap<String, GroupOffsets>> {
        KafkaFuture::join_map(self.futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;

    fn offsets(offset: i64) -> GroupOffsets {
        HashMap::from([(TopicPartition::new("t", 0), Some(OffsetAndMetadata::new(offset).unwrap()))])
    }

    #[tokio::test]
    async fn single_group_partitions_to_offset_and_metadata() {
        let handle: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        handle.complete(offsets(10));
        let result = ListConsumerGroupOffsetsResult::new(HashMap::from([("g".to_string(), handle.future())]));
        let future = result.partitions_to_offset_and_metadata().unwrap();
        assert_eq!(future.get().await.unwrap(), offsets(10));
    }

    #[test]
    fn multiple_groups_no_arg_is_illegal_state() {
        let h1: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        let result = ListConsumerGroupOffsetsResult::new(HashMap::from([
            ("g1".to_string(), h1.future()),
            ("g2".to_string(), h2.future()),
        ]));
        assert!(matches!(
            result.partitions_to_offset_and_metadata(),
            Err(Error::IllegalState(_))
        ));
    }

    #[tokio::test]
    async fn for_group_returns_requested_group() {
        let h1: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        h1.complete(offsets(5));
        let result = ListConsumerGroupOffsetsResult::new(HashMap::from([("g1".to_string(), h1.future())]));
        let future = result.partitions_to_offset_and_metadata_for_group("g1").unwrap();
        assert_eq!(future.get().await.unwrap(), offsets(5));
        assert!(matches!(
            result.partitions_to_offset_and_metadata_for_group("absent"),
            Err(Error::IllegalArgument(_))
        ));
    }

    #[tokio::test]
    async fn all_collects_every_group() {
        let h1: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<GroupOffsets> = KafkaFutureImpl::new();
        h1.complete(offsets(1));
        h2.complete(offsets(2));
        let result = ListConsumerGroupOffsetsResult::new(HashMap::from([
            ("g1".to_string(), h1.future()),
            ("g2".to_string(), h2.future()),
        ]));
        let all = result.all().get().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all.get("g1"), Some(&offsets(1)));
    }
}

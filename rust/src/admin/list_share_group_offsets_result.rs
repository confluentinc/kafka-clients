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

//! The result of `Admin::list_share_group_offsets_with_options`.

use std::collections::HashMap;

use crate::admin::SharePartitionOffsetInfo;
use crate::common::{Error, KafkaFuture, TopicPartition};

/// A map of topic partitions to their share-partition offset information. A
/// `None` value indicates the group has no offset information for that
/// partition (Java's `null` value in the map).
///
/// Crate-private shorthand: Java names no such type and writes
/// `Map<TopicPartition, SharePartitionOffsetInfo>` out in each signature.
pub(crate) type ShareGroupOffsets = HashMap<TopicPartition, Option<SharePartitionOffsetInfo>>;

/// The result of the `Admin::list_share_group_offsets_with_options` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsResult")]
pub struct ListShareGroupOffsetsResult {
    futures: HashMap<String, KafkaFuture<ShareGroupOffsets>>,
}

impl ListShareGroupOffsetsResult {
    /// Creates a result from the per-group-id futures.
    ///
    /// Java's constructor takes a `Map<CoordinatorKey, KafkaFuture<...>>` and
    /// re-keys it by group id; the admin client performs that conversion before
    /// calling this, so the map is already keyed by group id here.
    #[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsResult#ListShareGroupOffsetsResult")]
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<ShareGroupOffsets>>) -> Self {
        Self { futures }
    }

    /// Return the future when the requests for all groups succeed.
    ///
    /// # Returns
    ///
    /// Future which yields all `Map<String, Map<TopicPartition, SharePartitionOffsetInfo>>`
    /// objects, if requests for all the groups succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsResult#all")]
    pub fn all(&self) -> KafkaFuture<HashMap<String, HashMap<TopicPartition, Option<SharePartitionOffsetInfo>>>> {
        KafkaFuture::join_map(self.futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    /// Returns a future which yields a map of topic partitions to offsets for the specified
    /// group. If the group doesn't have offset information for a specific partition, the
    /// corresponding value in the returned map will be `None`.
    ///
    /// # Arguments
    ///
    /// - `group_id` - the group id.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if offsets for the group were not requested.
    #[doc(alias = "org.apache.kafka.clients.admin.ListShareGroupOffsetsResult#partitionsToOffsetInfo")]
    pub fn partitions_to_offset_info(
        &self,
        group_id: &str,
    ) -> Result<KafkaFuture<HashMap<TopicPartition, Option<SharePartitionOffsetInfo>>>, Error> {
        self.futures
            .get(group_id)
            .cloned()
            .ok_or_else(|| Error::local_illegal_argument(format!("Group ID not found: {group_id}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    fn offsets(start_offset: i64) -> ShareGroupOffsets {
        HashMap::from([
            (
                TopicPartition::new("t", 0),
                Some(SharePartitionOffsetInfo::new(start_offset, Some(0), None)),
            ),
            (TopicPartition::new("t", 1), None),
        ])
    }

    /// New test, no Java original (Java has no `ListShareGroupOffsetsResultTest`):
    /// each group's future is reachable by its id, and `all()` collects them.
    #[tokio::test]
    async fn per_group_and_all() {
        let g1: KafkaFutureImpl<ShareGroupOffsets> = KafkaFutureImpl::new();
        let g2: KafkaFutureImpl<ShareGroupOffsets> = KafkaFutureImpl::new();
        g1.complete(offsets(1));
        g2.complete(offsets(2));
        let result = ListShareGroupOffsetsResult::new(HashMap::from([
            ("g1".to_string(), g1.future()),
            ("g2".to_string(), g2.future()),
        ]));
        assert_eq!(result.partitions_to_offset_info("g1").unwrap().get().await.unwrap(), offsets(1));
        assert_eq!(
            result.all().get().await.unwrap(),
            HashMap::from([("g1".to_string(), offsets(1)), ("g2".to_string(), offsets(2))])
        );
    }

    /// New test, no Java original: asking for a group that was not requested
    /// fails at once with Java's message.
    #[test]
    fn unknown_group_is_illegal_argument() {
        let result = ListShareGroupOffsetsResult::new(HashMap::new());
        let error = result.partitions_to_offset_info("absent").unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(error.message(), "Group ID not found: absent");
    }

    /// New test, no Java original: one failed group fails `all()`.
    #[tokio::test]
    async fn all_fails_when_one_group_fails() {
        let g1: KafkaFutureImpl<ShareGroupOffsets> = KafkaFutureImpl::new();
        let g2: KafkaFutureImpl<ShareGroupOffsets> = KafkaFutureImpl::new();
        g1.complete(offsets(1));
        g2.complete_with_error(Error::new(Errors::GroupAuthorizationFailed));
        let result = ListShareGroupOffsetsResult::new(HashMap::from([
            ("g1".to_string(), g1.future()),
            ("g2".to_string(), g2.future()),
        ]));
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::GroupAuthorizationFailed);
    }
}

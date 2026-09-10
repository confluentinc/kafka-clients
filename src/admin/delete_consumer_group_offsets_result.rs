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

//! The result of `Admin::delete_consumer_group_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult`.

use std::collections::{HashMap, HashSet};

use crate::common::protocol::Errors;
use crate::common::{Error, KafkaFuture, TopicPartition};

/// The per-partition delete errors carried by the underlying future.
type PartitionErrors = HashMap<TopicPartition, Errors>;

/// The result of `Admin::delete_consumer_group_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult`.
#[derive(Clone, Debug)]
pub struct DeleteConsumerGroupOffsetsResult {
    future: KafkaFuture<PartitionErrors>,
    partitions: HashSet<TopicPartition>,
}

impl DeleteConsumerGroupOffsetsResult {
    /// Creates a result wrapping the single group's per-partition-error future
    /// and the set of partitions from the original request.
    pub(crate) fn new(future: KafkaFuture<PartitionErrors>, partitions: HashSet<TopicPartition>) -> Self {
        Self { future, partitions }
    }

    /// Returns a future which can be used to check the result for a given
    /// partition.
    ///
    /// Mirrors `partitionResult(TopicPartition)`.
    ///
    /// # Errors
    ///
    /// Returns an error (Java's `IllegalArgumentException`) immediately if the
    /// partition was not included in the original request. The returned future
    /// fails if the deletion for the partition failed (or the partition is
    /// missing from the response).
    pub fn partition_result(&self, partition: &TopicPartition) -> Result<KafkaFuture<()>, Error> {
        if !self.partitions.contains(partition) {
            return Err(Error::local_illegal_argument(format!(
                "Partition {partition} was not included in the original request"
            )));
        }
        let partition = partition.clone();
        Ok(self
            .future
            .then_apply_try(move |topic_partitions| match sub_level_error(&topic_partitions, &partition) {
                Some(error) => Err(error),
                None => Ok(()),
            }))
    }

    /// Returns a future which succeeds only if all the deletions succeed.
    /// If not, the first partition error is returned.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        // Sort for a stable "first error" (Java relies on set iteration order,
        // which is unspecified anyway).
        let mut partitions: Vec<TopicPartition> = self.partitions.iter().cloned().collect();
        partitions.sort_by_key(|tp| (tp.topic().to_string(), tp.partition()));
        self.future.then_apply_try(move |topic_partitions| {
            for partition in &partitions {
                if let Some(error) = sub_level_error(&topic_partitions, partition) {
                    return Err(error);
                }
            }
            Ok(())
        })
    }
}

/// Mirrors `KafkaAdminClient.getSubLevelError` specialised for the delete-offsets
/// result: an absent partition yields the "not included in the response"
/// `IllegalArgumentException`, a present partition yields its error (or `None`
/// when the error is `NONE`).
fn sub_level_error(partition_level_errors: &PartitionErrors, partition: &TopicPartition) -> Option<Error> {
    match partition_level_errors.get(partition) {
        None => Some(Error::local_illegal_argument(format!(
            "Offset deletion result for partition \"{partition}\" was not included in the response"
        ))),
        Some(&Errors::None) => None,
        Some(&error) => Some(Error::new(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;

    fn tp_zero() -> TopicPartition {
        TopicPartition::new("topic", 0)
    }

    fn tp_one() -> TopicPartition {
        TopicPartition::new("topic", 1)
    }

    fn partitions() -> HashSet<TopicPartition> {
        HashSet::from([tp_zero(), tp_one()])
    }

    /// Translated from `testTopLevelErrorConstructor`.
    #[tokio::test]
    async fn top_level_error_constructor() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::group_authorization("group"));
        let result = DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions());
        assert!(matches!(result.all().get().await.unwrap_err(), Error::GroupAuthorization(_)));
    }

    /// Translated from `testPartitionLevelErrorConstructor`.
    #[tokio::test]
    async fn partition_level_error_constructor() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([
            (tp_zero(), Errors::None),
            (tp_one(), Errors::UnknownTopicOrPartition),
        ]));
        let result = DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnknownTopicOrPartition);
        assert_eq!(result.partition_result(&tp_zero()).unwrap().get().await.unwrap(), ());
        assert_eq!(
            result.partition_result(&tp_one()).unwrap().get().await.unwrap_err().error(),
            Errors::UnknownTopicOrPartition
        );
    }

    /// Translated from `testPartitionMissingInResponseErrorConstructor`.
    #[tokio::test]
    async fn partition_missing_in_response_error_constructor() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([(tp_zero(), Errors::None)]));
        let result = DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions());
        assert!(matches!(result.all().get().await.unwrap_err(), Error::LocalIllegalArgument(_)));
        assert_eq!(result.partition_result(&tp_zero()).unwrap().get().await.unwrap(), ());
        assert!(matches!(
            result.partition_result(&tp_one()).unwrap().get().await.unwrap_err(),
            Error::LocalIllegalArgument(_)
        ));
    }

    /// Translated from `testPartitionMissingInRequestErrorConstructor` (the
    /// synchronous `IllegalArgumentException` when asking about a partition not
    /// in the original request).
    #[tokio::test]
    async fn partition_missing_in_request_error_constructor() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([
            (tp_zero(), Errors::None),
            (tp_one(), Errors::UnknownTopicOrPartition),
        ]));
        let result = DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions());
        assert!(matches!(
            result.partition_result(&TopicPartition::new("invalid-topic", 0)),
            Err(Error::LocalIllegalArgument(_))
        ));
    }

    /// Translated from `testNoErrorConstructor`.
    #[tokio::test]
    async fn no_error_constructor() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([(tp_zero(), Errors::None), (tp_one(), Errors::None)]));
        let result = DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions());
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp_zero()).unwrap().get().await.unwrap(), ());
        assert_eq!(result.partition_result(&tp_one()).unwrap().get().await.unwrap(), ());
    }
}

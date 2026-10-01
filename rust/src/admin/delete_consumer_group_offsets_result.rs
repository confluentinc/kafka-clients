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
#[doc(alias = "org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult")]
pub struct DeleteConsumerGroupOffsetsResult {
    future: KafkaFuture<PartitionErrors>,
    partitions: HashSet<TopicPartition>,
}

impl DeleteConsumerGroupOffsetsResult {
    /// Creates a result wrapping the single group's per-partition-error future
    /// and the set of partitions from the original request.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult#DeleteConsumerGroupOffsetsResult")]
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
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult#partitionResult")]
    pub fn partition_result(&self, partition: &TopicPartition) -> Result<KafkaFuture<()>, Error> {
        self.ensure_requested(partition)?;
        let partition = partition.clone();
        Ok(self
            .future
            .then_apply_try(move |topic_partitions| partition_result_of(&topic_partitions, &partition)))
    }

    /// Returns a future which succeeds only if all the deletions succeed.
    /// If not, the first partition error is returned.
    ///
    /// Mirrors `all()`.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteConsumerGroupOffsetsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        let partitions = self.sorted_partitions();
        self.future
            .then_apply_try(move |topic_partitions| all_of(&topic_partitions, &partitions))
    }

    /// The single future every accessor derives from — Java's
    /// `KafkaFuture<Map<TopicPartition, Errors>> future` field.
    ///
    /// Crate-internal so the C FFI can await the one outcome once and then
    /// evaluate [`partition_result`](Self::partition_result) /
    /// [`all`](Self::all) against it through
    /// [`resolved_partition_result`](Self::resolved_partition_result) /
    /// [`resolved_all`](Self::resolved_all).
    #[cfg_attr(not(any(test, feature = "ffi")), expect(dead_code))]
    pub(crate) fn future(&self) -> &KafkaFuture<PartitionErrors> {
        &self.future
    }

    /// Evaluates [`partition_result`](Self::partition_result) against an
    /// already-resolved outcome of [`future`](Self::future), without awaiting.
    ///
    /// Same derivation as the future-based accessor: the "not included in the
    /// original request" check comes first and does not depend on the outcome
    /// (Java throws it before touching the future); then a failed outcome
    /// propagates its error, as Java's `whenComplete` forwards the `throwable`.
    #[cfg_attr(not(any(test, feature = "ffi")), expect(dead_code))]
    pub(crate) fn resolved_partition_result(
        &self,
        outcome: &Result<PartitionErrors, Error>,
        partition: &TopicPartition,
    ) -> Result<(), Error> {
        self.ensure_requested(partition)?;
        partition_result_of(outcome.as_ref().map_err(Error::clone)?, partition)
    }

    /// Evaluates [`all`](Self::all) against an already-resolved outcome of
    /// [`future`](Self::future), without awaiting.
    #[cfg_attr(not(any(test, feature = "ffi")), expect(dead_code))]
    pub(crate) fn resolved_all(&self, outcome: &Result<PartitionErrors, Error>) -> Result<(), Error> {
        all_of(outcome.as_ref().map_err(Error::clone)?, &self.sorted_partitions())
    }

    /// Java's synchronous `partitionResult` guard.
    fn ensure_requested(&self, partition: &TopicPartition) -> Result<(), Error> {
        if self.partitions.contains(partition) {
            Ok(())
        } else {
            Err(Error::local_illegal_argument(format!(
                "Partition {partition} was not included in the original request"
            )))
        }
    }

    /// The requested partitions in a stable order, for a deterministic "first
    /// error" in `all()` (Java relies on set iteration order, which is
    /// unspecified anyway).
    fn sorted_partitions(&self) -> Vec<TopicPartition> {
        let mut partitions: Vec<TopicPartition> = self.partitions.iter().cloned().collect();
        partitions.sort_by_key(|tp| (tp.topic().to_string(), tp.partition()));
        partitions
    }
}

/// The body of Java's `partitionResult` `whenComplete` lambda, for a
/// successfully resolved map.
fn partition_result_of(topic_partitions: &PartitionErrors, partition: &TopicPartition) -> Result<(), Error> {
    match sub_level_error(topic_partitions, partition) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// The body of Java's `all()` `whenComplete` lambda, for a successfully
/// resolved map: the first requested partition with a sub-level error fails.
fn all_of(topic_partitions: &PartitionErrors, partitions: &[TopicPartition]) -> Result<(), Error> {
    for partition in partitions {
        if let Some(error) = sub_level_error(topic_partitions, partition) {
            return Err(error);
        }
    }
    Ok(())
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
    use crate::common::internals::KafkaFutureImpl;

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

    /// `partition_result` / `all` and their `resolved_*` twins share one
    /// derivation, so they must agree on every requested key, including the
    /// exact messages; the unrequested-partition guard fires first, whatever
    /// the outcome.
    #[tokio::test]
    async fn resolved_accessors_agree_with_the_future_based_ones() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([(tp_zero(), Errors::None)]));
        let result = DeleteConsumerGroupOffsetsResult::new(handle.future(), partitions());
        let outcome = result.future().get().await;

        for tp in [tp_zero(), tp_one()] {
            let via_future = result.partition_result(&tp).unwrap().get().await;
            let resolved = result.resolved_partition_result(&outcome, &tp);
            assert_eq!(format!("{via_future:?}"), format!("{resolved:?}"), "{tp}");
        }
        assert_eq!(
            result.resolved_partition_result(&outcome, &tp_one()).unwrap_err().message(),
            "Offset deletion result for partition \"topic-1\" was not included in the response"
        );
        assert_eq!(
            format!("{:?}", result.all().get().await),
            format!("{:?}", result.resolved_all(&outcome))
        );

        let unrequested = TopicPartition::new("invalid-topic", 0);
        let via_future = result.partition_result(&unrequested).unwrap_err();
        let failed: Result<PartitionErrors, Error> = Err(Error::group_authorization("g"));
        for outcome in [&outcome, &failed] {
            let resolved = result.resolved_partition_result(outcome, &unrequested).unwrap_err();
            assert_eq!(format!("{via_future:?}"), format!("{resolved:?}"));
            assert_eq!(
                resolved.message(),
                "Partition invalid-topic-0 was not included in the original request"
            );
        }

        // A failed outcome reaches the requested-key accessors unchanged.
        assert!(matches!(
            result.resolved_partition_result(&failed, &tp_zero()),
            Err(Error::GroupAuthorization(_))
        ));
        assert!(matches!(result.resolved_all(&failed), Err(Error::GroupAuthorization(_))));
    }
}

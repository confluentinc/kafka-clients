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

//! The result of `Admin::alter_consumer_group_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult`.

use std::collections::HashMap;

use crate::common::protocol::Errors;
use crate::common::{Error, KafkaFuture, TopicPartition};

/// The per-partition commit errors carried by the underlying future.
type PartitionErrors = HashMap<TopicPartition, Errors>;

/// The result of `Admin::alter_consumer_group_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult")]
pub struct AlterConsumerGroupOffsetsResult {
    future: KafkaFuture<PartitionErrors>,
}

impl AlterConsumerGroupOffsetsResult {
    /// Creates a result wrapping the single group's per-partition-error future.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult#AlterConsumerGroupOffsetsResult")]
    pub(crate) fn new(future: KafkaFuture<PartitionErrors>) -> Self {
        Self { future }
    }

    /// Returns a future which can be used to check the result for a given
    /// partition.
    ///
    /// Mirrors `partitionResult(TopicPartition)`. The returned future fails
    /// (Java's `IllegalArgumentException`) if the partition was not part of the
    /// alter request, and otherwise fails with the partition's error if it was
    /// not `NONE`.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult#partitionResult")]
    pub fn partition_result(&self, partition: &TopicPartition) -> KafkaFuture<()> {
        let partition = partition.clone();
        self.future
            .then_apply_try(move |topic_partitions| partition_result_of(&topic_partitions, &partition))
    }

    /// Returns a future which succeeds if all the alter offsets succeed.
    ///
    /// Mirrors `all()`.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConsumerGroupOffsetsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        self.future
            .then_apply_try(|topic_partition_errors| all_of(&topic_partition_errors))
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
    /// Same derivation as the future-based accessor (both call one private
    /// helper): a failed outcome propagates its error, exactly as Java's
    /// `whenComplete` forwards the `throwable`.
    #[cfg_attr(not(any(test, feature = "ffi")), expect(dead_code))]
    pub(crate) fn resolved_partition_result(
        &self,
        outcome: &Result<PartitionErrors, Error>,
        partition: &TopicPartition,
    ) -> Result<(), Error> {
        partition_result_of(outcome.as_ref().map_err(Error::clone)?, partition)
    }

    /// Evaluates [`all`](Self::all) against an already-resolved outcome of
    /// [`future`](Self::future), without awaiting.
    #[cfg_attr(not(any(test, feature = "ffi")), expect(dead_code))]
    pub(crate) fn resolved_all(&self, outcome: &Result<PartitionErrors, Error>) -> Result<(), Error> {
        all_of(outcome.as_ref().map_err(Error::clone)?)
    }
}

/// The body of Java's `partitionResult` `whenComplete` lambda, for a
/// successfully resolved map.
fn partition_result_of(topic_partitions: &PartitionErrors, partition: &TopicPartition) -> Result<(), Error> {
    match topic_partitions.get(partition) {
        None => Err(Error::local_illegal_argument(format!(
            "Alter offset for partition \"{partition}\" was not attempted"
        ))),
        Some(&Errors::None) => Ok(()),
        Some(&error) => Err(Error::new(error)),
    }
}

/// The body of Java's `all()` `thenApply` lambda.
fn all_of(topic_partition_errors: &PartitionErrors) -> Result<(), Error> {
    let mut partitions_failed: Vec<&TopicPartition> = topic_partition_errors
        .iter()
        .filter(|(_, error)| **error != Errors::None)
        .map(|(tp, _)| tp)
        .collect();
    for &error in topic_partition_errors.values() {
        if error != Errors::None {
            // Sort only when reporting, for a stable message (Java relies on
            // the map's list order, which is unspecified anyway).
            partitions_failed.sort_by_key(|tp| (tp.topic().to_string(), tp.partition()));
            let list = partitions_failed.iter().map(|tp| tp.to_string()).collect::<Vec<_>>().join(", ");
            return Err(Error::with_message(
                error,
                format!("Failed altering group offsets for the following partitions: [{list}]"),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;

    fn t0p0() -> TopicPartition {
        TopicPartition::new("foo", 0)
    }

    fn t0p1() -> TopicPartition {
        TopicPartition::new("bar", 0)
    }

    #[tokio::test]
    async fn all_and_partition_result_succeed_when_no_errors() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([(t0p0(), Errors::None), (t0p1(), Errors::None)]));
        let result = AlterConsumerGroupOffsetsResult::new(handle.future());
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.partition_result(&t0p0()).get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn partition_result_not_attempted_is_illegal_argument() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([(t0p0(), Errors::None)]));
        let result = AlterConsumerGroupOffsetsResult::new(handle.future());
        let err = result
            .partition_result(&TopicPartition::new("absent", 0))
            .get()
            .await
            .unwrap_err();
        assert!(matches!(err, Error::LocalIllegalArgument(_)));
    }

    #[tokio::test]
    async fn partition_error_surfaces_in_partition_result_and_all() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([
            (t0p0(), Errors::None),
            (t0p1(), Errors::UnknownTopicOrPartition),
        ]));
        let result = AlterConsumerGroupOffsetsResult::new(handle.future());
        assert_eq!(result.partition_result(&t0p0()).get().await.unwrap(), ());
        assert_eq!(
            result.partition_result(&t0p1()).get().await.unwrap_err().error(),
            Errors::UnknownTopicOrPartition
        );
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::UnknownTopicOrPartition);
    }

    /// `partition_result` / `all` and their `resolved_*` twins share one
    /// derivation, so they must agree on every key — requested-and-ok,
    /// requested-and-failed, and not attempted — including the exact messages.
    #[tokio::test]
    async fn resolved_accessors_agree_with_the_future_based_ones() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(HashMap::from([
            (t0p0(), Errors::None),
            (t0p1(), Errors::UnknownTopicOrPartition),
        ]));
        let result = AlterConsumerGroupOffsetsResult::new(handle.future());
        let outcome = result.future().get().await;

        for tp in [t0p0(), t0p1(), TopicPartition::new("absent", 3)] {
            let via_future = result.partition_result(&tp).get().await;
            let resolved = result.resolved_partition_result(&outcome, &tp);
            assert_eq!(format!("{via_future:?}"), format!("{resolved:?}"), "{tp}");
        }
        assert_eq!(
            result
                .resolved_partition_result(&outcome, &TopicPartition::new("absent", 3))
                .unwrap_err()
                .message(),
            "Alter offset for partition \"absent-3\" was not attempted"
        );

        let via_future = result.all().get().await;
        let resolved = result.resolved_all(&outcome);
        assert_eq!(format!("{via_future:?}"), format!("{resolved:?}"));
        let err = resolved.unwrap_err();
        assert_eq!(err.error(), Errors::UnknownTopicOrPartition);
        assert_eq!(
            err.message(),
            "Failed altering group offsets for the following partitions: [bar-0]"
        );
    }

    /// A failed outcome reaches every resolved accessor unchanged, as Java's
    /// `whenComplete` / `thenApply` forward the `throwable`.
    #[test]
    fn resolved_accessors_propagate_a_failed_outcome() {
        let result = AlterConsumerGroupOffsetsResult::new(KafkaFutureImpl::new().future());
        let outcome: Result<PartitionErrors, Error> = Err(Error::group_authorization("g"));
        assert!(matches!(
            result.resolved_partition_result(&outcome, &t0p0()),
            Err(Error::GroupAuthorization(_))
        ));
        assert!(matches!(result.resolved_all(&outcome), Err(Error::GroupAuthorization(_))));
    }
}

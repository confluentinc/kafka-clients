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
pub struct AlterConsumerGroupOffsetsResult {
    future: KafkaFuture<PartitionErrors>,
}

impl AlterConsumerGroupOffsetsResult {
    /// Creates a result wrapping the single group's per-partition-error future.
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
    pub fn partition_result(&self, partition: &TopicPartition) -> KafkaFuture<()> {
        let partition = partition.clone();
        self.future
            .then_apply_try(move |topic_partitions| match topic_partitions.get(&partition) {
                None => Err(Error::local_illegal_argument(format!(
                    "Alter offset for partition \"{partition}\" was not attempted"
                ))),
                Some(&Errors::None) => Ok(()),
                Some(&error) => Err(Error::new(error)),
            })
    }

    /// Returns a future which succeeds if all the alter offsets succeed.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        self.future.then_apply_try(|topic_partition_errors| {
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::kafka_future::KafkaFutureImpl;

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
}

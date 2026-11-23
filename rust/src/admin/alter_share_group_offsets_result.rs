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

//! The result of `Admin::alter_share_group_offsets_with_options`.

use std::collections::HashMap;

use crate::common::{Error, KafkaFuture, TopicPartition};

/// The per-partition errors carried by the underlying future: `None` for a
/// partition whose offset was altered (Java's `null` value).
type PartitionErrors = HashMap<TopicPartition, Option<Error>>;

/// The result of the `Admin::alter_share_group_offsets_with_options` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult")]
pub struct AlterShareGroupOffsetsResult {
    future: KafkaFuture<PartitionErrors>,
}

impl AlterShareGroupOffsetsResult {
    /// Creates a result wrapping the single group's per-partition-error future.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult#AlterShareGroupOffsetsResult")]
    pub(crate) fn new(future: KafkaFuture<PartitionErrors>) -> Self {
        Self { future }
    }

    /// Returns a future which can be used to check the result for a given partition.
    ///
    /// # Arguments
    ///
    /// - `partition` - the partition to check.
    ///
    /// # Errors
    ///
    /// The returned future completes with:
    /// - `Error::is_local_illegal_argument_error` if the alter was not attempted for the
    ///   partition;
    /// - the partition's own error, if its alter failed.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult#partitionResult")]
    pub fn partition_result(&self, partition: &TopicPartition) -> KafkaFuture<()> {
        let partition = partition.clone();
        self.future.then_apply(
            move |topic_partitions: PartitionErrors| match topic_partitions.get(&partition) {
                None => Err(Error::local_illegal_argument(format!(
                    "Alter offset for partition \"{partition}\" was not attempted"
                ))),
                Some(None) => Ok(()),
                Some(Some(error)) => Err(error.clone()),
            },
        )
    }

    /// Returns a future which succeeds if all the alter offsets succeed. If not, the first topic
    /// error shall be returned.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterShareGroupOffsetsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        self.future.then_apply(|topic_partition_errors: PartitionErrors| {
            // Java takes the first error, and lists the failed partitions, in its map's
            // iteration order, which is unspecified; topic-partition order keeps the
            // error and its message stable.
            let mut entries: Vec<(&TopicPartition, &Option<Error>)> = topic_partition_errors.iter().collect();
            entries.sort_by(|(a, _), (b, _)| (a.topic(), a.partition()).cmp(&(b.topic(), b.partition())));
            let partitions_failed = entries
                .iter()
                .filter(|(_, error)| error.is_some())
                .map(|(partition, _)| partition.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            for (_, error) in &entries {
                if let Some(error) = error {
                    let failed =
                        format!("Failed altering group offsets for the following partitions: [{partitions_failed}]");
                    let message = error.message();
                    let message = if message.is_empty() {
                        failed
                    } else {
                        format!("{message} {failed}")
                    };
                    return Err(Error::with_message(error.error(), message));
                }
            }
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    fn foo0() -> TopicPartition {
        TopicPartition::new("foo", 0)
    }

    fn foo1() -> TopicPartition {
        TopicPartition::new("foo", 1)
    }

    fn bar0() -> TopicPartition {
        TopicPartition::new("bar", 0)
    }

    fn result(errors: PartitionErrors) -> AlterShareGroupOffsetsResult {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete(errors);
        AlterShareGroupOffsetsResult::new(handle.future())
    }

    /// New test, no Java original (Java has no `AlterShareGroupOffsetsResultTest`):
    /// with no partition errors, `all()` and every attempted partition succeed,
    /// and an unattempted partition fails with the exact Java message.
    #[tokio::test]
    async fn succeeds_when_no_partition_failed() {
        let result = result(HashMap::from([(foo0(), None), (bar0(), None)]));
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.partition_result(&foo0()).get().await.unwrap(), ());
        let error = result.partition_result(&foo1()).get().await.unwrap_err();
        assert!(error.is_local_illegal_argument_error(), "{error:?}");
        assert_eq!(error.message(), "Alter offset for partition \"foo-1\" was not attempted");
    }

    /// New test, no Java original: a partition error fails that partition and
    /// `all()`, whose message appends the failed partitions to the error's own
    /// message, as Java's `all()` does.
    #[tokio::test]
    async fn partition_error_surfaces_in_partition_result_and_all() {
        let failure = Errors::TopicAuthorizationFailed.error_with_optional_message(Some("Topic authorization failed."));
        let result = result(HashMap::from([(foo0(), None), (foo1(), Some(failure)), (bar0(), None)]));
        assert_eq!(result.partition_result(&foo0()).get().await.unwrap(), ());
        let error = result.partition_result(&foo1()).get().await.unwrap_err();
        assert_eq!(error.error(), Errors::TopicAuthorizationFailed);
        assert_eq!(error.message(), "Topic authorization failed.");

        let error = result.all().get().await.unwrap_err();
        assert_eq!(error.error(), Errors::TopicAuthorizationFailed);
        assert_eq!(
            error.message(),
            "Topic authorization failed. Failed altering group offsets for the following partitions: [foo-1]"
        );
    }

    /// New test, no Java original: the failed-partition list names every
    /// failed partition, in topic-partition order.
    #[tokio::test]
    async fn all_lists_every_failed_partition() {
        let failure = || Some(Errors::GroupAuthorizationFailed.error_with_optional_message(Some("denied")));
        let result = result(HashMap::from([(foo1(), failure()), (foo0(), failure()), (bar0(), failure())]));
        let error = result.all().get().await.unwrap_err();
        assert_eq!(error.error(), Errors::GroupAuthorizationFailed);
        assert_eq!(
            error.message(),
            "denied Failed altering group offsets for the following partitions: [bar-0, foo-0, foo-1]"
        );
    }

    /// New test, no Java original: an error with an empty message is reported
    /// without the leading message, as Java's `all()` does.
    #[tokio::test]
    async fn all_omits_an_empty_message() {
        let failure = Errors::KafkaStorageError.error_with_optional_message(Some(""));
        let result = result(HashMap::from([(foo0(), Some(failure))]));
        let error = result.all().get().await.unwrap_err();
        assert_eq!(error.error(), Errors::KafkaStorageError);
        assert_eq!(
            error.message(),
            "Failed altering group offsets for the following partitions: [foo-0]"
        );
    }

    /// New test, no Java original: a failed request fails `all()` and every
    /// partition with the request's error.
    #[tokio::test]
    async fn request_error_fails_everything() {
        let handle: KafkaFutureImpl<PartitionErrors> = KafkaFutureImpl::new();
        handle.complete_with_error(Error::new(Errors::CoordinatorNotAvailable));
        let result = AlterShareGroupOffsetsResult::new(handle.future());
        assert_eq!(result.all().get().await.unwrap_err().error(), Errors::CoordinatorNotAvailable);
        assert_eq!(
            result.partition_result(&foo0()).get().await.unwrap_err().error(),
            Errors::CoordinatorNotAvailable
        );
    }
}

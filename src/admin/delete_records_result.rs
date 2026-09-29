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

//! The result of `Admin::delete_records`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteRecordsResult`.

use std::collections::HashMap;

use crate::admin::DeletedRecords;
use crate::common::{KafkaFuture, TopicPartition};

/// The result of `Admin::delete_records`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteRecordsResult`.
#[derive(Clone, Debug)]
pub struct DeleteRecordsResult {
    futures: HashMap<TopicPartition, KafkaFuture<DeletedRecords>>,
}

impl DeleteRecordsResult {
    /// Creates a result from the per-partition futures.
    pub(crate) fn new(futures: HashMap<TopicPartition, KafkaFuture<DeletedRecords>>) -> Self {
        Self { futures }
    }

    /// Return a map from topic partition to futures which can be used to check
    /// the status of individual deletions.
    pub fn low_watermarks(&self) -> &HashMap<TopicPartition, KafkaFuture<DeletedRecords>> {
        &self.futures
    }

    /// Return a future which succeeds only if all the records deletions succeed.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().map(|f| f.then_apply(|_| ())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;

    #[tokio::test]
    async fn all_succeeds_when_each_completes() {
        let h1: KafkaFutureImpl<DeletedRecords> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h1.future());
        let result = DeleteRecordsResult::new(map);
        h1.complete(DeletedRecords::new(3));
        assert_eq!(result.all().get().await.unwrap(), ());
        let lw = result.low_watermarks().get(&TopicPartition::new("t", 0)).unwrap();
        assert_eq!(lw.get().await.unwrap().low_watermark(), 3);
    }

    #[tokio::test]
    async fn all_fails_if_one_fails() {
        let h1: KafkaFutureImpl<DeletedRecords> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h1.future());
        let result = DeleteRecordsResult::new(map);
        h1.complete_with_error(Error::local_illegal_state("boom".to_string()));
        assert!(result.all().get().await.is_err());
    }
}

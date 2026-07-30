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

//! The result of `Admin::abort_transaction`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AbortTransactionResult`.

use std::collections::HashMap;

use crate::common::{KafkaFuture, TopicPartition};

/// The result of `Admin::abort_transaction`.
///
/// Corresponds to `org.apache.kafka.clients.admin.AbortTransactionResult`.
#[derive(Clone, Debug)]
pub struct AbortTransactionResult {
    futures: HashMap<TopicPartition, KafkaFuture<()>>,
}

impl AbortTransactionResult {
    /// Creates a result from the per-partition futures.
    pub(crate) fn new(futures: HashMap<TopicPartition, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a future that completes when the transaction abort completes (or
    /// fails on error / timeout).
    ///
    /// Mirrors `AbortTransactionResult.all`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::KafkaError;
    use crate::common::kafka_future::KafkaFutureImpl;

    #[tokio::test]
    async fn all_succeeds_when_partition_completes() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h.future());
        let result = AbortTransactionResult::new(map);
        h.complete(());
        assert_eq!(result.all().get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn all_fails_when_partition_fails() {
        let h: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("t", 0), h.future());
        let result = AbortTransactionResult::new(map);
        h.complete_exceptionally(KafkaError::IllegalState("boom".to_string()));
        assert!(result.all().get().await.is_err());
    }
}

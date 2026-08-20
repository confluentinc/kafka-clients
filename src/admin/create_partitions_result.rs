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

//! The result of `Admin::create_partitions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.CreatePartitionsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;

/// The result of `Admin::create_partitions`.
///
/// Corresponds to `org.apache.kafka.clients.admin.CreatePartitionsResult`.
#[derive(Clone, Debug)]
pub struct CreatePartitionsResult {
    values: HashMap<String, KafkaFuture<()>>,
}

impl CreatePartitionsResult {
    /// Creates a result from the per-topic futures.
    pub(crate) fn new(values: HashMap<String, KafkaFuture<()>>) -> Self {
        Self { values }
    }

    /// Return a map from topic names to futures, which can be used to check the
    /// status of individual partition creations.
    pub fn values(&self) -> &HashMap<String, KafkaFuture<()>> {
        &self.values
    }

    /// Return a future which succeeds if all the partition creations succeed.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.values.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::kafka_future::KafkaFutureImpl;

    #[tokio::test]
    async fn all_succeeds_when_each_completes() {
        let h1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert("a".to_string(), h1.future());
        map.insert("b".to_string(), h2.future());
        let result = CreatePartitionsResult::new(map);
        h1.complete(());
        h2.complete(());
        assert_eq!(result.all().get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn all_fails_if_one_fails() {
        let h1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let mut map = HashMap::new();
        map.insert("a".to_string(), h1.future());
        let result = CreatePartitionsResult::new(map);
        h1.complete_exceptionally(Error::illegal_state("boom".to_string()));
        assert!(result.all().get().await.is_err());
    }
}

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

//! The result of the `Admin::delete_consumer_groups` call for a
//! `Collection<String>` (group id) input.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteConsumerGroupsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;

/// The result of the `Admin::delete_consumer_groups` call for a
/// `Collection<String>` (group id) input.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteConsumerGroupsResult`.
#[derive(Clone, Debug)]
pub struct DeleteConsumerGroupsResult {
    futures: HashMap<String, KafkaFuture<()>>,
}

impl DeleteConsumerGroupsResult {
    /// Creates a result from a per-group-id future map.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from group id to futures which can be used to check the
    /// status of individual deletions.
    ///
    /// Mirrors `deletedGroups()`.
    pub fn deleted_groups(&self) -> HashMap<String, KafkaFuture<()>> {
        self.futures.clone()
    }

    /// Returns a future which succeeds only if all the consumer group deletions
    /// succeed.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;

    #[tokio::test]
    async fn all_succeeds_when_every_group_succeeds() {
        let h1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        h1.complete(());
        h2.complete(());
        let result = DeleteConsumerGroupsResult::new(HashMap::from([
            ("g1".to_string(), h1.future()),
            ("g2".to_string(), h2.future()),
        ]));
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.deleted_groups()["g1"].get().await.unwrap(), ());
    }

    #[tokio::test]
    async fn all_fails_when_any_group_fails() {
        let h1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let h2: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        h1.complete(());
        h2.complete_with_error(Error::group_authorization("g2"));
        let result = DeleteConsumerGroupsResult::new(HashMap::from([
            ("g1".to_string(), h1.future()),
            ("g2".to_string(), h2.future()),
        ]));
        assert!(matches!(result.all().get().await.unwrap_err(), Error::GroupAuthorization(_)));
        assert_eq!(result.deleted_groups()["g1"].get().await.unwrap(), ());
    }
}

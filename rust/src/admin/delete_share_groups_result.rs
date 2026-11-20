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

//! The result of `Admin::delete_share_groups_with_options`.

use std::collections::HashMap;

use crate::common::KafkaFuture;

/// The result of the `Admin::delete_share_groups_with_options` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupsResult")]
pub struct DeleteShareGroupsResult {
    futures: HashMap<String, KafkaFuture<()>>,
}

impl DeleteShareGroupsResult {
    /// Creates a result from a per-group-id future map.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupsResult#DeleteShareGroupsResult")]
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from group id to futures which can be used to check the status of
    /// individual deletions.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupsResult#deletedGroups")]
    pub fn deleted_groups(&self) -> HashMap<String, KafkaFuture<()>> {
        self.futures.clone()
    }

    /// Returns a future which succeeds only if all the share group deletions succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.DeleteShareGroupsResult#all")]
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::Error;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;

    /// New test, no Java original (Java has no `DeleteShareGroupsResultTest`):
    /// `all()` succeeds when every deletion does.
    #[tokio::test]
    async fn all_succeeds_when_every_group_succeeds() {
        let g1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let g2: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        g1.complete(());
        g2.complete(());
        let result = DeleteShareGroupsResult::new(HashMap::from([
            ("g1".to_string(), g1.future()),
            ("g2".to_string(), g2.future()),
        ]));
        assert_eq!(result.all().get().await.unwrap(), ());
        assert_eq!(result.deleted_groups().len(), 2);
        assert_eq!(result.deleted_groups()["g1"].get().await.unwrap(), ());
    }

    /// New test, no Java original: one failed deletion fails `all()` with its
    /// error, and leaves the other group's future successful.
    #[tokio::test]
    async fn all_fails_when_one_deletion_fails() {
        let g1: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        let g2: KafkaFutureImpl<()> = KafkaFutureImpl::new();
        g1.complete(());
        g2.complete_with_error(Error::new(Errors::NonEmptyGroup));
        let result = DeleteShareGroupsResult::new(HashMap::from([
            ("g1".to_string(), g1.future()),
            ("g2".to_string(), g2.future()),
        ]));
        let error = result.all().get().await.unwrap_err();
        assert_eq!(error.error(), Errors::NonEmptyGroup);
        assert_eq!(error.message(), Errors::NonEmptyGroup.message());
        assert_eq!(result.deleted_groups()["g1"].get().await.unwrap(), ());
    }
}

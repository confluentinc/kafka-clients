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

//! The result of `Admin::describe_share_groups_with_options`.

use std::collections::HashMap;

use crate::admin::ShareGroupDescription;
use crate::common::KafkaFuture;

/// The result of the `KafkaAdminClient::describe_share_groups_with_options` call.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeShareGroupsResult")]
pub struct DescribeShareGroupsResult {
    futures: HashMap<String, KafkaFuture<ShareGroupDescription>>,
}

impl DescribeShareGroupsResult {
    /// Creates a result from the per-group-id futures.
    ///
    /// # Arguments
    ///
    /// - `futures` - the future of each group id.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeShareGroupsResult#DescribeShareGroupsResult")]
    pub fn new(futures: HashMap<String, KafkaFuture<ShareGroupDescription>>) -> Self {
        Self { futures }
    }

    /// Returns a map from group id to futures which yield share group descriptions.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeShareGroupsResult#describedGroups")]
    pub fn described_groups(&self) -> HashMap<String, KafkaFuture<ShareGroupDescription>> {
        self.futures.clone()
    }

    /// Returns a future which yields all ShareGroupDescription objects, if all the describes
    /// succeed.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeShareGroupsResult#all")]
    pub fn all(&self) -> KafkaFuture<HashMap<String, ShareGroupDescription>> {
        KafkaFuture::join_map(self.futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::internals::KafkaFutureImpl;
    use crate::common::protocol::Errors;
    use crate::common::{Error, GroupState};

    fn description(group_id: &str) -> ShareGroupDescription {
        ShareGroupDescription::new(group_id, Vec::new(), GroupState::Stable, None, 1, 1)
    }

    /// New test, no Java original (Java has no `DescribeShareGroupsResultTest`):
    /// `all()` collects every group's description.
    #[tokio::test]
    async fn all_collects_every_group() {
        let g1: KafkaFutureImpl<ShareGroupDescription> = KafkaFutureImpl::new();
        let g2: KafkaFutureImpl<ShareGroupDescription> = KafkaFutureImpl::new();
        g1.complete(description("g1"));
        g2.complete(description("g2"));
        let result = DescribeShareGroupsResult::new(HashMap::from([
            ("g1".to_string(), g1.future()),
            ("g2".to_string(), g2.future()),
        ]));
        assert_eq!(result.described_groups().len(), 2);
        assert_eq!(
            result.all().get().await.unwrap(),
            HashMap::from([
                ("g1".to_string(), description("g1")),
                ("g2".to_string(), description("g2"))
            ])
        );
    }

    /// New test, no Java original: one failed describe fails `all()` with its
    /// error, while the other group's future still succeeds.
    #[tokio::test]
    async fn all_fails_when_one_describe_fails() {
        let g1: KafkaFutureImpl<ShareGroupDescription> = KafkaFutureImpl::new();
        let g2: KafkaFutureImpl<ShareGroupDescription> = KafkaFutureImpl::new();
        g1.complete(description("g1"));
        g2.complete_with_error(Errors::GroupIdNotFound.error_with_optional_message(Some("Group g2 not found.")));
        let result = DescribeShareGroupsResult::new(HashMap::from([
            ("g1".to_string(), g1.future()),
            ("g2".to_string(), g2.future()),
        ]));
        let error = result.all().get().await.unwrap_err();
        assert!(matches!(error, Error::GroupIdNotFound(_)), "{error:?}");
        assert_eq!(error.message(), "Group g2 not found.");
        assert_eq!(result.described_groups()["g1"].get().await.unwrap(), description("g1"));
    }
}

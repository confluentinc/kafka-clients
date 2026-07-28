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

//! The result of `Admin::describe_classic_groups`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeClassicGroupsResult`.

use std::collections::HashMap;

use crate::admin::ClassicGroupDescription;
use crate::common::KafkaFuture;

/// The result of `Admin::describe_classic_groups`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeClassicGroupsResult`.
#[derive(Clone, Debug)]
pub struct DescribeClassicGroupsResult {
    futures: HashMap<String, KafkaFuture<ClassicGroupDescription>>,
}

impl DescribeClassicGroupsResult {
    #[allow(dead_code)] // wired by KafkaAdminClient later in this phase
    /// Creates a result from the per-group-id futures.
    pub(crate) fn new(futures: HashMap<String, KafkaFuture<ClassicGroupDescription>>) -> Self {
        Self { futures }
    }

    /// A map from group id to futures yielding group descriptions.
    ///
    /// Mirrors `describedGroups()`.
    pub fn described_groups(&self) -> HashMap<String, KafkaFuture<ClassicGroupDescription>> {
        self.futures.clone()
    }

    /// A future yielding all descriptions if all describes succeed.
    ///
    /// Mirrors `all()`.
    pub fn all(&self) -> KafkaFuture<HashMap<String, ClassicGroupDescription>> {
        KafkaFuture::join_map(self.futures.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}

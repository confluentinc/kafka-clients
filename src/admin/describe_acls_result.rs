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

//! The result of `Admin::describe_acls`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeAclsResult`.

use crate::common::KafkaFuture;
use crate::common::acl::AclBinding;

/// The result of the `Admin::describe_acls` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeAclsResult`.
#[derive(Clone, Debug)]
pub struct DescribeAclsResult {
    future: KafkaFuture<Vec<AclBinding>>,
}

impl DescribeAclsResult {
    /// Creates a new result from the described-ACLs future.
    pub fn new(future: KafkaFuture<Vec<AclBinding>>) -> Self {
        Self { future }
    }

    /// Return a future containing the ACLs requested.
    pub fn values(&self) -> &KafkaFuture<Vec<AclBinding>> {
        &self.future
    }
}

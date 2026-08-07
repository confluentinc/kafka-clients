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

//! The result of `Admin::create_acls`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.CreateAclsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::acl::AclBinding;

/// The result of the `Admin::create_acls` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.CreateAclsResult`.
#[derive(Clone, Debug)]
pub struct CreateAclsResult {
    futures: HashMap<AclBinding, KafkaFuture<()>>,
}

impl CreateAclsResult {
    /// Creates a new result from the per-binding futures.
    pub fn new(futures: HashMap<AclBinding, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Return a map from ACL bindings to futures which can be used to check the
    /// status of the creation of each ACL binding.
    pub fn values(&self) -> &HashMap<AclBinding, KafkaFuture<()>> {
        &self.futures
    }

    /// Return a future which succeeds only if all the ACL creations succeed.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

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

//! The result of `Admin::incremental_alter_configs`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterConfigsResult`.

use std::collections::HashMap;

use crate::common::KafkaFuture;
use crate::common::config::ConfigResource;

/// The result of the `Admin::incremental_alter_configs` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterConfigsResult`.
pub struct AlterConfigsResult {
    futures: HashMap<ConfigResource, KafkaFuture<()>>,
}

impl AlterConfigsResult {
    /// Creates a new result from the per-resource futures.
    pub(crate) fn new(futures: HashMap<ConfigResource, KafkaFuture<()>>) -> Self {
        Self { futures }
    }

    /// Returns a map from resources to futures which can be used to check the
    /// status of the alteration for each resource.
    pub fn values(&self) -> &HashMap<ConfigResource, KafkaFuture<()>> {
        &self.futures
    }

    /// Returns a future which succeeds only if all the alteration requests
    /// succeed.
    ///
    /// Corresponds to `AlterConfigsResult.all`.
    pub fn all(&self) -> KafkaFuture<()> {
        KafkaFuture::all_of(self.futures.values().cloned().collect())
    }
}

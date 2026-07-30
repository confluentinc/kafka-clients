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

//! The result of `Admin::list_config_resources`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListConfigResourcesResult`.

use crate::common::KafkaFuture;
use crate::common::config::ConfigResource;

/// The result of the `Admin::list_config_resources` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListConfigResourcesResult`.
pub struct ListConfigResourcesResult {
    future: KafkaFuture<Vec<ConfigResource>>,
}

impl ListConfigResourcesResult {
    /// Creates a new result from the config-resources future.
    pub(crate) fn new(future: KafkaFuture<Vec<ConfigResource>>) -> Self {
        Self { future }
    }

    /// Returns a future that yields the collection of config resources in the
    /// cluster.
    pub fn all(&self) -> KafkaFuture<Vec<ConfigResource>> {
        self.future.clone()
    }
}

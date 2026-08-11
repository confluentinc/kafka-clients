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

//! The result of `Admin::list_client_metrics_resources`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListClientMetricsResourcesResult`.

#![allow(deprecated)]

use crate::admin::ClientMetricsResourceListing;
use crate::common::KafkaFuture;

/// The result of the `Admin::list_client_metrics_resources` call.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListClientMetricsResourcesResult`
/// (deprecated since 4.1 in favor of
/// [`ListConfigResourcesResult`](crate::admin::ListConfigResourcesResult)).
#[deprecated(since = "4.1.0", note = "Use Admin::list_config_resources instead")]
pub struct ListClientMetricsResourcesResult {
    future: KafkaFuture<Vec<ClientMetricsResourceListing>>,
}

impl ListClientMetricsResourcesResult {
    /// Creates a new result from the client-metrics-listings future.
    pub(crate) fn new(future: KafkaFuture<Vec<ClientMetricsResourceListing>>) -> Self {
        Self { future }
    }

    /// Returns a future that yields either an exception, or the full set of
    /// client metrics listings.
    ///
    /// In the event of a failure, the future yields nothing but the first
    /// exception which occurred.
    pub fn all(&self) -> KafkaFuture<Vec<ClientMetricsResourceListing>> {
        self.future.clone()
    }
}

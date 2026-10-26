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

//! The entry point that creates an [`Admin`] client.

use crate::admin::{Admin, AdminClientConfig, KafkaAdminClient};
use crate::common::Error;

/// The base class for in-built admin clients.
///
/// Client code should use the newer `Admin` interface in preference to this
/// class.
///
/// # Translation notes
///
/// An uninhabited type: a namespace for [`AdminClient::create`], never a
/// value.
#[derive(Debug)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.AdminClient")]
pub enum AdminClient {}

impl AdminClient {
    /// Create a new Admin with the given configuration.
    ///
    /// # Arguments
    ///
    /// - `config` - The configuration.
    ///
    /// # Returns
    ///
    /// The new KafkaAdminClient.
    ///
    /// # Errors
    ///
    /// - `Error::is_kafka_error` (`Failed to create new KafkaAdminClient`, the
    ///   cause as its source) if the bootstrap addresses cannot be resolved, the
    ///   channel builder cannot be created, or `default.api.timeout.ms` is set
    ///   explicitly below `request.timeout.ms` (the source answering
    ///   `Error::is_config_error`, as Java's
    ///   `KafkaAdminClient.configureDefaultApiTimeoutMs` throws).
    ///
    /// # Panics
    ///
    /// If called outside a Tokio runtime, which runs the client's background
    /// task.
    ///
    /// # Translation notes
    ///
    /// Takes the parsed `AdminClientConfig`, not Java's map.
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClient#create")]
    pub fn create(config: AdminClientConfig) -> Result<Box<dyn Admin>, Error> {
        Ok(Box::new(KafkaAdminClient::new(config)?))
    }
}

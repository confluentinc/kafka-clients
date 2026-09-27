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
//!
//! Corresponds to `org.apache.kafka.clients.admin.AdminClient`.

use crate::admin::{Admin, AdminClientConfig, KafkaAdminClient};
use crate::common::Error;

/// The base class for in-built admin clients.
///
/// Java's `AdminClient` is an abstract class implementing `Admin` whose only
/// members are its static `create` factories. Rust has no abstract classes, so
/// it is an uninhabited type: a namespace for [`AdminClient::create`], never a
/// value. The client it creates is an `Admin` trait object.
#[derive(Debug)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.admin.AdminClient")]
pub enum AdminClient {}

impl AdminClient {
    /// Create a new Admin with the given configuration.
    ///
    /// Java's two overloads, `create(Properties)` and
    /// `create(Map<String, Object>)`, differ only in the container the
    /// configuration comes in; both parse it into an `AdminClientConfig`, which
    /// Rust takes directly, as `KafkaProducer::new` takes a `ProducerConfig`.
    ///
    /// ```no_run
    /// use std::collections::HashMap;
    ///
    /// use confluent_kafka::admin::{AdminClient, AdminClientConfig};
    ///
    /// let props = HashMap::from([("bootstrap.servers".to_string(), "localhost:9092".to_string())]);
    /// let config = AdminClientConfig::new(&props).expect("valid config");
    /// let admin = AdminClient::create(config).expect("admin client");
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if the bootstrap addresses cannot be resolved or the
    /// channel builder cannot be created.
    #[doc(alias = "org.apache.kafka.clients.admin.AdminClient#create")]
    pub fn create(config: AdminClientConfig) -> Result<Box<dyn Admin>, Error> {
        Ok(Box::new(KafkaAdminClient::new(config)?))
    }
}

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

//! A listing of a client metrics resource in the cluster.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ClientMetricsResourceListing`.

#![allow(deprecated)]

use std::fmt;

/// A listing of a client metrics resource in the cluster.
///
/// Corresponds to `org.apache.kafka.clients.admin.ClientMetricsResourceListing`
/// (deprecated since 4.1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[deprecated(since = "4.1.0", note = "Use Admin::list_config_resources instead")]
pub struct ClientMetricsResourceListing {
    name: String,
}

impl ClientMetricsResourceListing {
    /// Creates a new client metrics resource listing with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }

    /// The name of the client metrics resource.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl fmt::Display for ClientMetricsResourceListing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ClientMetricsResourceListing(name='{}')", self.name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accessor_returns_name() {
        let listing = ClientMetricsResourceListing::new("one");
        assert_eq!(listing.name(), "one");
    }

    #[test]
    fn equals_and_hash_by_name() {
        assert_eq!(
            ClientMetricsResourceListing::new("one"),
            ClientMetricsResourceListing::new("one")
        );
        assert_ne!(
            ClientMetricsResourceListing::new("one"),
            ClientMetricsResourceListing::new("two")
        );
    }

    #[test]
    fn display_matches_java_to_string() {
        assert_eq!(
            ClientMetricsResourceListing::new("one").to_string(),
            "ClientMetricsResourceListing(name='one')"
        );
    }
}

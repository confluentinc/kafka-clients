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

//! Options for `Admin::describe_features`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeFeaturesOptions`.

/// Options for `Admin::describe_features`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeFeaturesOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeFeaturesOptions {
    timeout_ms: Option<i32>,
    node_id: Option<i32>,
}

impl DescribeFeaturesOptions {
    /// Creates default options (default API timeout, arbitrary node).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout in milliseconds for this operation, or `None` to use the
    /// default API timeout for the `AdminClient`.
    #[must_use]
    pub fn timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout should be used.
    pub fn timeout(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Set the node id to which the request should be sent.
    ///
    /// Mirrors `DescribeFeaturesOptions.nodeId(int)`.
    #[must_use]
    pub fn node_id(mut self, node_id: i32) -> Self {
        self.node_id = Some(node_id);
        self
    }

    /// The node id to which the request should be sent. If empty, the request
    /// will be sent to an arbitrary controller/broker.
    ///
    /// Mirrors `DescribeFeaturesOptions.nodeId()`.
    pub fn get_node_id(&self) -> Option<i32> {
        self.node_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = DescribeFeaturesOptions::new();
        assert_eq!(options.timeout(), None);
        assert_eq!(options.get_node_id(), None);
    }

    #[test]
    fn fluent_setters() {
        let options = DescribeFeaturesOptions::new().timeout_ms(Some(100)).node_id(0);
        assert_eq!(options.timeout(), Some(100));
        assert_eq!(options.get_node_id(), Some(0));
    }
}

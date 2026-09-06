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

//! Options for `Admin::describe_producers`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeProducersOptions`.

/// Options for `Admin::describe_producers`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeProducersOptions`.
/// When [`broker_id`](Self::broker_id) is set, the request is sent directly to
/// that broker (via `StaticBrokerStrategy`) rather than looking up each
/// partition's leader.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeProducersOptions {
    timeout_ms: Option<i32>,
    broker_id: Option<i32>,
}

impl DescribeProducersOptions {
    /// Creates default options (default API timeout, no broker override).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout in milliseconds for this operation, or `None` to use the
    /// default API timeout for the `AdminClient`.
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout should be used.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Set the broker id to query for the topic partitions. Mirrors
    /// `DescribeProducersOptions.brokerId(int)`.
    #[must_use]
    pub fn broker_id(mut self, broker_id: i32) -> Self {
        self.broker_id = Some(broker_id);
        self
    }

    /// The broker id to query, if set. Mirrors `DescribeProducersOptions.brokerId()`.
    pub fn broker_id_opt(&self) -> Option<i32> {
        self.broker_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = DescribeProducersOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert_eq!(options.broker_id_opt(), None);
    }

    #[test]
    fn fluent_broker_id_and_timeout() {
        let options = DescribeProducersOptions::new().broker_id(3).set_timeout_ms(Some(5000));
        assert_eq!(options.broker_id_opt(), Some(3));
        assert_eq!(options.timeout_ms(), Some(5000));
    }
}

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

//! Options for `Admin::describe_cluster`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeClusterOptions`.

/// Options for `Admin::describe_cluster`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeClusterOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeClusterOptions {
    timeout_ms: Option<i32>,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
}

impl DescribeClusterOptions {
    /// Creates default options.
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

    /// Set whether to include cluster authorized operations.
    #[must_use]
    pub fn include_authorized_operations(mut self, include_authorized_operations: bool) -> Self {
        self.include_authorized_operations = include_authorized_operations;
        self
    }

    /// Whether to include cluster authorized operations.
    pub fn should_include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// Set whether to include fenced brokers when they are not fenced from the
    /// cluster (only supported by the broker endpoint at DescribeCluster v2+).
    #[must_use]
    pub fn include_fenced_brokers(mut self, include_fenced_brokers: bool) -> Self {
        self.include_fenced_brokers = include_fenced_brokers;
        self
    }

    /// Whether to include fenced brokers.
    pub fn should_include_fenced_brokers(&self) -> bool {
        self.include_fenced_brokers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = DescribeClusterOptions::new();
        assert_eq!(options.timeout(), None);
        assert!(!options.should_include_authorized_operations());
        assert!(!options.should_include_fenced_brokers());
    }

    #[test]
    fn fluent_setters() {
        let options = DescribeClusterOptions::new()
            .include_authorized_operations(true)
            .include_fenced_brokers(true)
            .timeout_ms(Some(1000));
        assert!(options.should_include_authorized_operations());
        assert!(options.should_include_fenced_brokers());
        assert_eq!(options.timeout(), Some(1000));
    }
}

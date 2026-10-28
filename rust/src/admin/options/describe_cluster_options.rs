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

/// Options for `Admin::describe_cluster`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions")]
pub struct DescribeClusterOptions {
    timeout_ms: Option<i32>,
    include_authorized_operations: bool,
    include_fenced_brokers: bool,
}

impl DescribeClusterOptions {
    /// Creates default options.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#DescribeClusterOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout in milliseconds for this operation, or `None` if the default
    /// API timeout for the `AdminClient` should be used.
    ///
    /// # Arguments
    ///
    /// - `timeout_ms` - the timeout in milliseconds, or `None` for the default API timeout.
    ///
    /// # Returns
    ///
    /// This `DescribeClusterOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Sets whether to include cluster authorized operations.
    ///
    /// # Arguments
    ///
    /// - `include_authorized_operations` - `true` to request the authorized operations.
    ///
    /// # Returns
    ///
    /// This `DescribeClusterOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#includeAuthorizedOperations")]
    pub fn set_include_authorized_operations(mut self, include_authorized_operations: bool) -> Self {
        self.include_authorized_operations = include_authorized_operations;
        self
    }

    /// Specify if authorized operations should be included in the response. Note that some
    /// older brokers cannot supply this information even if it is requested.
    ///
    /// # Returns
    ///
    /// `true` if authorized operations are requested.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#includeAuthorizedOperations")]
    pub fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// Sets whether to include fenced brokers in the response.
    ///
    /// # Arguments
    ///
    /// - `include_fenced_brokers` - `true` to request the fenced brokers.
    ///
    /// # Returns
    ///
    /// This `DescribeClusterOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#includeFencedBrokers")]
    pub fn set_include_fenced_brokers(mut self, include_fenced_brokers: bool) -> Self {
        self.include_fenced_brokers = include_fenced_brokers;
        self
    }

    /// Specify if fenced brokers should be included in the response. Note that some
    /// older brokers cannot supply this information even if it is requested.
    ///
    /// # Returns
    ///
    /// `true` if fenced brokers are requested.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClusterOptions#includeFencedBrokers")]
    pub fn include_fenced_brokers(&self) -> bool {
        self.include_fenced_brokers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = DescribeClusterOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(!options.include_authorized_operations());
        assert!(!options.include_fenced_brokers());
    }

    #[test]
    fn fluent_setters() {
        let options = DescribeClusterOptions::new()
            .set_include_authorized_operations(true)
            .set_include_fenced_brokers(true)
            .set_timeout_ms(Some(1000));
        assert!(options.include_authorized_operations());
        assert!(options.include_fenced_brokers());
        assert_eq!(options.timeout_ms(), Some(1000));
    }
}

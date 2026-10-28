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

//! Options for `Admin::describe_topics_with_topics`.

/// Default maximum number of partitions to be returned in a single response.
const DEFAULT_PARTITION_SIZE_LIMIT_PER_RESPONSE: i32 = 2000;

/// Options for `Admin::describe_topics_with_topics`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions")]
pub struct DescribeTopicsOptions {
    timeout_ms: Option<i32>,
    include_authorized_operations: bool,
    partition_size_limit_per_response: i32,
}

impl Default for DescribeTopicsOptions {
    fn default() -> Self {
        Self {
            timeout_ms: None,
            include_authorized_operations: false,
            partition_size_limit_per_response: DEFAULT_PARTITION_SIZE_LIMIT_PER_RESPONSE,
        }
    }
}

impl DescribeTopicsOptions {
    /// Creates default options.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#DescribeTopicsOptions")]
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
    /// This `DescribeTopicsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Sets whether to include authorized operations for the described topics.
    ///
    /// # Arguments
    ///
    /// - `include_authorized_operations` - `true` to request the authorized operations.
    ///
    /// # Returns
    ///
    /// This `DescribeTopicsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#includeAuthorizedOperations")]
    pub fn set_include_authorized_operations(mut self, include_authorized_operations: bool) -> Self {
        self.include_authorized_operations = include_authorized_operations;
        self
    }

    /// Returns whether authorized operations are requested for the described topics.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#includeAuthorizedOperations")]
    pub fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// Sets the maximum number of partitions to be returned in a single response.
    ///
    /// **This option:**
    /// - Is only effective when using topic names (not topic IDs).
    /// - Will not be effective if it is larger than the server-side configuration
    ///   `max.request.partition.size.limit`.
    ///
    /// # Arguments
    ///
    /// - `partition_size_limit_per_response` - the maximum number of partitions per response
    ///
    /// # Returns
    ///
    /// This `DescribeTopicsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#partitionSizeLimitPerResponse")]
    pub fn set_partition_size_limit_per_response(mut self, partition_size_limit_per_response: i32) -> Self {
        self.partition_size_limit_per_response = partition_size_limit_per_response;
        self
    }

    /// Returns the maximum number of partitions to be returned in a single response.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTopicsOptions#partitionSizeLimitPerResponse")]
    pub fn partition_size_limit_per_response(&self) -> i32 {
        self.partition_size_limit_per_response
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = DescribeTopicsOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(!options.include_authorized_operations());
        assert_eq!(options.partition_size_limit_per_response(), 2000);
    }

    #[test]
    fn fluent_setters() {
        let options = DescribeTopicsOptions::new()
            .set_include_authorized_operations(true)
            .set_partition_size_limit_per_response(50);
        assert!(options.include_authorized_operations());
        assert_eq!(options.partition_size_limit_per_response(), 50);
    }
}

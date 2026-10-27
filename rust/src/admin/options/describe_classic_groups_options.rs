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

//! Options for `Admin::describe_classic_groups`.

/// Options for `Admin::describe_classic_groups`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeClassicGroupsOptions")]
pub struct DescribeClassicGroupsOptions {
    timeout_ms: Option<i32>,
    include_authorized_operations: bool,
}

impl DescribeClassicGroupsOptions {
    /// Creates default options.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClassicGroupsOptions#DescribeClassicGroupsOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether to include authorized operations in the description.
    ///
    /// # Arguments
    ///
    /// - `include_authorized_operations` - `true` to request the authorized operations.
    ///
    /// # Returns
    ///
    /// This `DescribeClassicGroupsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClassicGroupsOptions#includeAuthorizedOperations")]
    pub fn set_include_authorized_operations(mut self, include_authorized_operations: bool) -> Self {
        self.include_authorized_operations = include_authorized_operations;
        self
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
    /// This `DescribeClassicGroupsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns whether authorized operations are requested.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeClassicGroupsOptions#includeAuthorizedOperations")]
    pub fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setter() {
        let options = DescribeClassicGroupsOptions::new();
        assert!(!options.include_authorized_operations());
        let options = options.set_include_authorized_operations(true);
        assert!(options.include_authorized_operations());
    }
}

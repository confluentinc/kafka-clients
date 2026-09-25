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

//! Options for `Admin::describe_consumer_groups`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions`.

/// Options for `Admin::describe_consumer_groups`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DescribeConsumerGroupsOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DescribeConsumerGroupsOptions {
    timeout_ms: Option<i32>,
    include_authorized_operations: bool,
}

impl DescribeConsumerGroupsOptions {
    /// Creates default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether to include authorized operations in the description. Mirrors
    /// `includeAuthorizedOperations`.
    #[must_use]
    pub fn set_include_authorized_operations(mut self, include_authorized_operations: bool) -> Self {
        self.include_authorized_operations = include_authorized_operations;
        self
    }

    /// Set the operation timeout in milliseconds (or `None` for the default).
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Whether authorized operations are requested. Mirrors
    /// `includeAuthorizedOperations()`.
    pub fn include_authorized_operations(&self) -> bool {
        self.include_authorized_operations
    }

    /// The operation timeout in milliseconds, or `None` for the default.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setter() {
        let options = DescribeConsumerGroupsOptions::new();
        assert!(!options.include_authorized_operations());
        let options = options.set_include_authorized_operations(true).set_timeout_ms(Some(100));
        assert!(options.include_authorized_operations());
        assert_eq!(options.timeout_ms(), Some(100));
    }
}

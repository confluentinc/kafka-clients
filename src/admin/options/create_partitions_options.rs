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

//! Options for `Admin::create_partitions`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.CreatePartitionsOptions`.

/// Options for `Admin::create_partitions`.
///
/// Corresponds to `org.apache.kafka.clients.admin.CreatePartitionsOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreatePartitionsOptions {
    timeout_ms: Option<i32>,
    validate_only: bool,
    retry_on_quota_violation: bool,
}

impl Default for CreatePartitionsOptions {
    fn default() -> Self {
        Self { timeout_ms: None, validate_only: false, retry_on_quota_violation: true }
    }
}

impl CreatePartitionsOptions {
    /// Creates default options (`validate_only=false`,
    /// `retry_on_quota_violation=true`, default API timeout).
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

    /// Set to true if the request should be validated without creating new
    /// partitions.
    #[must_use]
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Return true if the request should be validated without creating new
    /// partitions.
    pub fn should_validate_only(&self) -> bool {
        self.validate_only
    }

    /// Set to true if quota violation should be automatically retried.
    #[must_use]
    pub fn retry_on_quota_violation(mut self, retry_on_quota_violation: bool) -> Self {
        self.retry_on_quota_violation = retry_on_quota_violation;
        self
    }

    /// Returns true if quota violation should be automatically retried.
    pub fn should_retry_on_quota_violation(&self) -> bool {
        self.retry_on_quota_violation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = CreatePartitionsOptions::new();
        assert_eq!(options.timeout(), None);
        assert!(!options.should_validate_only());
        assert!(options.should_retry_on_quota_violation());
    }

    #[test]
    fn fluent_setters() {
        let options = CreatePartitionsOptions::new()
            .timeout_ms(Some(5000))
            .validate_only(true)
            .retry_on_quota_violation(false);
        assert_eq!(options.timeout(), Some(5000));
        assert!(options.should_validate_only());
        assert!(!options.should_retry_on_quota_violation());
    }
}

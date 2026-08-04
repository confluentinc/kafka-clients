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

//! Options for `Admin::delete_topics`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.DeleteTopicsOptions`.

/// Options for `Admin::delete_topics`.
///
/// Corresponds to `org.apache.kafka.clients.admin.DeleteTopicsOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteTopicsOptions {
    timeout_ms: Option<i32>,
    retry_on_quota_violation: bool,
}

impl Default for DeleteTopicsOptions {
    fn default() -> Self {
        Self { timeout_ms: None, retry_on_quota_violation: true }
    }
}

impl DeleteTopicsOptions {
    /// Creates default options (`retry_on_quota_violation=true`, default API
    /// timeout).
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
        let options = DeleteTopicsOptions::new();
        assert_eq!(options.timeout(), None);
        assert!(options.should_retry_on_quota_violation());
    }

    #[test]
    fn fluent_setters() {
        let options = DeleteTopicsOptions::new()
            .timeout_ms(Some(1000))
            .retry_on_quota_violation(false);
        assert_eq!(options.timeout(), Some(1000));
        assert!(!options.should_retry_on_quota_violation());
    }
}

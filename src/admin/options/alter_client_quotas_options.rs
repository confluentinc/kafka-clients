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

//! Options for `Admin::alter_client_quotas`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterClientQuotasOptions`.

/// Options for `Admin::alter_client_quotas`.
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterClientQuotasOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AlterClientQuotasOptions {
    timeout_ms: Option<i32>,
    validate_only: bool,
}

impl AlterClientQuotasOptions {
    /// Creates default options (default API timeout, not validate-only).
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

    /// Sets whether the request should be validated without altering the
    /// configs.
    ///
    /// Mirrors `AlterClientQuotasOptions.validateOnly(boolean)`.
    #[must_use]
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns whether the request should be validated without altering the
    /// configs.
    ///
    /// Mirrors `AlterClientQuotasOptions.validateOnly()`.
    pub fn is_validate_only(&self) -> bool {
        self.validate_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_and_setters() {
        let opts = AlterClientQuotasOptions::new();
        assert_eq!(opts.timeout(), None);
        assert!(!opts.is_validate_only());

        let opts = AlterClientQuotasOptions::new().timeout_ms(Some(5000)).validate_only(true);
        assert_eq!(opts.timeout(), Some(5000));
        assert!(opts.is_validate_only());
    }
}

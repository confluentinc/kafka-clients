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

//! Options for `Admin::incremental_alter_configs`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.AlterConfigsOptions`.

/// Options for `Admin::incremental_alter_configs`.
///
/// Corresponds to `org.apache.kafka.clients.admin.AlterConfigsOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AlterConfigsOptions {
    timeout_ms: Option<i32>,
    validate_only: bool,
}

impl AlterConfigsOptions {
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

    /// Set whether the request should be validated without altering the configs.
    #[must_use]
    pub fn validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Whether the request should be validated without altering the configs.
    pub fn should_validate_only(&self) -> bool {
        self.validate_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = AlterConfigsOptions::new();
        assert_eq!(options.timeout(), None);
        assert!(!options.should_validate_only());
    }

    #[test]
    fn fluent_setters() {
        let options = AlterConfigsOptions::new().validate_only(true);
        assert!(options.should_validate_only());
    }
}

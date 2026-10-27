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

/// Options for `Admin::incremental_alter_configs`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.AlterConfigsOptions")]
pub struct AlterConfigsOptions {
    timeout_ms: Option<i32>,
    validate_only: bool,
}

impl AlterConfigsOptions {
    /// Creates default options.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConfigsOptions#AlterConfigsOptions")]
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
    /// This `AlterConfigsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConfigsOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConfigsOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Set to true if the request should be validated without altering the configs.
    ///
    /// # Arguments
    ///
    /// - `validate_only` - `true` to only validate the request.
    ///
    /// # Returns
    ///
    /// This `AlterConfigsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConfigsOptions#validateOnly")]
    pub fn set_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns true if the request should be validated without altering the configs.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterConfigsOptions#shouldValidateOnly")]
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
        assert_eq!(options.timeout_ms(), None);
        assert!(!options.should_validate_only());
    }

    #[test]
    fn fluent_setters() {
        let options = AlterConfigsOptions::new().set_validate_only(true);
        assert!(options.should_validate_only());
    }
}

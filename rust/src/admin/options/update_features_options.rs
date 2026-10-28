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

//! Options for `Admin::update_features`.

/// Options for `Admin::update_features`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.UpdateFeaturesOptions")]
pub struct UpdateFeaturesOptions {
    timeout_ms: Option<i32>,
    validate_only: bool,
}

impl UpdateFeaturesOptions {
    /// Creates default options (default API timeout, `validate_only = false`).
    #[doc(alias = "org.apache.kafka.clients.admin.UpdateFeaturesOptions#UpdateFeaturesOptions")]
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
    /// This `UpdateFeaturesOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Set whether the request should only be validated, without performing the upgrade or
    /// downgrade.
    ///
    /// # Arguments
    ///
    /// - `validate_only` - `true` to only validate the request.
    ///
    /// # Returns
    ///
    /// This `UpdateFeaturesOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.UpdateFeaturesOptions#validateOnly")]
    pub fn set_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Returns whether the request should only be validated.
    #[doc(alias = "org.apache.kafka.clients.admin.UpdateFeaturesOptions#validateOnly")]
    pub fn validate_only(&self) -> bool {
        self.validate_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = UpdateFeaturesOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(!options.validate_only());
    }

    #[test]
    fn fluent_setters() {
        let options = UpdateFeaturesOptions::new().set_timeout_ms(Some(100)).set_validate_only(true);
        assert_eq!(options.timeout_ms(), Some(100));
        assert!(options.validate_only());
    }
}

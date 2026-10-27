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

//! Options for `Admin::describe_configs`.

/// Options for `Admin::describe_configs`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions")]
pub struct DescribeConfigsOptions {
    timeout_ms: Option<i32>,
    include_synonyms: bool,
    include_documentation: bool,
}

impl DescribeConfigsOptions {
    /// Creates default options.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#DescribeConfigsOptions")]
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
    /// This `DescribeConfigsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }

    /// Set to true if synonym configs should be returned in the response.
    ///
    /// # Arguments
    ///
    /// - `include_synonyms` - `true` to return synonym configs.
    ///
    /// # Returns
    ///
    /// This `DescribeConfigsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#includeSynonyms")]
    pub fn set_include_synonyms(mut self, include_synonyms: bool) -> Self {
        self.include_synonyms = include_synonyms;
        self
    }

    /// Returns true if synonym configs should be returned in the response.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#includeSynonyms")]
    pub fn include_synonyms(&self) -> bool {
        self.include_synonyms
    }

    /// Set to true if config documentation should be returned in the response.
    ///
    /// # Arguments
    ///
    /// - `include_documentation` - `true` to return config documentation.
    ///
    /// # Returns
    ///
    /// This `DescribeConfigsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#includeDocumentation")]
    pub fn set_include_documentation(mut self, include_documentation: bool) -> Self {
        self.include_documentation = include_documentation;
        self
    }

    /// Returns true if config documentation should be returned in the response.
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeConfigsOptions#includeDocumentation")]
    pub fn include_documentation(&self) -> bool {
        self.include_documentation
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = DescribeConfigsOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(!options.include_synonyms());
        assert!(!options.include_documentation());
    }

    #[test]
    fn fluent_setters() {
        let options = DescribeConfigsOptions::new()
            .set_include_synonyms(true)
            .set_include_documentation(true);
        assert!(options.include_synonyms());
        assert!(options.include_documentation());
    }
}

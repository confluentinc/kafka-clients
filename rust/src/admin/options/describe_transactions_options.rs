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

//! Options for `Admin::describe_transactions`.

/// Options for `Admin::describe_transactions`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.DescribeTransactionsOptions")]
pub struct DescribeTransactionsOptions {
    timeout_ms: Option<i32>,
}

impl DescribeTransactionsOptions {
    /// Creates default options (default API timeout).
    #[doc(alias = "org.apache.kafka.clients.admin.DescribeTransactionsOptions#DescribeTransactionsOptions")]
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
    /// This `DescribeTransactionsOptions` instance.
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
}

impl std::fmt::Display for DescribeTransactionsOptions {
    /// Java's `toString()`: an unset timeout prints as `null`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timeout_ms = self.timeout_ms.map_or_else(|| "null".to_string(), |t| t.to_string());
        write!(f, "DescribeTransactionsOptions(timeoutMs={timeout_ms})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_follows_java() {
        assert_eq!(
            DescribeTransactionsOptions::new().to_string(),
            "DescribeTransactionsOptions(timeoutMs=null)"
        );
        assert_eq!(
            DescribeTransactionsOptions::new().set_timeout_ms(Some(500)).to_string(),
            "DescribeTransactionsOptions(timeoutMs=500)"
        );
    }

    #[test]
    fn defaults_match_java() {
        assert_eq!(DescribeTransactionsOptions::new().timeout_ms(), None);
    }
}

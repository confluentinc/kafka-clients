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

//! Options for `Admin::abort_transaction`.

/// Options for `Admin::abort_transaction`.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionOptions")]
pub struct AbortTransactionOptions {
    timeout_ms: Option<i32>,
}

impl AbortTransactionOptions {
    /// Creates default options (default API timeout).
    #[doc(alias = "org.apache.kafka.clients.admin.AbortTransactionOptions#AbortTransactionOptions")]
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
    /// This `AbortTransactionOptions` instance.
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

impl std::fmt::Display for AbortTransactionOptions {
    /// Java's `toString()`: an unset timeout prints as `null`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let timeout_ms = self.timeout_ms.map_or_else(|| "null".to_string(), |t| t.to_string());
        write!(f, "AbortTransactionOptions(timeoutMs={timeout_ms})")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_string_follows_java() {
        assert_eq!(
            AbortTransactionOptions::new().to_string(),
            "AbortTransactionOptions(timeoutMs=null)"
        );
        assert_eq!(
            AbortTransactionOptions::new().set_timeout_ms(Some(500)).to_string(),
            "AbortTransactionOptions(timeoutMs=500)"
        );
    }

    #[test]
    fn defaults_match_java() {
        assert_eq!(AbortTransactionOptions::new().timeout_ms(), None);
    }
}

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

//! Options for `Admin::force_terminate_transaction`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.TerminateTransactionOptions`.

/// Options for `Admin::force_terminate_transaction`.
///
/// Corresponds to `org.apache.kafka.clients.admin.TerminateTransactionOptions`.
/// In Java this class only extends `AbstractOptions` (a `timeout_ms` field)
/// with no additional fields.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TerminateTransactionOptions {
    timeout_ms: Option<i32>,
}

impl TerminateTransactionOptions {
    /// Creates default options (default API timeout).
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timeout in milliseconds for this operation, or `None` to use the
    /// default API timeout for the `AdminClient`.
    #[must_use]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The timeout in milliseconds for this operation, or `None` if the default
    /// API timeout should be used.
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        assert_eq!(TerminateTransactionOptions::new().timeout_ms(), None);
    }
}

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

//! Options for `Admin::list_offsets`.
//!
//! Corresponds to `org.apache.kafka.clients.admin.ListOffsetsOptions`.

use crate::common::IsolationLevel;

/// Options for `Admin::list_offsets`.
///
/// Corresponds to `org.apache.kafka.clients.admin.ListOffsetsOptions`, which
/// extends `AbstractOptions` (a `timeout_ms` field) and adds an isolation
/// level (defaulting to `READ_UNCOMMITTED`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListOffsetsOptions {
    timeout_ms: Option<i32>,
    isolation_level: IsolationLevel,
}

impl Default for ListOffsetsOptions {
    fn default() -> Self {
        // Mirrors Java's no-arg constructor `this(IsolationLevel.READ_UNCOMMITTED)`.
        Self { timeout_ms: None, isolation_level: IsolationLevel::ReadUncommitted }
    }
}

impl ListOffsetsOptions {
    /// Creates default options (default API timeout, `READ_UNCOMMITTED`).
    ///
    /// Mirrors `new ListOffsetsOptions()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options with the given isolation level.
    ///
    /// Mirrors `new ListOffsetsOptions(IsolationLevel)`.
    pub fn with_isolation_level(isolation_level: IsolationLevel) -> Self {
        Self { timeout_ms: None, isolation_level }
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

    /// The isolation level for this operation.
    ///
    /// Mirrors `isolationLevel()`.
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = ListOffsetsOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert_eq!(options.isolation_level(), IsolationLevel::ReadUncommitted);
    }

    #[test]
    fn new_isolation_level_and_timeout() {
        let options = ListOffsetsOptions::with_isolation_level(IsolationLevel::ReadCommitted).set_timeout_ms(Some(200));
        assert_eq!(options.isolation_level(), IsolationLevel::ReadCommitted);
        assert_eq!(options.timeout_ms(), Some(200));
    }
}

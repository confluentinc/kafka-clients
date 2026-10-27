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

use crate::common::IsolationLevel;

/// Options for `Admin::list_offsets`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.ListOffsetsOptions")]
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
    #[doc(alias = "org.apache.kafka.clients.admin.ListOffsetsOptions#ListOffsetsOptions")]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options with the given isolation level.
    ///
    /// # Arguments
    ///
    /// - `isolation_level` - the isolation level of the offsets to list.
    #[doc(alias = "org.apache.kafka.clients.admin.ListOffsetsOptions#ListOffsetsOptions")]
    pub fn with_isolation_level(isolation_level: IsolationLevel) -> Self {
        Self { timeout_ms: None, isolation_level }
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
    /// This `ListOffsetsOptions` instance.
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

    /// Returns the isolation level for this operation.
    #[doc(alias = "org.apache.kafka.clients.admin.ListOffsetsOptions#isolationLevel")]
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

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

//! Options for `Admin::alter_partition_reassignments`.
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions`.

/// Options for `Admin::alter_partition_reassignments`.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AlterPartitionReassignmentsOptions {
    timeout_ms: Option<i32>,
    allow_replication_factor_change: bool,
}

impl Default for AlterPartitionReassignmentsOptions {
    fn default() -> Self {
        // Mirrors Java's field initializer `allowReplicationFactorChange = true`.
        Self { timeout_ms: None, allow_replication_factor_change: true }
    }
}

impl AlterPartitionReassignmentsOptions {
    /// Creates default options (default API timeout, replication-factor change
    /// allowed).
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

    /// Set the option indicating if the alter-partition-reassignments call
    /// should be allowed to alter the replication factor of a partition.
    ///
    /// Mirrors `allowReplicationFactorChange(boolean)`.
    #[must_use]
    pub fn allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }

    /// A boolean indicating if the alter-partition-reassignments should be
    /// allowed to alter the replication factor of a partition.
    ///
    /// Mirrors `allowReplicationFactorChange()`.
    pub fn should_allow_replication_factor_change(&self) -> bool {
        self.allow_replication_factor_change
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_java() {
        let options = AlterPartitionReassignmentsOptions::new();
        assert_eq!(options.timeout_ms(), None);
        assert!(options.should_allow_replication_factor_change());
    }

    #[test]
    fn fluent_setters() {
        let options = AlterPartitionReassignmentsOptions::new()
            .set_timeout_ms(Some(5000))
            .allow_replication_factor_change(false);
        assert_eq!(options.timeout_ms(), Some(5000));
        assert!(!options.should_allow_replication_factor_change());
    }
}

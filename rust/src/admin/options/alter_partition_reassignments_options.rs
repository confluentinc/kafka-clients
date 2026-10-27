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

/// Options for `Admin::alter_partition_reassignments`.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions")]
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
    #[doc(
        alias = "org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions#AlterPartitionReassignmentsOptions"
    )]
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
    /// This `AlterPartitionReassignmentsOptions` instance.
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

    /// Set the option indicating if the alter partition reassignments call should be
    /// allowed to alter the replication factor of a partition.
    /// In cases where it is not allowed, any replication factor change will result in an error
    /// returned by the API.
    ///
    /// # Arguments
    ///
    /// - `allow` - whether the replication factor of a partition may change.
    ///
    /// # Returns
    ///
    /// This `AlterPartitionReassignmentsOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions#allowReplicationFactorChange")]
    pub fn set_allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }

    /// Returns a boolean indicating if the alter partition reassignments should be
    /// allowed to alter the replication factor of a partition.
    /// In cases where it is not allowed, any replication factor change will result in an error
    /// returned by the API.
    #[doc(alias = "org.apache.kafka.clients.admin.AlterPartitionReassignmentsOptions#allowReplicationFactorChange")]
    pub fn allow_replication_factor_change(&self) -> bool {
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
        assert!(options.allow_replication_factor_change());
    }

    #[test]
    fn fluent_setters() {
        let options = AlterPartitionReassignmentsOptions::new()
            .set_timeout_ms(Some(5000))
            .set_allow_replication_factor_change(false);
        assert_eq!(options.timeout_ms(), Some(5000));
        assert!(!options.allow_replication_factor_change());
    }
}

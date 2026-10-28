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

//! Options for closing a consumer.
//!
//! Translated from `org.apache.kafka.clients.consumer.CloseOptions`.

use std::time::Duration;

/// Enum to specify the group membership operation upon leaving a group.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
#[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions$GroupMembershipOperation")]
pub enum GroupMembershipOperation {
    /// The consumer will leave the group. This is the default for dynamic
    /// members, and can be used by static members when they want to
    /// permanently leave the group and trigger a rebalance.
    LeaveGroup,
    /// The consumer will remain in the group. This is the default for static
    /// members, allowing them to rejoin quickly without triggering a
    /// rebalance. When used by dynamic members, no leave heartbeat will be
    /// sent and the member will be removed by the coordinator after the
    /// session timeout expires.
    RemainInGroup,
    /// Applies the default behavior:
    /// - For **static members**: The consumer will remain in the group.
    /// - For **dynamic members**: The consumer will leave the group.
    Default,
}

/// Java's inherited `Enum.toString()`: the constant's name.
impl std::fmt::Display for GroupMembershipOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::LeaveGroup => "LEAVE_GROUP",
            Self::RemainInGroup => "REMAIN_IN_GROUP",
            Self::Default => "DEFAULT",
        })
    }
}

/// Options for controlling the consumer close behavior.
///
/// This class allows customization of the close timeout and group membership
/// operation when a consumer is being shut down.
#[derive(Clone, Debug)]
#[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions")]
pub struct CloseOptions {
    operation: GroupMembershipOperation,
    timeout: Option<Duration>,
}

impl CloseOptions {
    /// Create a new `CloseOptions` with default values (Java's private
    /// no-arg constructor).
    fn empty() -> Self {
        Self { operation: GroupMembershipOperation::Default, timeout: None }
    }

    /// Static method to create a `CloseOptions` with a custom timeout.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time to wait for the consumer to close.
    ///
    /// # Returns
    ///
    /// A new `CloseOptions` instance with the specified timeout.
    ///
    /// # Translation notes
    ///
    /// `timeout` cannot be `None`: for the default timeout use
    /// [`CloseOptions::default`].
    #[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions#timeout")]
    pub fn new_with_timeout(timeout: Duration) -> Self {
        Self::empty().with_timeout(timeout)
    }

    /// Static method to create a `CloseOptions` with a specified group
    /// membership operation.
    ///
    /// # Arguments
    ///
    /// - `operation` - The group membership operation to apply. Must be one of `LEAVE_GROUP`,
    ///   `REMAIN_IN_GROUP`, or `DEFAULT`.
    ///
    /// # Returns
    ///
    /// A new `CloseOptions` instance with the specified group membership operation.
    #[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions#groupMembershipOperation")]
    pub fn with_operation(operation: GroupMembershipOperation) -> Self {
        Self::empty().with_group_membership_operation(operation)
    }

    /// Fluent method to set the timeout for the close process.
    ///
    /// # Arguments
    ///
    /// - `timeout` - The maximum time to wait for the consumer to close.
    ///
    /// # Returns
    ///
    /// This `CloseOptions` instance.
    ///
    /// # Translation notes
    ///
    /// `timeout` cannot be `None`: to keep the default timeout, do not call
    /// this method.
    #[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions#withTimeout")]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Fluent method to set the group membership operation upon shutdown.
    ///
    /// # Arguments
    ///
    /// - `operation` - The group membership operation to apply. Must be one of `LEAVE_GROUP`,
    ///   `REMAIN_IN_GROUP`, or `DEFAULT`.
    ///
    /// # Returns
    ///
    /// This `CloseOptions` instance.
    #[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions#withGroupMembershipOperation")]
    pub fn with_group_membership_operation(mut self, operation: GroupMembershipOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Returns the group membership operation configured for this close.
    ///
    /// # Returns
    ///
    /// The group membership operation
    #[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions#groupMembershipOperation")]
    pub fn group_membership_operation(&self) -> GroupMembershipOperation {
        self.operation
    }

    /// Returns the timeout configured for this close.
    ///
    /// # Returns
    ///
    /// The timeout, or `None` if using the default timeout
    #[doc(alias = "org.apache.kafka.clients.consumer.CloseOptions#timeout")]
    pub fn timeout(&self) -> Option<Duration> {
        self.timeout
    }
}

impl Default for CloseOptions {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let opts = CloseOptions::default();
        assert_eq!(opts.group_membership_operation(), GroupMembershipOperation::Default);
        assert_eq!(opts.timeout(), None);
    }

    #[test]
    fn test_timeout_constructor() {
        let opts = CloseOptions::new_with_timeout(Duration::from_secs(1));
        assert_eq!(opts.timeout(), Some(Duration::from_secs(1)));
        assert_eq!(opts.group_membership_operation(), GroupMembershipOperation::Default);
    }

    #[test]
    fn test_group_membership_operation_constructor() {
        let opts = CloseOptions::with_operation(GroupMembershipOperation::LeaveGroup);
        assert_eq!(opts.group_membership_operation(), GroupMembershipOperation::LeaveGroup);
        assert_eq!(opts.timeout(), None);
    }
}

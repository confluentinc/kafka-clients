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

/// Default timeout (milliseconds) applied when [`CloseOptions::timeout`] is
/// not set. In Java this constant lives in `ConsumerUtils`; we expose it here
/// for now and a later phase will move it to its canonical location and
/// re-export from there.
///
/// NOTE: Phase 6 will move this constant to a `ConsumerUtils` translation
/// and re-export it here for backwards compatibility.
pub const DEFAULT_CLOSE_TIMEOUT_MS: u64 = 30_000;

/// The group membership operation to apply when the consumer closes.
///
/// Corresponds to Java's `CloseOptions.GroupMembershipOperation`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum GroupMembershipOperation {
    /// The consumer will leave the group.
    LeaveGroup,
    /// The consumer will remain in the group.
    RemainInGroup,
    /// Apply the default behavior:
    /// - static members remain in the group;
    /// - dynamic members leave the group.
    Default,
}

/// Options for closing a consumer.
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.CloseOptions`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloseOptions {
    operation: GroupMembershipOperation,
    timeout: Option<Duration>,
}

impl CloseOptions {
    /// Create a new `CloseOptions` with default values.
    ///
    /// Equivalent to Java's private no-arg constructor.
    fn empty() -> Self {
        Self { operation: GroupMembershipOperation::Default, timeout: None }
    }

    /// Static method to create a `CloseOptions` with a custom timeout.
    ///
    /// Corresponds to Java's static `CloseOptions.timeout(Duration)`. Java
    /// accepts a nullable `Duration` here and internally wraps it via
    /// `Optional.ofNullable`; Rust's type system makes `Duration` non-null,
    /// so for "no timeout, use default" callers should use
    /// [`CloseOptions::default`] (or omit the call to `timeout`).
    pub fn timeout(timeout: Duration) -> Self {
        Self::empty().with_timeout(timeout)
    }

    /// Static method to create a `CloseOptions` with a specified group
    /// membership operation.
    ///
    /// Corresponds to Java's static
    /// `CloseOptions.groupMembershipOperation(GroupMembershipOperation)`.
    pub fn group_membership_operation(operation: GroupMembershipOperation) -> Self {
        Self::empty().with_group_membership_operation(operation)
    }

    /// Fluent setter for the close timeout.
    ///
    /// Corresponds to Java's `CloseOptions.withTimeout(Duration)` which
    /// accepts a nullable `Duration`. In Rust, `Duration` is non-null; the
    /// internal `Option<Duration>` field always becomes `Some(timeout)`
    /// after this call. Callers wanting "no timeout, use default" should
    /// leave the field at its default by skipping this setter (or via
    /// [`CloseOptions::default`]).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Fluent setter for the group membership operation upon shutdown.
    ///
    /// In Java, this method rejects a `null` operation via
    /// `Objects.requireNonNull`. In Rust the type system makes the
    /// non-null guarantee redundant.
    pub fn with_group_membership_operation(mut self, operation: GroupMembershipOperation) -> Self {
        self.operation = operation;
        self
    }

    /// The group membership operation to apply upon shutdown.
    pub fn group_membership_operation_value(&self) -> GroupMembershipOperation {
        self.operation
    }

    /// The maximum time to wait for the close process to complete.
    ///
    /// `None` means the default timeout will be used.
    pub fn timeout_value(&self) -> Option<Duration> {
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
        assert_eq!(opts.group_membership_operation_value(), GroupMembershipOperation::Default);
        assert_eq!(opts.timeout_value(), None);
    }

    #[test]
    fn test_timeout_constructor() {
        let opts = CloseOptions::timeout(Duration::from_secs(1));
        assert_eq!(opts.timeout_value(), Some(Duration::from_secs(1)));
        assert_eq!(opts.group_membership_operation_value(), GroupMembershipOperation::Default);
    }

    #[test]
    fn test_group_membership_operation_constructor() {
        let opts = CloseOptions::group_membership_operation(GroupMembershipOperation::LeaveGroup);
        assert_eq!(opts.group_membership_operation_value(), GroupMembershipOperation::LeaveGroup);
        assert_eq!(opts.timeout_value(), None);
    }
}

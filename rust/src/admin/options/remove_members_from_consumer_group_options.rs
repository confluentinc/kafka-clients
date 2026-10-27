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

//! Options for `Admin::remove_members_from_consumer_group`.

use std::collections::HashSet;

use crate::admin::MemberToRemove;
use crate::common::Error;

/// Options for `Admin::remove_members_from_consumer_group`. Carries the members
/// to be removed from the consumer group.
#[derive(Clone, Debug, Default)]
#[doc(alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions")]
pub struct RemoveMembersFromConsumerGroupOptions {
    members: HashSet<MemberToRemove>,
    reason: Option<String>,
    timeout_ms: Option<i32>,
}

impl RemoveMembersFromConsumerGroupOptions {
    /// Creates options that remove every member of the group: `remove_all()` returns `true`.
    #[doc(
        alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions#RemoveMembersFromConsumerGroupOptions"
    )]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates options for removing the given members.
    ///
    /// # Arguments
    ///
    /// - `members` - the members to remove from the group; must not be empty.
    ///
    /// # Errors
    ///
    /// - `Error::is_local_illegal_argument_error` if `members` is empty ("Invalid empty members
    ///   has been provided"). Use [`Self::new`] to remove all members instead.
    #[doc(
        alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions#RemoveMembersFromConsumerGroupOptions"
    )]
    pub fn with_members(members: impl IntoIterator<Item = MemberToRemove>) -> Result<Self, Error> {
        let members: HashSet<MemberToRemove> = members.into_iter().collect();
        if members.is_empty() {
            return Err(Error::local_illegal_argument("Invalid empty members has been provided"));
        }
        Ok(Self { members, reason: None, timeout_ms: None })
    }

    /// Sets an optional reason.
    ///
    /// # Arguments
    ///
    /// - `reason` - the reason sent with the request, or `None` to clear a
    ///   reason set earlier.
    #[doc(alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions#reason")]
    pub fn set_reason(&mut self, reason: Option<String>) {
        self.reason = reason;
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
    /// This `RemoveMembersFromConsumerGroupOptions` instance.
    #[must_use]
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn set_timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Returns the members to remove, empty when all members are removed.
    #[doc(alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions#members")]
    pub fn members(&self) -> &HashSet<MemberToRemove> {
        &self.members
    }

    /// Returns the optional reason.
    #[doc(alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions#reason")]
    pub fn reason(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Returns whether all members should be removed, as no specific members were provided.
    #[doc(alias = "org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions#removeAll")]
    pub fn remove_all(&self) -> bool {
        self.members.is_empty()
    }

    /// Returns the timeout in milliseconds for this operation, or `None` if the
    /// default API timeout for the `AdminClient` should be used.
    #[doc(alias = "org.apache.kafka.clients.admin.AbstractOptions#timeoutMs")]
    pub fn timeout_ms(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `RemoveMembersFromConsumerGroupOptionsTest.testConstructor`.
    #[test]
    fn test_constructor() {
        let options = RemoveMembersFromConsumerGroupOptions::with_members([MemberToRemove::new("instance-1")]).unwrap();
        assert_eq!(options.members(), &HashSet::from([MemberToRemove::new("instance-1")]));
        assert!(!options.remove_all());

        // Construct will fail if illegal empty members provided.
        assert!(matches!(
            RemoveMembersFromConsumerGroupOptions::with_members([]),
            Err(Error::LocalIllegalArgument(_))
        ));
    }

    #[test]
    fn default_is_remove_all() {
        let options = RemoveMembersFromConsumerGroupOptions::new();
        assert!(options.remove_all());
        assert_eq!(options.reason(), None);
        assert_eq!(options.timeout_ms(), None);
    }

    #[test]
    fn reason_and_timeout_setters() {
        let mut options = RemoveMembersFromConsumerGroupOptions::with_members([MemberToRemove::new("i")]).unwrap();
        options.set_reason(Some("because".to_string()));
        let options = options.set_timeout_ms(Some(50));
        assert_eq!(options.reason(), Some("because"));
        assert_eq!(options.timeout_ms(), Some(50));
    }

    /// New test, no Java original: Java's `reason(null)` stores `null`, so it
    /// clears a reason set earlier (`RemoveMembersFromConsumerGroupOptions#reason`).
    #[test]
    fn set_reason_none_clears_the_reason() {
        let mut options = RemoveMembersFromConsumerGroupOptions::new();
        options.set_reason(Some("because".to_string()));
        options.set_reason(None);
        assert_eq!(options.reason(), None);
    }
}

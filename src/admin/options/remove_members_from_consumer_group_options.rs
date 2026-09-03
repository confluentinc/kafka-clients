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
//!
//! Corresponds to
//! `org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions`.

use std::collections::HashSet;

use crate::admin::MemberToRemove;
use crate::common::Error;

/// Options for `Admin::remove_members_from_consumer_group`. Carries the members
/// to be removed from the consumer group.
///
/// Corresponds to
/// `org.apache.kafka.clients.admin.RemoveMembersFromConsumerGroupOptions`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RemoveMembersFromConsumerGroupOptions {
    members: HashSet<MemberToRemove>,
    reason: Option<String>,
    timeout_ms: Option<i32>,
}

impl RemoveMembersFromConsumerGroupOptions {
    /// Creates options for removing the given members.
    ///
    /// Mirrors Java's `RemoveMembersFromConsumerGroupOptions(Collection<MemberToRemove>)`.
    ///
    /// # Errors
    ///
    /// Returns an error (Java's `IllegalArgumentException`) if `members` is
    /// empty. Use [`Default::default`] to remove all members instead.
    pub fn new(members: impl IntoIterator<Item = MemberToRemove>) -> Result<Self, Error> {
        let members: HashSet<MemberToRemove> = members.into_iter().collect();
        if members.is_empty() {
            return Err(Error::local_illegal_argument("Invalid empty members has been provided"));
        }
        Ok(Self { members, reason: None, timeout_ms: None })
    }

    /// Sets an optional reason.
    ///
    /// Mirrors Java's `reason(String)`.
    pub fn reason(&mut self, reason: impl Into<String>) {
        self.reason = Some(reason.into());
    }

    /// Set the operation timeout in milliseconds (or `None` for the default).
    #[must_use]
    pub fn timeout_ms(mut self, timeout_ms: Option<i32>) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// The members to remove.
    ///
    /// Mirrors Java's `members()`.
    pub fn members(&self) -> &HashSet<MemberToRemove> {
        &self.members
    }

    /// The optional reason.
    ///
    /// Mirrors Java's `reason()`.
    pub fn reason_value(&self) -> Option<&str> {
        self.reason.as_deref()
    }

    /// Whether all members should be removed (i.e. no specific members were
    /// provided).
    ///
    /// Mirrors Java's `removeAll()`.
    pub fn remove_all(&self) -> bool {
        self.members.is_empty()
    }

    /// The operation timeout in milliseconds, or `None` for the default.
    pub fn timeout(&self) -> Option<i32> {
        self.timeout_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Translated from `RemoveMembersFromConsumerGroupOptionsTest.testConstructor`.
    #[test]
    fn test_constructor() {
        let options = RemoveMembersFromConsumerGroupOptions::new([MemberToRemove::new("instance-1")]).unwrap();
        assert_eq!(options.members(), &HashSet::from([MemberToRemove::new("instance-1")]));
        assert!(!options.remove_all());

        // Construct will fail if illegal empty members provided.
        assert!(matches!(
            RemoveMembersFromConsumerGroupOptions::new([]),
            Err(Error::LocalIllegalArgument(_))
        ));
    }

    #[test]
    fn default_is_remove_all() {
        let options = RemoveMembersFromConsumerGroupOptions::default();
        assert!(options.remove_all());
        assert_eq!(options.reason_value(), None);
        assert_eq!(options.timeout(), None);
    }

    #[test]
    fn reason_and_timeout_setters() {
        let mut options = RemoveMembersFromConsumerGroupOptions::new([MemberToRemove::new("i")]).unwrap();
        options.reason("because");
        let options = options.timeout_ms(Some(50));
        assert_eq!(options.reason_value(), Some("because"));
        assert_eq!(options.timeout(), Some(50));
    }
}

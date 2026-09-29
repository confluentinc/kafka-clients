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

//! The key used by [`CoordinatorStrategy`](super::CoordinatorStrategy)
//! to identify a group or transactional coordinator lookup.
//!
//! Corresponds to `org.apache.kafka.clients.admin.internals.CoordinatorKey`.

use std::fmt;

use crate::common::requests::CoordinatorType;

/// A coordinator lookup key: an id value (group id or transactional id) plus the
/// coordinator type it identifies.
///
/// Corresponds to `CoordinatorKey`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct CoordinatorKey {
    /// The id value (group id or transactional id).
    pub(crate) id_value: String,
    /// The coordinator type this key identifies.
    pub(crate) coordinator_type: CoordinatorType,
}

impl CoordinatorKey {
    /// Creates a coordinator key for a consumer group id.
    ///
    /// Mirrors `CoordinatorKey.byGroupId`.
    pub(crate) fn by_group_id(group_id: impl Into<String>) -> Self {
        Self { id_value: group_id.into(), coordinator_type: CoordinatorType::Group }
    }

    /// Creates a coordinator key for a transactional id.
    ///
    /// Mirrors `CoordinatorKey.byTransactionalId`.
    pub(crate) fn by_transactional_id(transactional_id: impl Into<String>) -> Self {
        Self {
            id_value: transactional_id.into(),
            coordinator_type: CoordinatorType::Transaction,
        }
    }
}

impl fmt::Display for CoordinatorKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Mirrors Java's `CoordinatorKey.toString`; the coordinator type prints
        // as its uppercase Java enum name (GROUP / TRANSACTION / SHARE).
        let type_name = match self.coordinator_type {
            CoordinatorType::Group => "GROUP",
            CoordinatorType::Transaction => "TRANSACTION",
            CoordinatorType::Share => "SHARE",
        };
        write!(f, "CoordinatorKey(idValue='{}', type={})", self.id_value, type_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn by_group_id_sets_group_type() {
        let key = CoordinatorKey::by_group_id("g1");
        assert_eq!(key.id_value, "g1");
        assert_eq!(key.coordinator_type, CoordinatorType::Group);
    }

    #[test]
    fn by_transactional_id_sets_transaction_type() {
        let key = CoordinatorKey::by_transactional_id("t1");
        assert_eq!(key.id_value, "t1");
        assert_eq!(key.coordinator_type, CoordinatorType::Transaction);
    }

    #[test]
    fn equality_uses_both_id_and_type() {
        assert_eq!(CoordinatorKey::by_group_id("x"), CoordinatorKey::by_group_id("x"));
        assert_ne!(CoordinatorKey::by_group_id("x"), CoordinatorKey::by_transactional_id("x"));
    }

    #[test]
    fn display_matches_java() {
        assert_eq!(
            CoordinatorKey::by_group_id("foo").to_string(),
            "CoordinatorKey(idValue='foo', type=GROUP)"
        );
    }
}

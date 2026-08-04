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

//! Client quota entity type.
//!
//! Corresponds to `org.apache.kafka.common.quota.ClientQuotaEntity`.

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// The entity type value for a user.
pub const USER: &str = "user";
/// The entity type value for a client id.
pub const CLIENT_ID: &str = "client-id";
/// The entity type value for an IP address.
pub const IP: &str = "ip";

/// Describes a client quota entity, which is a mapping of entity types to their
/// names.
///
/// Corresponds to `org.apache.kafka.common.quota.ClientQuotaEntity`.
///
/// Java models the mapping as a `Map<String, String>` whose *values may be
/// null*: "If a name is null, then it is mapped to the built-in default entity
/// name" (e.g. `--entity-type users --entity-default`). Rust cannot store a
/// null `String`, so the faithful representation of Java's nullable map value
/// is `Option<String>`: `None` is the built-in default entity (wire-null entity
/// name), and `Some(name)` is a concretely-named entity (`Some(String::new())`
/// is the entity literally named `""`, which is distinct from the default).
#[derive(Clone, Debug, Eq)]
pub struct ClientQuotaEntity {
    entries: HashMap<String, Option<String>>,
}

impl ClientQuotaEntity {
    /// Returns whether the given entity type is one of the built-in types.
    ///
    /// Mirrors `ClientQuotaEntity.isValidEntityType`.
    pub fn is_valid_entity_type(entity_type: &str) -> bool {
        entity_type == USER || entity_type == CLIENT_ID || entity_type == IP
    }

    /// Constructs a quota entity for the given types and names. If a name is
    /// `None` (Java `null`), then it is mapped to the built-in default entity
    /// name.
    ///
    /// Mirrors `ClientQuotaEntity(Map<String, String>)`.
    pub fn new(entries: HashMap<String, Option<String>>) -> Self {
        Self { entries }
    }

    /// Returns the map of entity type to its name. A `None` value denotes the
    /// built-in default entity (Java's `null` name).
    ///
    /// Mirrors `ClientQuotaEntity.entries()`.
    pub fn entries(&self) -> &HashMap<String, Option<String>> {
        &self.entries
    }
}

impl PartialEq for ClientQuotaEntity {
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

// `HashMap` does not implement `Hash`, so we implement it manually with an
// order-independent accumulation, matching Java's `Objects.hash(entries)`
// where `Map.hashCode()` is the order-independent sum of its entry hashes.
impl Hash for ClientQuotaEntity {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut acc: u64 = 0;
        for (k, v) in &self.entries {
            let mut entry_hasher = std::collections::hash_map::DefaultHasher::new();
            k.hash(&mut entry_hasher);
            v.hash(&mut entry_hasher);
            acc = acc.wrapping_add(entry_hasher.finish());
        }
        state.write_u64(acc);
    }
}

impl std::fmt::Display for ClientQuotaEntity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientQuotaEntity(entries={:?})", self.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entity(pairs: &[(&str, &str)]) -> ClientQuotaEntity {
        ClientQuotaEntity::new(pairs.iter().map(|(k, v)| ((*k).to_string(), Some((*v).to_string()))).collect())
    }

    // New test, no Java original: the four `common/quota` classes have no
    // dedicated Java test files (upstream gap, see DoD #3 note in PLAN.md).
    #[test]
    fn is_valid_entity_type_matches_builtins() {
        assert!(ClientQuotaEntity::is_valid_entity_type(USER));
        assert!(ClientQuotaEntity::is_valid_entity_type(CLIENT_ID));
        assert!(ClientQuotaEntity::is_valid_entity_type(IP));
        assert!(!ClientQuotaEntity::is_valid_entity_type("group"));
    }

    // New test, no Java original.
    #[test]
    fn equality_is_order_independent() {
        let a = entity(&[(USER, "u1"), (CLIENT_ID, "c1")]);
        let b = entity(&[(CLIENT_ID, "c1"), (USER, "u1")]);
        assert_eq!(a, b);
        assert_ne!(a, entity(&[(USER, "u2")]));
    }

    // New test, no Java original: equal entities must hash equal so they can be
    // used as `HashMap` keys.
    #[test]
    fn equal_entities_hash_equal() {
        use std::collections::HashMap as Map;
        let mut map: Map<ClientQuotaEntity, i32> = Map::new();
        map.insert(entity(&[(USER, "u1"), (CLIENT_ID, "c1")]), 7);
        assert_eq!(map.get(&entity(&[(CLIENT_ID, "c1"), (USER, "u1")])), Some(&7));
    }

    // New test, no Java original.
    #[test]
    fn entries_accessor_round_trips() {
        let e = entity(&[(USER, "u1")]);
        assert_eq!(e.entries().get(USER), Some(&Some("u1".to_string())));
    }

    // New test, no Java original: a `None` name (the built-in default entity,
    // e.g. `--entity-type users --entity-default`) is representable and stays
    // distinct from an entity literally named "" (`Some("")`).
    #[test]
    fn default_entity_none_distinct_from_empty_name() {
        let default_user = ClientQuotaEntity::new(HashMap::from([(USER.to_string(), None)]));
        let empty_named_user = ClientQuotaEntity::new(HashMap::from([(USER.to_string(), Some(String::new()))]));
        assert_eq!(default_user.entries().get(USER), Some(&None));
        assert_eq!(empty_named_user.entries().get(USER), Some(&Some(String::new())));
        assert_ne!(default_user, empty_named_user);
    }
}

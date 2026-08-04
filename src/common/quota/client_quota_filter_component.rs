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

//! Client quota filter component type.
//!
//! Corresponds to `org.apache.kafka.common.quota.ClientQuotaFilterComponent`.

/// The match specification of a [`ClientQuotaFilterComponent`].
///
/// This models the tri-state that Java represents with `Optional<String>`:
///   - `Exact(name)`  — matches the name exactly (`Optional.of(name)`),
///   - `Default`      — matches the built-in default name (`Optional.empty()`),
///   - `Any`          — matches any specified name (Java `null`).
///
/// Java distinguishes `Optional.empty()` (default) from `null` (any), so the
/// two must remain distinct here; folding them together would break
/// `equals`/wire encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ClientQuotaMatch {
    /// Matches the provided entity name exactly (`Optional.of(name)`).
    Exact(String),
    /// Matches the built-in default entity name (`Optional.empty()`).
    Default,
    /// Matches any specified name for the entity type (Java `null`).
    Any,
}

/// Describes a component for applying a client quota filter.
///
/// Corresponds to `org.apache.kafka.common.quota.ClientQuotaFilterComponent`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ClientQuotaFilterComponent {
    entity_type: String,
    match_spec: ClientQuotaMatch,
}

impl ClientQuotaFilterComponent {
    /// Constructs and returns a filter component that exactly matches the
    /// provided entity name for the entity type.
    ///
    /// Mirrors `ClientQuotaFilterComponent.ofEntity`.
    pub fn of_entity(entity_type: impl Into<String>, entity_name: impl Into<String>) -> Self {
        Self {
            entity_type: entity_type.into(),
            match_spec: ClientQuotaMatch::Exact(entity_name.into()),
        }
    }

    /// Constructs and returns a filter component that matches the built-in
    /// default entity name for the entity type.
    ///
    /// Mirrors `ClientQuotaFilterComponent.ofDefaultEntity`.
    pub fn of_default_entity(entity_type: impl Into<String>) -> Self {
        Self { entity_type: entity_type.into(), match_spec: ClientQuotaMatch::Default }
    }

    /// Constructs and returns a filter component that matches any specified name
    /// for the entity type.
    ///
    /// Mirrors `ClientQuotaFilterComponent.ofEntityType`.
    pub fn of_entity_type(entity_type: impl Into<String>) -> Self {
        Self { entity_type: entity_type.into(), match_spec: ClientQuotaMatch::Any }
    }

    /// Returns the component's entity type.
    ///
    /// Mirrors `ClientQuotaFilterComponent.entityType()`.
    pub fn entity_type(&self) -> &str {
        &self.entity_type
    }

    /// Returns the match specification.
    ///
    /// Mirrors `ClientQuotaFilterComponent.match()`, whose `Optional<String>`
    /// tri-state is modeled by [`ClientQuotaMatch`]:
    ///   - `Exact(name)`: the name that's matched exactly,
    ///   - `Default`: matches the default name,
    ///   - `Any`: matches any specified name.
    pub fn match_spec(&self) -> &ClientQuotaMatch {
        &self.match_spec
    }
}

impl std::fmt::Display for ClientQuotaFilterComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let match_str = match &self.match_spec {
            ClientQuotaMatch::Exact(name) => format!("Optional[{name}]"),
            ClientQuotaMatch::Default => "Optional.empty".to_string(),
            ClientQuotaMatch::Any => "null".to_string(),
        };
        write!(
            f,
            "ClientQuotaFilterComponent(entityType={}, match={})",
            self.entity_type, match_str
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::quota::client_quota_entity::USER;

    /// Translated from `KafkaAdminClientTest.testEqualsOfClientQuotaFilterComponent`.
    #[test]
    fn test_equals_of_client_quota_filter_component() {
        assert_eq!(
            ClientQuotaFilterComponent::of_default_entity(USER),
            ClientQuotaFilterComponent::of_default_entity(USER)
        );

        assert_eq!(
            ClientQuotaFilterComponent::of_entity_type(USER),
            ClientQuotaFilterComponent::of_entity_type(USER)
        );

        // match = null is different from match = Empty
        assert_ne!(
            ClientQuotaFilterComponent::of_default_entity(USER),
            ClientQuotaFilterComponent::of_entity_type(USER)
        );

        assert_eq!(
            ClientQuotaFilterComponent::of_entity(USER, "user"),
            ClientQuotaFilterComponent::of_entity(USER, "user")
        );

        assert_ne!(
            ClientQuotaFilterComponent::of_entity(USER, "user"),
            ClientQuotaFilterComponent::of_default_entity(USER)
        );

        assert_ne!(
            ClientQuotaFilterComponent::of_entity(USER, "user"),
            ClientQuotaFilterComponent::of_entity_type(USER)
        );
    }

    // New test, no Java original.
    #[test]
    fn accessors_return_expected_values() {
        let c = ClientQuotaFilterComponent::of_entity(USER, "u1");
        assert_eq!(c.entity_type(), USER);
        assert_eq!(c.match_spec(), &ClientQuotaMatch::Exact("u1".to_string()));

        assert_eq!(
            ClientQuotaFilterComponent::of_default_entity(USER).match_spec(),
            &ClientQuotaMatch::Default
        );
        assert_eq!(
            ClientQuotaFilterComponent::of_entity_type(USER).match_spec(),
            &ClientQuotaMatch::Any
        );
    }
}

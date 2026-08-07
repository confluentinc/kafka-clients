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

//! Client quota filter type.
//!
//! Corresponds to `org.apache.kafka.common.quota.ClientQuotaFilter`.

use crate::common::quota::ClientQuotaFilterComponent;

/// Describes a client quota entity filter.
///
/// Corresponds to `org.apache.kafka.common.quota.ClientQuotaFilter`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientQuotaFilter {
    components: Vec<ClientQuotaFilterComponent>,
    strict: bool,
}

impl ClientQuotaFilter {
    /// A filter to be applied to matching client quotas.
    ///
    /// Mirrors the private `ClientQuotaFilter(Collection, boolean)`
    /// constructor.
    fn new(components: Vec<ClientQuotaFilterComponent>, strict: bool) -> Self {
        Self { components, strict }
    }

    /// Constructs and returns a quota filter that matches all provided
    /// components. Matching entities with entity types that are not specified by
    /// a component will also be included in the result.
    ///
    /// Mirrors `ClientQuotaFilter.contains`.
    pub fn contains(components: Vec<ClientQuotaFilterComponent>) -> Self {
        Self::new(components, false)
    }

    /// Constructs and returns a quota filter that matches all provided
    /// components. Matching entities with entity types that are not specified by
    /// a component will *not* be included in the result.
    ///
    /// Mirrors `ClientQuotaFilter.containsOnly`.
    pub fn contains_only(components: Vec<ClientQuotaFilterComponent>) -> Self {
        Self::new(components, true)
    }

    /// Constructs and returns a quota filter that matches all configured
    /// entities.
    ///
    /// Mirrors `ClientQuotaFilter.all`.
    pub fn all() -> Self {
        Self::new(Vec::new(), false)
    }

    /// Returns the filter's components.
    ///
    /// Mirrors `ClientQuotaFilter.components()`.
    pub fn components(&self) -> &[ClientQuotaFilterComponent] {
        &self.components
    }

    /// Returns whether the filter is strict, i.e. only includes specified
    /// components.
    ///
    /// Mirrors `ClientQuotaFilter.strict()`.
    pub fn strict(&self) -> bool {
        self.strict
    }
}

impl std::fmt::Display for ClientQuotaFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ClientQuotaFilter(components={:?}, strict={})", self.components, self.strict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::quota::client_quota_entity::USER;

    // New test, no Java original: the four `common/quota` classes have no
    // dedicated Java test files (upstream gap, see DoD #3 note in PLAN.md).
    #[test]
    fn contains_is_not_strict() {
        let f = ClientQuotaFilter::contains(vec![ClientQuotaFilterComponent::of_entity(USER, "u1")]);
        assert!(!f.strict());
        assert_eq!(f.components().len(), 1);
    }

    // New test, no Java original.
    #[test]
    fn contains_only_is_strict() {
        let f = ClientQuotaFilter::contains_only(vec![ClientQuotaFilterComponent::of_default_entity(USER)]);
        assert!(f.strict());
    }

    // New test, no Java original.
    #[test]
    fn all_is_empty_and_not_strict() {
        let f = ClientQuotaFilter::all();
        assert!(f.components().is_empty());
        assert!(!f.strict());
    }

    // New test, no Java original.
    #[test]
    fn equality_considers_components_and_strict() {
        let c = vec![ClientQuotaFilterComponent::of_entity(USER, "u1")];
        assert_eq!(ClientQuotaFilter::contains(c.clone()), ClientQuotaFilter::contains(c.clone()));
        assert_ne!(ClientQuotaFilter::contains(c.clone()), ClientQuotaFilter::contains_only(c));
    }
}

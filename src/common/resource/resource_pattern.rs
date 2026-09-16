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

//! ACL resource patterns.
//!
//! Corresponds to `org.apache.kafka.common.resource.ResourcePattern`.

use crate::common::Error;

use super::{PatternType, ResourcePatternFilter, ResourceType};

/// A special literal resource name that corresponds to 'all resources of a
/// certain type'.
pub const WILDCARD_RESOURCE: &str = "*";

/// Represents a pattern that is used by ACLs to match zero or more
/// [`Resource`](super::Resource)s.
///
/// Corresponds to `org.apache.kafka.common.resource.ResourcePattern`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResourcePattern {
    resource_type: ResourceType,
    name: String,
    pattern_type: PatternType,
}

impl ResourcePattern {
    /// Create a pattern using the supplied parameters.
    ///
    /// # Arguments
    /// * `resource_type` - specific resource type
    /// * `name` - resource name, which can be the [`WILDCARD_RESOURCE`]
    /// * `pattern_type` - specific resource pattern type, which controls how the
    ///   pattern will match resource names
    ///
    /// # Errors
    ///
    /// Returns [`Error::LocalIllegalArgument`] if `resource_type` is
    /// [`ResourceType::Any`], or `pattern_type` is [`PatternType::Match`] or
    /// [`PatternType::Any`] (mirrors Java's `IllegalArgumentException`).
    pub fn new(
        resource_type: ResourceType,
        name: impl Into<String>,
        pattern_type: PatternType,
    ) -> Result<ResourcePattern, Error> {
        if resource_type == ResourceType::Any {
            return Err(Error::local_illegal_argument("resourceType must not be ANY"));
        }
        if pattern_type == PatternType::Match || pattern_type == PatternType::Any {
            return Err(Error::local_illegal_argument(format!("patternType must not be {pattern_type}")));
        }
        Ok(ResourcePattern { resource_type, name: name.into(), pattern_type })
    }

    /// Return the specific resource type this pattern matches.
    pub fn resource_type(&self) -> ResourceType {
        self.resource_type
    }

    /// Return the resource name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Return the resource pattern type.
    pub fn pattern_type(&self) -> PatternType {
        self.pattern_type
    }

    /// Return a filter which matches only this pattern.
    pub fn to_filter(&self) -> ResourcePatternFilter {
        ResourcePatternFilter::new(self.resource_type, Some(self.name.clone()), self.pattern_type)
    }

    /// Return `true` if this pattern has any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.resource_type.is_unknown() || self.pattern_type.is_unknown()
    }
}

impl std::fmt::Display for ResourcePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ResourcePattern(resourceType={}, name={}, patternType={})",
            self.resource_type, self.name, self.pattern_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Java's `ResourcePatternTest.shouldThrowIfResourceNameIsNull` is not
    // representable: the Rust `name` parameter is a non-nullable `String`, so
    // there is no null to reject (Java's `Objects.requireNonNull(name)`).

    #[test]
    fn should_throw_if_resource_type_is_any() {
        let err = ResourcePattern::new(ResourceType::Any, "name", PatternType::Literal);
        assert!(matches!(err, Err(Error::LocalIllegalArgument(_))));
    }

    #[test]
    fn should_throw_if_pattern_type_is_match() {
        let err = ResourcePattern::new(ResourceType::Topic, "name", PatternType::Match);
        assert!(matches!(err, Err(Error::LocalIllegalArgument(_))));
    }

    #[test]
    fn should_throw_if_pattern_type_is_any() {
        let err = ResourcePattern::new(ResourceType::Topic, "name", PatternType::Any);
        assert!(matches!(err, Err(Error::LocalIllegalArgument(_))));
    }

    #[test]
    fn accessors_and_to_filter() {
        let pattern = ResourcePattern::new(ResourceType::Topic, "foo", PatternType::Literal).unwrap();
        assert_eq!(pattern.resource_type(), ResourceType::Topic);
        assert_eq!(pattern.name(), "foo");
        assert_eq!(pattern.pattern_type(), PatternType::Literal);
        assert!(!pattern.is_unknown());
        let filter = pattern.to_filter();
        assert!(filter.matches(&pattern));
    }

    #[test]
    fn is_unknown_for_unknown_components() {
        let pattern = ResourcePattern::new(ResourceType::Unknown, "foo", PatternType::Literal).unwrap();
        assert!(pattern.is_unknown());
        let pattern = ResourcePattern::new(ResourceType::Topic, "foo", PatternType::Unknown).unwrap();
        assert!(pattern.is_unknown());
    }
}

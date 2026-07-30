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

//! ACL resource pattern filters.
//!
//! Corresponds to `org.apache.kafka.common.resource.ResourcePatternFilter`.

use super::resource_pattern::WILDCARD_RESOURCE;
use super::{PatternType, ResourcePattern, ResourceType};

/// Represents a filter that can match [`ResourcePattern`].
///
/// Corresponds to `org.apache.kafka.common.resource.ResourcePatternFilter`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ResourcePatternFilter {
    resource_type: ResourceType,
    name: Option<String>,
    pattern_type: PatternType,
}

impl ResourcePatternFilter {
    /// Create a filter using the supplied parameters.
    ///
    /// # Arguments
    /// * `resource_type` - resource type. If [`ResourceType::Any`], the filter
    ///   will ignore the resource type of the pattern. If any other resource
    ///   type, the filter will match only patterns with the same type.
    /// * `name` - resource name or `None`. If `None`, the filter will ignore the
    ///   name of resources. If [`WILDCARD_RESOURCE`], will match only wildcard
    ///   patterns.
    /// * `pattern_type` - resource pattern type. If [`PatternType::Any`], the
    ///   filter will match patterns regardless of pattern type. If
    ///   [`PatternType::Match`], the filter will match patterns that would match
    ///   the supplied `name`, including matching prefixed and wildcard patterns.
    ///   If any other resource pattern type, the filter will match only patterns
    ///   with the same type.
    pub fn new(resource_type: ResourceType, name: Option<String>, pattern_type: PatternType) -> ResourcePatternFilter {
        ResourcePatternFilter { resource_type, name, pattern_type }
    }

    /// A filter which matches any resource pattern.
    pub fn any() -> ResourcePatternFilter {
        ResourcePatternFilter::new(ResourceType::Any, None, PatternType::Any)
    }

    /// Return `true` if this filter has any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.resource_type.is_unknown() || self.pattern_type.is_unknown()
    }

    /// Return the specific resource type this pattern matches.
    pub fn resource_type(&self) -> ResourceType {
        self.resource_type
    }

    /// Return the resource name, or `None` to match any name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Return the resource pattern type.
    pub fn pattern_type(&self) -> PatternType {
        self.pattern_type
    }

    /// Return `true` if this filter matches the given pattern.
    pub fn matches(&self, pattern: &ResourcePattern) -> bool {
        if self.resource_type != ResourceType::Any && self.resource_type != pattern.resource_type() {
            return false;
        }

        if self.pattern_type != PatternType::Any
            && self.pattern_type != PatternType::Match
            && self.pattern_type != pattern.pattern_type()
        {
            return false;
        }

        let Some(name) = self.name.as_deref() else {
            return true;
        };

        if self.pattern_type == PatternType::Any || self.pattern_type == pattern.pattern_type() {
            return name == pattern.name();
        }

        match pattern.pattern_type() {
            PatternType::Literal => name == pattern.name() || pattern.name() == WILDCARD_RESOURCE,
            PatternType::Prefixed => name.starts_with(pattern.name()),
            // Java throws IllegalArgumentException here; this branch is only
            // reachable for MATCH/ANY/UNKNOWN pattern types on the *pattern*
            // (not the filter), which are not valid concrete patterns.
            other => panic!("Unsupported PatternType: {other}"),
        }
    }

    /// Return `true` if this filter could only match one pattern. In other
    /// words, if there are no ANY or UNKNOWN fields.
    pub fn matches_at_most_one(&self) -> bool {
        self.find_indefinite_field().is_none()
    }

    /// Return a string describing any ANY or UNKNOWN field, or `None` if there is
    /// no such field.
    pub fn find_indefinite_field(&self) -> Option<String> {
        if self.resource_type == ResourceType::Any {
            return Some("Resource type is ANY.".to_string());
        }
        if self.resource_type == ResourceType::Unknown {
            return Some("Resource type is UNKNOWN.".to_string());
        }
        if self.name.is_none() {
            return Some("Resource name is NULL.".to_string());
        }
        if self.pattern_type == PatternType::Match {
            return Some("Resource pattern type is MATCH.".to_string());
        }
        if self.pattern_type == PatternType::Unknown {
            return Some("Resource pattern type is UNKNOWN.".to_string());
        }
        None
    }
}

impl std::fmt::Display for ResourcePatternFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ResourcePattern(resourceType={}, name={}, patternType={})",
            self.resource_type,
            self.name.as_deref().unwrap_or("<any>"),
            self.pattern_type
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pattern(rt: ResourceType, name: &str, pt: PatternType) -> ResourcePattern {
        ResourcePattern::new(rt, name, pt).unwrap()
    }

    fn filter(rt: ResourceType, name: Option<&str>, pt: PatternType) -> ResourcePatternFilter {
        ResourcePatternFilter::new(rt, name.map(str::to_string), pt)
    }

    #[test]
    fn should_be_unknown_if_resource_type_unknown() {
        assert!(filter(ResourceType::Unknown, None, PatternType::Literal).is_unknown());
    }

    #[test]
    fn should_be_unknown_if_pattern_type_unknown() {
        assert!(filter(ResourceType::Group, None, PatternType::Unknown).is_unknown());
    }

    #[test]
    fn should_not_match_if_different_resource_type() {
        assert!(
            !filter(ResourceType::Topic, Some("Name"), PatternType::Literal).matches(&pattern(
                ResourceType::Group,
                "Name",
                PatternType::Literal
            ))
        );
    }

    #[test]
    fn should_not_match_if_different_name() {
        assert!(
            !filter(ResourceType::Topic, Some("Different"), PatternType::Prefixed).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Prefixed
            ))
        );
    }

    #[test]
    fn should_not_match_if_different_name_case() {
        assert!(
            !filter(ResourceType::Topic, Some("NAME"), PatternType::Literal).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Literal
            ))
        );
    }

    #[test]
    fn should_not_match_if_different_pattern_type() {
        assert!(
            !filter(ResourceType::Topic, Some("Name"), PatternType::Literal).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Prefixed
            ))
        );
    }

    #[test]
    fn should_match_where_resource_type_is_any() {
        assert!(filter(ResourceType::Any, Some("Name"), PatternType::Prefixed).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Prefixed
        )));
    }

    #[test]
    fn should_match_where_resource_name_is_any() {
        assert!(filter(ResourceType::Topic, None, PatternType::Prefixed).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Prefixed
        )));
    }

    #[test]
    fn should_match_where_pattern_type_is_any() {
        assert!(filter(ResourceType::Topic, None, PatternType::Any).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Prefixed
        )));
    }

    #[test]
    fn should_match_where_pattern_type_is_match() {
        assert!(filter(ResourceType::Topic, None, PatternType::Match).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Prefixed
        )));
    }

    #[test]
    fn should_match_literal_if_exact_match() {
        assert!(
            filter(ResourceType::Topic, Some("Name"), PatternType::Literal).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Literal
            ))
        );
    }

    #[test]
    fn should_match_literal_if_name_matches_and_filter_is_on_pattern_type_any() {
        assert!(filter(ResourceType::Topic, Some("Name"), PatternType::Any).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Literal
        )));
    }

    #[test]
    fn should_match_literal_if_name_matches_and_filter_is_on_pattern_type_match() {
        assert!(filter(ResourceType::Topic, Some("Name"), PatternType::Match).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Literal
        )));
    }

    #[test]
    fn should_not_match_literal_if_name_prefixed() {
        assert!(
            !filter(ResourceType::Topic, Some("Name-something"), PatternType::Match).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Literal
            ))
        );
    }

    #[test]
    fn should_match_literal_wildcard_if_exact_match() {
        assert!(filter(ResourceType::Topic, Some("*"), PatternType::Literal).matches(&pattern(
            ResourceType::Topic,
            "*",
            PatternType::Literal
        )));
    }

    #[test]
    fn should_not_match_literal_wildcard_against_other_name() {
        assert!(
            !filter(ResourceType::Topic, Some("Name"), PatternType::Literal).matches(&pattern(
                ResourceType::Topic,
                "*",
                PatternType::Literal
            ))
        );
    }

    #[test]
    fn should_not_match_literal_wildcard_the_way_around() {
        assert!(!filter(ResourceType::Topic, Some("*"), PatternType::Literal).matches(&pattern(
            ResourceType::Topic,
            "Name",
            PatternType::Literal
        )));
    }

    #[test]
    fn should_not_match_literal_wildcard_if_filter_has_pattern_type_of_any() {
        assert!(!filter(ResourceType::Topic, Some("Name"), PatternType::Any).matches(&pattern(
            ResourceType::Topic,
            "*",
            PatternType::Literal
        )));
    }

    #[test]
    fn should_match_literal_wildcard_if_filter_has_pattern_type_of_match() {
        assert!(filter(ResourceType::Topic, Some("Name"), PatternType::Match).matches(&pattern(
            ResourceType::Topic,
            "*",
            PatternType::Literal
        )));
    }

    #[test]
    fn should_match_prefixed_if_exact_match() {
        assert!(
            filter(ResourceType::Topic, Some("Name"), PatternType::Prefixed).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Prefixed
            ))
        );
    }

    #[test]
    fn should_not_match_if_both_prefixed_and_filter_is_prefix_of_resource() {
        assert!(
            !filter(ResourceType::Topic, Some("Name"), PatternType::Prefixed).matches(&pattern(
                ResourceType::Topic,
                "Name-something",
                PatternType::Prefixed
            ))
        );
    }

    #[test]
    fn should_not_match_if_both_prefixed_and_resource_is_prefix_of_filter() {
        assert!(
            !filter(ResourceType::Topic, Some("Name-something"), PatternType::Prefixed).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Prefixed
            ))
        );
    }

    #[test]
    fn should_not_match_prefixed_if_name_prefixed_any_filter_type_is_any() {
        assert!(
            !filter(ResourceType::Topic, Some("Name-something"), PatternType::Any).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Prefixed
            ))
        );
    }

    #[test]
    fn should_match_prefixed_if_name_prefixed_any_filter_type_is_match() {
        assert!(
            filter(ResourceType::Topic, Some("Name-something"), PatternType::Match).matches(&pattern(
                ResourceType::Topic,
                "Name",
                PatternType::Prefixed
            ))
        );
    }
}

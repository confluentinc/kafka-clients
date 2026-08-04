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

//! ACL binding filters.
//!
//! Corresponds to `org.apache.kafka.common.acl.AclBindingFilter`.

use crate::common::resource::ResourcePatternFilter;

use super::{AccessControlEntryFilter, AclBinding};

/// A filter which can match `AclBinding` objects.
///
/// Corresponds to `org.apache.kafka.common.acl.AclBindingFilter`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct AclBindingFilter {
    pattern_filter: ResourcePatternFilter,
    entry_filter: AccessControlEntryFilter,
}

impl AclBindingFilter {
    /// Create an instance of this filter with the provided parameters.
    ///
    /// # Arguments
    /// * `pattern_filter` - pattern filter
    /// * `entry_filter` - access control entry filter
    pub fn new(pattern_filter: ResourcePatternFilter, entry_filter: AccessControlEntryFilter) -> AclBindingFilter {
        AclBindingFilter { pattern_filter, entry_filter }
    }

    /// A filter which matches any ACL binding.
    pub fn any() -> AclBindingFilter {
        AclBindingFilter::new(ResourcePatternFilter::any(), AccessControlEntryFilter::any())
    }

    /// Return `true` if this filter has any UNKNOWN components.
    pub fn is_unknown(&self) -> bool {
        self.pattern_filter.is_unknown() || self.entry_filter.is_unknown()
    }

    /// Return the resource pattern filter.
    pub fn pattern_filter(&self) -> &ResourcePatternFilter {
        &self.pattern_filter
    }

    /// Return the access control entry filter.
    pub fn entry_filter(&self) -> &AccessControlEntryFilter {
        &self.entry_filter
    }

    /// Return true if the resource and entry filters can only match one ACE. In
    /// other words, if there are no ANY or UNKNOWN fields.
    pub fn matches_at_most_one(&self) -> bool {
        self.pattern_filter.matches_at_most_one() && self.entry_filter.matches_at_most_one()
    }

    /// Return a string describing an ANY or UNKNOWN field, or `None` if there is
    /// no such field.
    pub fn find_indefinite_field(&self) -> Option<String> {
        if let Some(indefinite) = self.pattern_filter.find_indefinite_field() {
            return Some(indefinite);
        }
        self.entry_filter.find_indefinite_field()
    }

    /// Return true if the resource filter matches the binding's resource and the
    /// entry filter matches the binding's entry.
    pub fn matches(&self, binding: &AclBinding) -> bool {
        self.pattern_filter.matches(binding.pattern()) && self.entry_filter.matches(binding.entry())
    }
}

impl std::fmt::Display for AclBindingFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(patternFilter={}, entryFilter={})", self.pattern_filter, self.entry_filter)
    }
}

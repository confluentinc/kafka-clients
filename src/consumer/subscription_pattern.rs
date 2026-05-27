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

//! A regular expression used to subscribe to topics.
//!
//! Translated from `org.apache.kafka.clients.consumer.SubscriptionPattern`.

use std::fmt;

/// Represents a regular expression compatible with Google RE2/J, used to
/// subscribe to topics.
///
/// This just keeps the [`String`] representation of the pattern; all
/// validations to ensure it is RE2/J compatible are delegated to the broker
/// (matching Java's behavior).
///
/// Corresponds to Java's `org.apache.kafka.clients.consumer.SubscriptionPattern`.
#[derive(Clone, Debug)]
pub struct SubscriptionPattern {
    pattern: String,
}

impl SubscriptionPattern {
    /// Create a new `SubscriptionPattern` for the given regex pattern.
    ///
    /// The pattern is stored as-is; no client-side validation is performed.
    pub fn new(pattern: impl Into<String>) -> Self {
        Self { pattern: pattern.into() }
    }

    /// Returns the regular expression pattern compatible with RE2/J.
    pub fn pattern(&self) -> &str {
        &self.pattern
    }
}

impl fmt::Display for SubscriptionPattern {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.pattern)
    }
}

impl PartialEq for SubscriptionPattern {
    fn eq(&self, other: &Self) -> bool {
        self.pattern == other.pattern
    }
}

impl Eq for SubscriptionPattern {}

impl std::hash::Hash for SubscriptionPattern {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_accessor() {
        let p = SubscriptionPattern::new("foo.*");
        assert_eq!(p.pattern(), "foo.*");
    }

    #[test]
    fn test_display_is_pattern() {
        let p = SubscriptionPattern::new("foo.*");
        assert_eq!(p.to_string(), "foo.*");
    }

    #[test]
    fn test_equality_and_hash() {
        use std::collections::HashSet;
        let p1 = SubscriptionPattern::new("a.*");
        let p2 = SubscriptionPattern::new("a.*");
        let p3 = SubscriptionPattern::new("b.*");
        assert_eq!(p1, p2);
        assert_ne!(p1, p3);

        let mut set = HashSet::new();
        set.insert(p1);
        set.insert(p2);
        assert_eq!(set.len(), 1);
        set.insert(p3);
        assert_eq!(set.len(), 2);
    }
}

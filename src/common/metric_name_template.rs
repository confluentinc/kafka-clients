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

//! A template for a [`MetricName`](crate::common::MetricName).
//!
//! Translated from `org.apache.kafka.common.MetricNameTemplate`.

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

use indexmap::IndexSet;

/// A template for a metric name. It holds a name, group, and description, plus
/// the set of tag names that will be filled in at runtime with their values.
///
/// The order of the tags is preserved so that generated names can be compared
/// and sorted lexicographically, but two templates with the same tag names in
/// a different order are still considered equal.
#[derive(Clone, Debug)]
pub struct MetricNameTemplate {
    name: String,
    group: String,
    description: String,
    tags: IndexSet<String>,
}

impl MetricNameTemplate {
    /// Creates a new template. The order of the tags is preserved from the
    /// supplied set.
    ///
    /// * `name` — the name of the metric
    /// * `group` — the name of the group
    /// * `description` — the description of the metric
    /// * `tags` — the set of metric tag names; iteration order is retained
    pub fn new(
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tags: IndexSet<String>,
    ) -> Self {
        Self { name: name.into(), group: group.into(), description: description.into(), tags }
    }

    /// Creates a new template from an ordered list of tag names.
    pub fn with_tag_names(
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tag_names: &[&str],
    ) -> Self {
        Self::new(name, group, description, tag_names.iter().map(|t| (*t).to_string()).collect())
    }

    /// The name of the metric; never empty by construction.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The name of the group.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The description of the metric.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// The ordered set of tag names for the metric; possibly empty.
    pub fn tags(&self) -> &IndexSet<String> {
        &self.tags
    }
}

impl PartialEq for MetricNameTemplate {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.group == other.group
            && self.tags.len() == other.tags.len()
            && self.tags.iter().all(|t| other.tags.contains(t))
    }
}

impl Eq for MetricNameTemplate {}

impl Hash for MetricNameTemplate {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.group.hash(state);
        // Order-independent fold over the tag names, consistent with equality.
        let mut tags_hash: u64 = 0;
        for t in &self.tags {
            let mut entry = DefaultHasher::new();
            t.hash(&mut entry);
            tags_hash = tags_hash.wrapping_add(entry.finish());
        }
        tags_hash.hash(state);
    }
}

impl fmt::Display for MetricNameTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "name={}, group={}, tags=[", self.name, self.group)?;
        for (i, t) in self.tags.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{t}")?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_getters_preserve_tag_order() {
        let template = MetricNameTemplate::with_tag_names("n", "g", "d", &["client-id", "topic"]);
        assert_eq!(template.name(), "n");
        assert_eq!(template.group(), "g");
        assert_eq!(template.description(), "d");
        let order: Vec<&str> = template.tags().iter().map(String::as_str).collect();
        assert_eq!(order, vec!["client-id", "topic"]);
    }

    #[test]
    fn test_equality_is_tag_order_independent() {
        let a = MetricNameTemplate::with_tag_names("n", "g", "d", &["a", "b"]);
        let b = MetricNameTemplate::with_tag_names("n", "g", "other-desc", &["b", "a"]);
        assert_eq!(a, b);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        b.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_inequality() {
        let a = MetricNameTemplate::with_tag_names("n", "g", "d", &["a"]);
        assert_ne!(a, MetricNameTemplate::with_tag_names("n", "g", "d", &["a", "b"]));
        assert_ne!(a, MetricNameTemplate::with_tag_names("other", "g", "d", &["a"]));
    }

    #[test]
    fn test_to_string() {
        let template = MetricNameTemplate::with_tag_names("n", "g", "d", &["a", "b"]);
        assert_eq!(template.to_string(), "name=n, group=g, tags=[a, b]");
    }
}

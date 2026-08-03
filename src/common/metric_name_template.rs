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

//! A template for a `MetricName` (`org.apache.kafka.common.MetricNameTemplate`).

use indexmap::IndexSet;

/// A template for a `MetricName`. It contains a name, group, and description, as
/// well as all the tags that will be used to create the metric name. Tag values
/// are omitted from the template, but are filled in at runtime with their
/// specified values. The order of the tags is maintained so that the metric
/// names can be compared and sorted lexicographically.
#[derive(Clone, Debug)]
pub struct MetricNameTemplate {
    name: String,
    group: String,
    description: String,
    // Java uses a `LinkedHashSet<String>` to preserve insertion order while
    // remaining a set. `IndexSet` is the order-preserving Rust equivalent.
    tags: IndexSet<String>,
}

impl MetricNameTemplate {
    /// Create a new template. The order of the tags is preserved.
    ///
    /// * `name` - the name of the metric
    /// * `group` - the name of the group
    /// * `description` - the description of the metric
    /// * `tag_names` - the names of the metric tags in the preferred order
    pub fn new(
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tag_names: IndexSet<String>,
    ) -> Self {
        Self {
            name: name.into(),
            group: group.into(),
            description: description.into(),
            tags: tag_names,
        }
    }

    /// Get the name of the metric.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the name of the group.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Get the description of the metric.
    pub fn description(&self) -> &str {
        &self.description
    }

    /// Get the ordered set of tag names for the metric.
    pub fn tags(&self) -> &IndexSet<String> {
        &self.tags
    }
}

impl PartialEq for MetricNameTemplate {
    fn eq(&self, other: &Self) -> bool {
        // Mirrors Java: equality is on name, group, and tags (set equality,
        // order-independent).
        self.name == other.name && self.group == other.group && self.tags == other.tags
    }
}

impl Eq for MetricNameTemplate {}

impl std::fmt::Display for MetricNameTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "name={}, group={}, tags={:?}", self.name, self.group, self.tags)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> IndexSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn equality_is_tag_order_independent() {
        let a = MetricNameTemplate::new("n", "g", "d", set(&["a", "b"]));
        let b = MetricNameTemplate::new("n", "g", "different", set(&["b", "a"]));
        assert_eq!(a, b);
    }

    #[test]
    fn inequality_on_name_group_tags() {
        let base = MetricNameTemplate::new("n", "g", "", set(&["a"]));
        assert_ne!(base, MetricNameTemplate::new("n2", "g", "", set(&["a"])));
        assert_ne!(base, MetricNameTemplate::new("n", "g2", "", set(&["a"])));
        assert_ne!(base, MetricNameTemplate::new("n", "g", "", set(&["b"])));
    }
}

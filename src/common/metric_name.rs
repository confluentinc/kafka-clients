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

//! The `MetricName` class encapsulates a metric's name, logical group and its
//! related attributes (`org.apache.kafka.common.MetricName`).

use std::collections::BTreeMap;

/// The `MetricName` class encapsulates a metric's name, logical group and its
/// related attributes. It should be constructed using `Metrics::metric_name(...)`.
///
/// This class captures the following parameters:
///   - `name`: The name of the metric
///   - `group`: logical group name of the metrics to which this metric belongs.
///   - `description`: A human-readable description to include in the metric. This is optional.
///   - `tags`: additional key/value attributes of the metric. This is optional.
///
/// `group` and `tags` parameters can be used to create unique metric names while
/// reporting in any custom reporting.
///
/// Two `MetricName`s are equal if their `name`, `group`, and `tags` match;
/// `description` is intentionally excluded from equality and hashing, matching
/// the Java implementation.
#[derive(Clone, Debug)]
pub struct MetricName {
    name: String,
    group: String,
    description: String,
    // Java stores tags in an (insertion-ordered) `Map`, but equality and hashing
    // are order-independent. `BTreeMap` gives us deterministic iteration plus
    // order-independent `Eq`/`Hash`, value-identical to Java's `Map.equals`.
    tags: BTreeMap<String, String>,
}

impl MetricName {
    /// Create a `MetricName`.
    ///
    /// Please create `MetricName` via [`crate::common::metrics::Metrics::metric_name`].
    ///
    /// * `name` - The name of the metric
    /// * `group` - logical group name of the metrics to which this metric belongs
    /// * `description` - A human-readable description to include in the metric
    /// * `tags` - additional key/value attributes of the metric
    pub fn new(
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tags: BTreeMap<String, String>,
    ) -> Self {
        Self { name: name.into(), group: group.into(), description: description.into(), tags }
    }

    /// The name of the metric.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The logical group name of the metrics to which this metric belongs.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The additional key/value attributes of the metric.
    pub fn tags(&self) -> &BTreeMap<String, String> {
        &self.tags
    }

    /// A human-readable description of the metric.
    pub fn description(&self) -> &str {
        &self.description
    }
}

impl PartialEq for MetricName {
    fn eq(&self, other: &Self) -> bool {
        // Mirrors Java: equality is on group, name, and tags only (not description).
        self.group == other.group && self.name == other.name && self.tags == other.tags
    }
}

impl Eq for MetricName {}

impl std::hash::Hash for MetricName {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Mirrors Java: hashCode is on group, name, and tags only.
        self.group.hash(state);
        self.name.hash(state);
        self.tags.hash(state);
    }
}

impl std::fmt::Display for MetricName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetricName [name={}, group={}, description={}, tags={:?}]",
            self.name, self.group, self.description, self.tags
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn equality_ignores_description() {
        let n1 = MetricName::new("n", "g", "desc-a", tags(&[("k", "v")]));
        let n2 = MetricName::new("n", "g", "desc-b", tags(&[("k", "v")]));
        assert_eq!(n1, n2);
    }

    #[test]
    fn equality_is_tag_order_independent() {
        let mut t1 = BTreeMap::new();
        t1.insert("a".to_string(), "1".to_string());
        t1.insert("b".to_string(), "2".to_string());
        let mut t2 = BTreeMap::new();
        t2.insert("b".to_string(), "2".to_string());
        t2.insert("a".to_string(), "1".to_string());
        assert_eq!(MetricName::new("n", "g", "", t1), MetricName::new("n", "g", "", t2));
    }

    #[test]
    fn inequality_on_name_group_tags() {
        let base = MetricName::new("n", "g", "", tags(&[("k", "v")]));
        assert_ne!(base, MetricName::new("n2", "g", "", tags(&[("k", "v")])));
        assert_ne!(base, MetricName::new("n", "g2", "", tags(&[("k", "v")])));
        assert_ne!(base, MetricName::new("n", "g", "", tags(&[("k", "v2")])));
    }
}

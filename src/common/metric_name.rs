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

//! A metric's name, logical group, and related attributes.
//!
//! Translated from `org.apache.kafka.common.MetricName`.

use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

use indexmap::IndexMap;

/// Encapsulates a metric's name, logical group, and its related attributes.
///
/// A `MetricName` should be constructed through `Metrics::metric_name(...)`.
/// The `group` and `tags` are used to build unique metric names for
/// reporting.
///
/// The `description` is not part of a metric's identity: it is excluded from
/// equality and hashing, matching the Java contract.
#[derive(Clone, Debug)]
pub struct MetricName {
    name: String,
    group: String,
    description: String,
    tags: IndexMap<String, String>,
}

impl MetricName {
    /// Creates a new metric name.
    ///
    /// * `name` — the name of the metric
    /// * `group` — logical group name of the metrics this metric belongs to
    /// * `description` — a human-readable description of the metric
    /// * `tags` — additional key/value attributes of the metric
    pub fn new(
        name: impl Into<String>,
        group: impl Into<String>,
        description: impl Into<String>,
        tags: IndexMap<String, String>,
    ) -> Self {
        Self { name: name.into(), group: group.into(), description: description.into(), tags }
    }

    /// The name of the metric.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The logical group name of the metric.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The additional key/value attributes of the metric.
    pub fn tags(&self) -> &IndexMap<String, String> {
        &self.tags
    }

    /// The human-readable description of the metric.
    pub fn description(&self) -> &str {
        &self.description
    }
}

impl PartialEq for MetricName {
    fn eq(&self, other: &Self) -> bool {
        // Two names with the same tags but different insertion order are equal,
        // matching Java's order-independent `Map` equality. `description` is
        // deliberately not part of a name's identity.
        self.name == other.name
            && self.group == other.group
            && self.tags.len() == other.tags.len()
            && self.tags.iter().all(|(k, v)| other.tags.get(k) == Some(v))
    }
}

impl Eq for MetricName {}

impl Hash for MetricName {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.group.hash(state);
        // Fold the tag entries with a commutative operation so that the hash is
        // independent of iteration order, keeping it consistent with equality.
        let mut tags_hash: u64 = 0;
        for (k, v) in &self.tags {
            let mut entry = DefaultHasher::new();
            k.hash(&mut entry);
            v.hash(&mut entry);
            tags_hash = tags_hash.wrapping_add(entry.finish());
        }
        tags_hash.hash(state);
    }
}

impl fmt::Display for MetricName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MetricName [name={}, group={}, description={}, tags={{",
            self.name, self.group, self.description
        )?;
        for (i, (k, v)) in self.tags.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{k}={v}")?;
        }
        write!(f, "}}]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&str, &str)]) -> IndexMap<String, String> {
        pairs.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect()
    }

    #[test]
    fn test_getters() {
        let name = MetricName::new("n", "g", "d", tags(&[("client-id", "c1")]));
        assert_eq!(name.name(), "n");
        assert_eq!(name.group(), "g");
        assert_eq!(name.description(), "d");
        assert_eq!(name.tags().get("client-id").map(String::as_str), Some("c1"));
    }

    #[test]
    fn test_equality_ignores_description() {
        let a = MetricName::new("n", "g", "desc-a", tags(&[("k", "v")]));
        let b = MetricName::new("n", "g", "desc-b", tags(&[("k", "v")]));
        assert_eq!(a, b);
    }

    #[test]
    fn test_equality_is_tag_order_independent() {
        let a = MetricName::new("n", "g", "d", tags(&[("k1", "v1"), ("k2", "v2")]));
        let b = MetricName::new("n", "g", "d", tags(&[("k2", "v2"), ("k1", "v1")]));
        assert_eq!(a, b);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        b.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_inequality() {
        let a = MetricName::new("n", "g", "d", tags(&[("k", "v")]));
        let different_name = MetricName::new("other", "g", "d", tags(&[("k", "v")]));
        let different_group = MetricName::new("n", "other", "d", tags(&[("k", "v")]));
        let different_tags = MetricName::new("n", "g", "d", tags(&[("k", "other")]));
        assert_ne!(a, different_name);
        assert_ne!(a, different_group);
        assert_ne!(a, different_tags);
    }

    #[test]
    fn test_usable_as_map_key() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(MetricName::new("n", "g", "d", tags(&[("k1", "v1"), ("k2", "v2")])), 1);
        // Lookup with the tags in a different order must still hit.
        let lookup = MetricName::new("n", "g", "ignored-desc", tags(&[("k2", "v2"), ("k1", "v1")]));
        assert_eq!(map.get(&lookup), Some(&1));
    }

    #[test]
    fn test_to_string() {
        let name = MetricName::new("message-size-avg", "producer-metrics", "average", tags(&[("client-id", "c1")]));
        assert_eq!(
            name.to_string(),
            "MetricName [name=message-size-avg, group=producer-metrics, description=average, tags={client-id=c1}]"
        );
    }
}

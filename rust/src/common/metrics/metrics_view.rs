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

//! A live, read-only view of a metrics registry, the Rust form of the
//! `Collections.unmodifiableMap(metrics.metrics())` the Java clients return.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::common::MetricName;
use crate::common::metrics::KafkaMetric;

/// The map a metrics registry holds: every metric, indexed by its name.
pub(crate) type MetricMap = HashMap<MetricName, Arc<KafkaMetric>>;

/// The metrics map shared by a registry and the views it hands out.
///
/// Copy-on-write: a reader clones the inner `Arc` under the read lock, a
/// writer goes through `Arc::make_mut` under the write lock, which copies the
/// map only while a reader still holds the previous version. So a reader never
/// blocks a writer for longer than an `Arc` clone, and a held version never
/// changes, as Java's weakly consistent `ConcurrentHashMap` iteration never
/// fails.
///
/// Rust-only (DoD #7): Java's `Metrics` keeps a `ConcurrentHashMap` that
/// `unmodifiableMap` wraps; the standard library has no concurrent map, so the
/// registry and its views share this one instead.
#[derive(Clone, Default)]
pub(crate) struct SharedMetricMap(Arc<RwLock<Arc<MetricMap>>>);

impl SharedMetricMap {
    /// The map as it is now: one `Arc` clone, no entry copied.
    pub(crate) fn current(&self) -> Arc<MetricMap> {
        Arc::clone(&self.0.read().expect("metrics lock poisoned"))
    }

    /// Inserts `metric` under `name` unless a metric is already registered
    /// there, which it returns instead.
    pub(crate) fn insert_if_absent(&self, name: MetricName, metric: Arc<KafkaMetric>) -> Option<Arc<KafkaMetric>> {
        let mut map = self.0.write().expect("metrics lock poisoned");
        if let Some(existing) = map.get(&name) {
            return Some(Arc::clone(existing));
        }
        Arc::make_mut(&mut map).insert(name, metric);
        None
    }

    /// Registers `metric` under `name`, replacing any metric already there.
    pub(crate) fn insert(&self, name: MetricName, metric: Arc<KafkaMetric>) {
        Arc::make_mut(&mut self.0.write().expect("metrics lock poisoned")).insert(name, metric);
    }

    /// Removes the metric registered under `name` and returns it.
    pub(crate) fn remove(&self, name: &MetricName) -> Option<Arc<KafkaMetric>> {
        let mut map = self.0.write().expect("metrics lock poisoned");
        if !map.contains_key(name) {
            // No write, so no copy of a map a reader still holds.
            return None;
        }
        Arc::make_mut(&mut map).remove(name)
    }

    /// Replaces the whole map.
    pub(crate) fn replace(&self, metrics: MetricMap) {
        *self.0.write().expect("metrics lock poisoned") = Arc::new(metrics);
    }

    /// The metric registered under `name`.
    pub(crate) fn get(&self, name: &MetricName) -> Option<Arc<KafkaMetric>> {
        self.0.read().expect("metrics lock poisoned").get(name).cloned()
    }

    /// Whether a metric is registered under `name`.
    pub(crate) fn contains_key(&self, name: &MetricName) -> bool {
        self.0.read().expect("metrics lock poisoned").contains_key(name)
    }

    /// How many metrics are registered.
    pub(crate) fn len(&self) -> usize {
        self.0.read().expect("metrics lock poisoned").len()
    }
}

/// A live, read-only view of the metrics of a client, indexed by metric name:
/// what Java's clients return from `metrics()` as
/// `Collections.unmodifiableMap(metrics.metrics())`.
///
/// Every lookup reads the registry as it is at the time of the call, so a
/// metric registered after the view was returned is visible through it and a
/// removed one is not. [`Self::snapshot`] gives the whole map as it is now, to
/// iterate without blocking the registry, as Java's weakly consistent iteration
/// does. A view stays valid after its client is closed.
///
/// A view can also stand alone, built from a map ([`From`]) or empty
/// ([`Default`]), for a test double or a user implementation of `Producer` or
/// `Consumer`.
///
/// A clone is a second view of the same map, as copying a Java reference is.
///
/// Java's `Map` interface lets `unmodifiableMap` stand for a live map; Rust has
/// no read-only map trait in its standard library, and returning a `HashMap`
/// would copy the registry on every call.
#[derive(Clone, Default)]
#[non_exhaustive]
// Rust's form of the java.util.Map view the Java clients' public metrics() returns
#[doc(alias = "rust-only")]
pub struct MetricsView {
    map: SharedMetricMap,
}

impl MetricsView {
    /// A view of the registry map `map`.
    pub(crate) fn of(map: SharedMetricMap) -> Self {
        Self { map }
    }

    /// Returns the metric registered under `name`, if any, as the registry is
    /// at the time of the call.
    ///
    /// # Arguments
    ///
    /// - `name` - the name of the metric
    pub fn get(&self, name: &MetricName) -> Option<Arc<KafkaMetric>> {
        self.map.get(name)
    }

    /// Returns whether a metric is registered under `name`, as the registry is
    /// at the time of the call.
    ///
    /// # Arguments
    ///
    /// - `name` - the name of the metric
    pub fn contains_key(&self, name: &MetricName) -> bool {
        self.map.contains_key(name)
    }

    /// Returns how many metrics the registry holds at the time of the call.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns whether the registry holds no metric at the time of the call.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the map as it is now, indexed by metric name.
    ///
    /// The returned map never changes: a metric registered or removed later
    /// shows in the view, not in the snapshot. Taking it copies no entry and
    /// never blocks the registry; a later registration copies the map once
    /// while a snapshot of the old version is held.
    pub fn snapshot(&self) -> Arc<HashMap<MetricName, Arc<KafkaMetric>>> {
        self.map.current()
    }
}

impl From<HashMap<MetricName, Arc<KafkaMetric>>> for MetricsView {
    /// A view of `metrics`, tied to no registry.
    ///
    /// # Arguments
    ///
    /// - `metrics` - the metrics the view holds, indexed by metric name
    fn from(metrics: HashMap<MetricName, Arc<KafkaMetric>>) -> Self {
        let map = SharedMetricMap::default();
        map.replace(metrics);
        Self { map }
    }
}

impl std::fmt::Debug for MetricsView {
    // The names of the metrics, not their values.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snapshot = self.snapshot();
        f.debug_struct("MetricsView")
            .field("metrics", &snapshot.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::metrics::Metrics;
    use std::collections::BTreeMap;

    fn name(metrics: &Metrics, n: &str) -> MetricName {
        metrics.metric_name_with_description_tags(n, "grp", "", BTreeMap::new())
    }

    #[test]
    fn test_view_is_live() {
        let metrics = Metrics::new();
        let view = metrics.metrics();
        let before = view.len();
        let a = name(&metrics, "a");
        assert!(!view.contains_key(&a));
        metrics
            .add_metric_with_measurable(a.clone(), Box::new(crate::common::metrics::stats::Value::new()))
            .unwrap();
        assert!(view.contains_key(&a));
        assert!(view.get(&a).is_some());
        assert_eq!(before + 1, view.len());
        assert!(metrics.remove_metric(&a).is_some());
        assert!(!view.contains_key(&a));
        assert!(view.get(&a).is_none());
        assert_eq!(before, view.len());
    }

    #[test]
    fn test_snapshot_is_stable_and_copies_once() {
        let metrics = Metrics::new();
        let view = metrics.metrics();
        let held = view.snapshot();
        let a = name(&metrics, "a");
        let b = name(&metrics, "b");
        metrics
            .add_metric_with_measurable(a.clone(), Box::new(crate::common::metrics::stats::Value::new()))
            .unwrap();
        let after_a = view.snapshot();
        // The first write copied the map, since `held` still holds it.
        assert!(!Arc::ptr_eq(&held, &after_a));
        drop(after_a);
        let ptr_after_a = Arc::as_ptr(&view.snapshot());
        metrics
            .add_metric_with_measurable(b.clone(), Box::new(crate::common::metrics::stats::Value::new()))
            .unwrap();
        let after_b = view.snapshot();
        // No snapshot held the second version, so the second write was in place.
        assert_eq!(ptr_after_a, Arc::as_ptr(&after_b));
        assert!(!held.contains_key(&a) && !held.contains_key(&b));
        assert!(view.contains_key(&a) && view.contains_key(&b));
        assert_eq!(held.len() + 2, view.len());
    }

    #[test]
    fn test_standalone_views() {
        assert!(MetricsView::default().is_empty());
        let metrics = Metrics::new();
        let view = MetricsView::from(HashMap::clone(&metrics.metrics().snapshot()));
        assert_eq!(metrics.metrics().len(), view.len());
        // Not tied to the registry.
        metrics
            .add_metric_with_measurable(name(&metrics, "a"), Box::new(crate::common::metrics::stats::Value::new()))
            .unwrap();
        assert_eq!(metrics.metrics().len(), view.len() + 1);
        assert!(format!("{view:?}").contains("count"));
    }

    #[test]
    fn test_view_outlives_registry() {
        let metrics = Metrics::new();
        let view = metrics.metrics();
        let len = view.len();
        drop(metrics);
        assert_eq!(len, view.len());
    }
}

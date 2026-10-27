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

//! `kafka_common_metrics_MetricsView_*`: the C handle of the Rust-only
//! [`MetricsView`], the live read-only metrics map the clients' `metrics()`
//! return (Java's `Collections.unmodifiableMap(metrics.metrics())`).

use std::collections::hash_map;
use std::ffi::c_void;
use std::sync::Arc;

use crate::common::MetricName;
use crate::common::metrics::{KafkaMetric, MetricsView};
use crate::ffi::common::metric_name::{
    box_metric_name, destroy_metric_name_element, kafka_common_MetricName_t, metric_name_ref,
};
use crate::ffi::common::metrics::kafka_metric::{
    box_kafka_metric, destroy_kafka_metric_element, kafka_common_metrics_KafkaMetric_t,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_iterator, box_map_entry, destroy_map_entry_element, kafka_Iterator_t};

/// Opaque handle to a [`MetricsView`]: a live, read-only map from
/// `kafka_common_MetricName_t` to `kafka_common_metrics_KafkaMetric_t`.
///
/// Every call reads the registry as it is at the time of the call. The view
/// stays valid after its client is destroyed.
#[repr(C)]
// the C handle of the Rust-only `MetricsView` the public `metrics()` exports return
#[doc(alias = "rust-only")]
pub struct kafka_common_metrics_MetricsView_t {
    _private: [u8; 0],
}

/// Hands `view` to C as an owned handle, freed with
/// `kafka_common_metrics_MetricsView_destroy`.
pub(crate) fn box_metrics_view(view: MetricsView) -> *mut kafka_common_metrics_MetricsView_t {
    Box::into_raw(Box::new(view)) as *mut kafka_common_metrics_MetricsView_t
}

/// The view behind a handle.
///
/// # Safety
///
/// `self_` must be a valid view handle, alive for `'a`.
unsafe fn view_ref<'a>(self_: *const kafka_common_metrics_MetricsView_t) -> &'a MetricsView {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid view handle alive for `'a`, the
    // `Box::into_raw` of `box_metrics_view`; only `_destroy` takes it back and the view is used
    // through `&self` only, so the shared reference is sound for `'a`.
    unsafe { &*(self_ as *const MetricsView) }
}

/// The entries of one snapshot of the map, built one at a time as C asks for
/// them.
///
/// Self-referential: `iter` borrows the map `map` keeps alive.
struct SnapshotIter {
    // Declared first, so dropped first: it borrows from `*map`.
    iter: hash_map::Iter<'static, MetricName, Arc<KafkaMetric>>,
    map: Arc<std::collections::HashMap<MetricName, Arc<KafkaMetric>>>,
}

impl SnapshotIter {
    fn new(map: Arc<std::collections::HashMap<MetricName, Arc<KafkaMetric>>>) -> Self {
        // SAFETY: The map lives in the `Arc`'s heap allocation, which `map` keeps alive and which
        // does not move when the `Arc` (or `Self`) moves, so the reference stays valid as long as
        // `map` is held. `iter` is declared before `map`, so it is dropped first and never outlives
        // the map. The map is never mutated while this strong reference exists: the registry writes
        // only through `Arc::make_mut`, which copies a map another `Arc` still shares, and
        // `SharedMetricMap::replace` swaps the `Arc` rather than writing through it. So the shared
        // borrow the `'static` lifetime stands for holds for `iter`'s whole life.
        let map_ref: &'static std::collections::HashMap<MetricName, Arc<KafkaMetric>> = unsafe { &*Arc::as_ptr(&map) };
        Self { iter: map_ref.iter(), map }
    }
}

impl Iterator for SnapshotIter {
    type Item = *mut c_void;

    fn next(&mut self) -> Option<*mut c_void> {
        // `map` is read only through `iter`; naming it keeps the field's purpose explicit.
        let _ = &self.map;
        let (name, metric) = self.iter.next()?;
        Some(box_map_entry(
            box_metric_name(name.clone()) as *mut c_void,
            box_kafka_metric(Arc::clone(metric)) as *mut c_void,
            Some(destroy_metric_name_element),
            Some(destroy_kafka_metric_element),
        ))
    }
}

/// Returns the number of metrics the registry holds at the time of the call.
///
/// # Returns
///
/// the number of metrics
///
/// # Safety
///
/// `self_` must be a valid view handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsView_len(self_: *const kafka_common_metrics_MetricsView_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid view handle, as `view_ref`
    // requires; the reference is used only within this call.
    i32::try_from(unsafe { view_ref(self_) }.len()).unwrap_or(i32::MAX)
}

/// Returns `1` if the registry holds no metric at the time of the call, else
/// `0`.
///
/// # Returns
///
/// `1` if there is no metric
///
/// # Safety
///
/// `self_` must be a valid view handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsView_is_empty(
    self_: *const kafka_common_metrics_MetricsView_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid view handle, as `view_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { view_ref(self_) }.is_empty())
}

/// Returns `1` if a metric is registered under `name` at the time of the
/// call, else `0`.
///
/// # Parameters
///
/// - `name` - the name of the metric
///
/// # Returns
///
/// `1` if the metric is registered
///
/// # Safety
///
/// `self_` must be a valid view handle and `name` a valid metric-name handle,
/// both read during the call only.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsView_contains_key(
    self_: *const kafka_common_metrics_MetricsView_t,
    name: *const kafka_common_MetricName_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid view handle and `name` a valid
    // metric-name handle, as `view_ref` and `metric_name_ref` require; both are used only within
    // this call.
    i8::from(unsafe { view_ref(self_) }.contains_key(unsafe { metric_name_ref(name) }))
}

/// Returns the metric registered under `name` at the time of the call, or
/// `NULL` when none is.
///
/// # Parameters
///
/// - `name` - the name of the metric
///
/// # Returns
///
/// the metric, or `NULL`
///
/// # Safety
///
/// `self_` must be a valid view handle and `name` a valid metric-name handle,
/// both read during the call only. A returned metric is owned by the caller,
/// freed with `kafka_common_metrics_KafkaMetric_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsView_get(
    self_: *const kafka_common_metrics_MetricsView_t,
    name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid view handle and `name` a valid
    // metric-name handle, as `view_ref` and `metric_name_ref` require; both are used only within
    // this call.
    unsafe { view_ref(self_) }
        .get(unsafe { metric_name_ref(name) })
        .map_or(std::ptr::null_mut(), box_kafka_metric)
}

/// Returns an iterator over the map as it is now: a `kafka_Iterator_t` of
/// `kafka_Map_Entry_t *`, each with a `kafka_common_MetricName_t *` key and a
/// `kafka_common_metrics_KafkaMetric_t *` value, in no particular order.
///
/// The iterator never changes: a metric registered or removed later shows
/// in the view, not in it. Taking it copies no entry and never blocks the
/// registry.
///
/// # Returns
///
/// the iterator
///
/// # Safety
///
/// `self_` must be a valid view handle, read during the call only. The
/// returned iterator is owned by the caller, freed with
/// `kafka_Iterator_destroy`, and stays valid after the view is destroyed.
/// Each entry, with its key and value, is owned by the iterator: valid until
/// the next `kafka_Iterator_next` or `kafka_Iterator_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsView_iterator(
    self_: *const kafka_common_metrics_MetricsView_t,
) -> *mut kafka_Iterator_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid view handle, as `view_ref`
    // requires; the reference is used only within this call, and the iterator holds its own
    // `Arc` of the snapshot.
    let snapshot = unsafe { view_ref(self_) }.snapshot();
    box_iterator(Box::new(SnapshotIter::new(snapshot)), Some(destroy_map_entry_element))
}

/// Frees an owned view handle; null is a no-op. Iterators and metrics taken
/// from it stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned view handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsView_destroy(self_: *mut kafka_common_metrics_MetricsView_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // view handle not yet destroyed, the `Box::into_raw` of `box_metrics_view`, so `from_raw`
        // reclaims it once.
        drop(unsafe { Box::from_raw(self_ as *mut MetricsView) });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::Metric;
    use crate::common::metrics::{ClosureMeasurable, Metrics};
    use crate::ffi::common::metric_name::kafka_common_MetricName_destroy;
    use crate::ffi::common::metrics::kafka_metric::{kafka_common_metrics_KafkaMetric_destroy, kafka_metric_ref};
    use crate::ffi::util::{
        kafka_Iterator_destroy, kafka_Iterator_has_next, kafka_Iterator_next, kafka_Map_Entry_key, kafka_Map_Entry_t,
        kafka_Map_Entry_value,
    };

    fn add(metrics: &Metrics, name: &str) -> MetricName {
        let name = metrics.metric_name_with_tags(name, "grp", BTreeMap::new());
        metrics
            .add_metric_with_measurable(name.clone(), Box::new(ClosureMeasurable::new(|_, _| 0.0)))
            .unwrap();
        name
    }

    /// Collects the names an iterator over `view` hands out.
    ///
    /// # Safety
    ///
    /// `view` must be a valid view handle.
    unsafe fn iterated_names(view: *const kafka_common_metrics_MetricsView_t) -> Vec<MetricName> {
        let mut names = Vec::new();
        // SAFETY: Per this function's `# Safety`, `view` is a valid view handle; the iterator is
        // fresh, used by this thread only and destroyed once, and each entry is read before the
        // next `kafka_Iterator_next` frees it.
        unsafe {
            let iterator = kafka_common_metrics_MetricsView_iterator(view);
            while kafka_Iterator_has_next(iterator) == 1 {
                let entry = kafka_Iterator_next(iterator) as *const kafka_Map_Entry_t;
                let name = metric_name_ref(kafka_Map_Entry_key(entry) as *const kafka_common_MetricName_t);
                let metric =
                    kafka_metric_ref(kafka_Map_Entry_value(entry) as *const kafka_common_metrics_KafkaMetric_t);
                assert_eq!(metric.metric_name(), name, "an entry's value is the metric of its key");
                names.push(name.clone());
            }
            assert!(kafka_Iterator_next(iterator).is_null(), "past the end the guard returns NULL");
            kafka_Iterator_destroy(iterator);
        }
        names
    }

    #[test]
    fn a_view_is_live_and_its_iterator_is_not() {
        let metrics = Metrics::new();
        let a = add(&metrics, "a");
        let view = box_metrics_view(metrics.metrics());
        let name_a = box_metric_name(a.clone());
        // SAFETY: Test code: `view` and `name_a` are live handles, each destroyed once below; the
        // iterator is taken before `b` is added and destroyed after.
        unsafe {
            assert_eq!(kafka_common_metrics_MetricsView_len(view), 2, "`count` and `a`");
            assert_eq!(kafka_common_metrics_MetricsView_is_empty(view), 0);
            assert_eq!(kafka_common_metrics_MetricsView_contains_key(view, name_a), 1);
            let found = kafka_common_metrics_MetricsView_get(view, name_a);
            assert!(Arc::ptr_eq(kafka_metric_ref(found), &metrics.metric(&a).unwrap()));
            kafka_common_metrics_KafkaMetric_destroy(found);

            let held = kafka_common_metrics_MetricsView_iterator(view);
            let b = add(&metrics, "b");
            let name_b = box_metric_name(b.clone());
            assert_eq!(kafka_common_metrics_MetricsView_len(view), 3, "the view sees `b`");
            assert_eq!(kafka_common_metrics_MetricsView_contains_key(view, name_b), 1);
            let mut names = iterated_names(view);
            names.sort_by(|x, y| x.name().cmp(y.name()));
            assert_eq!(names.iter().map(MetricName::name).collect::<Vec<_>>(), ["a", "b", "count"]);
            let mut held_count = 0;
            while kafka_Iterator_has_next(held) == 1 {
                let entry = kafka_Iterator_next(held) as *const kafka_Map_Entry_t;
                let name = metric_name_ref(kafka_Map_Entry_key(entry) as *const kafka_common_MetricName_t);
                assert_ne!(name, &b, "the iterator taken before `b` never sees it");
                held_count += 1;
            }
            assert_eq!(held_count, 2);
            kafka_Iterator_destroy(held);

            metrics.remove_metric(&a);
            assert_eq!(kafka_common_metrics_MetricsView_contains_key(view, name_a), 0);
            assert!(kafka_common_metrics_MetricsView_get(view, name_a).is_null());
            kafka_common_MetricName_destroy(name_b);
            kafka_common_MetricName_destroy(name_a);
            kafka_common_metrics_MetricsView_destroy(view);
            kafka_common_metrics_MetricsView_destroy(std::ptr::null_mut());
        }
    }

    #[test]
    fn a_view_and_its_iterator_outlive_their_registry() {
        let metrics = Metrics::new();
        add(&metrics, "a");
        let view = box_metrics_view(metrics.metrics());
        // SAFETY: Test code: `view` is live until its destroy, the iterator until its own, which
        // comes after the view's.
        unsafe {
            let iterator = kafka_common_metrics_MetricsView_iterator(view);
            drop(metrics);
            assert_eq!(kafka_common_metrics_MetricsView_len(view), 2);
            kafka_common_metrics_MetricsView_destroy(view);
            assert_eq!(kafka_Iterator_has_next(iterator), 1);
            assert!(!kafka_Iterator_next(iterator).is_null());
            kafka_Iterator_destroy(iterator);
        }
    }

    #[test]
    fn an_empty_view() {
        let view = box_metrics_view(MetricsView::default());
        // SAFETY: Test code: `view` is live until its destroy.
        unsafe {
            assert_eq!(kafka_common_metrics_MetricsView_len(view), 0);
            assert_eq!(kafka_common_metrics_MetricsView_is_empty(view), 1);
            assert!(iterated_names(view).is_empty());
            kafka_common_metrics_MetricsView_destroy(view);
        }
    }
}

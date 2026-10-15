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

//! `kafka_common_metrics_*`: `org.apache.kafka.common.metrics` (CLAUDE.md §4).
//!
//! The package mixes classes (`Metrics`, `Sensor`, `KafkaMetric`,
//! `MetricConfig`, `Quota`, the `stats`) with interfaces (`Stat`,
//! `Measurable`, `MeasurableStat`, `CompoundStat`, `Gauge`,
//! `MetricsReporter`, the Rust-only `SampledStatKind`). Every interface
//! handle is an [`Interface`]: one C type stands both for an implementation
//! the C caller registered with the interface's `_new` and for the view a
//! class handle hands out through `__as_<Interface>` (an `Avg_t` seen as a
//! `Stat_t`) or a getter (`KafkaMetric_measurable`). The two differ only in
//! who owns them, see [`Interface::owned`].
//!
//! Where Java passes a `MetricConfig` to an interface method, Rust passes a
//! `kafka_common_metrics_MetricConfig_t` borrowed for the call; where an
//! interface method returns a value Java would own (a `MetricValue`, a list
//! of `NamedMeasurable`s), the C side builds it with the matching `_new` and
//! the Rust side takes it over.
//!
//! A standalone `Metrics_t` has no background task: every
//! `MetricsReporter` callback runs synchronously on the thread calling the
//! registry (`add_reporter`, `add_metric_*`, `remove_metric`, `close`).

use std::collections::HashMap;
use std::ffi::{CStr, c_char, c_void};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use crate::common::MetricName;
use crate::common::metrics::KafkaMetric;
use crate::ffi::common::metric_name::{box_metric_name, destroy_metric_name_element, metric_name_eq};
use crate::ffi::common::metrics::kafka_metric::{box_kafka_metric, destroy_kafka_metric_element};
use crate::ffi::util::{box_map, kafka_Map_t, owned_c_string};

pub(crate) mod compound_stat;
pub(crate) mod gauge;
pub(crate) mod kafka_metric;
pub(crate) mod measurable;
pub(crate) mod measurable_stat;
pub(crate) mod metric_config;
pub(crate) mod metric_value_provider;
// Java's `org.apache.kafka.common.metrics.Metrics`, as `common::metrics::metrics`.
#[expect(clippy::module_inception)]
pub(crate) mod metrics;
pub(crate) mod metrics_reporter;
pub(crate) mod quota;
pub(crate) mod sensor;
pub(crate) mod stat;
pub(crate) mod stats;
pub(crate) mod time_unit;

/// The C function behind a `type_name` slot of `Gauge_new`, `Measurable_new`
/// and `MeasurableStat_new`: a NUL-terminated name that stays valid and
/// unchanged for the rest of the program (a string literal), or `NULL` for
/// none.
pub(crate) type TypeNameFn = unsafe extern "C" fn(self_: *mut c_void) -> *const c_char;

/// What a C implementation reports for the Rust-only `type_name`: `None`
/// when the slot is `NULL` (a C function has no type, like a closure, which
/// `KafkaMetric`'s `Display` omits), when it returns `NULL`, or when the
/// name is not UTF-8.
///
/// # Safety
///
/// `slot` and `self_` must be a pair registered together through an
/// interface's `_new`, whose rustdoc has the C function return `NULL` or a
/// string valid and unchanged for the rest of the program, and allows the
/// call from any thread.
pub(crate) unsafe fn c_type_name(slot: Option<TypeNameFn>, self_: *mut c_void) -> Option<&'static str> {
    let type_name = slot?;
    // SAFETY: Per this function's `# Safety`, `type_name` and `self_` were registered together
    // through an interface's `_new`, which allows this call from any thread.
    let name = unsafe { type_name(self_) };
    if name.is_null() {
        return None;
    }
    // SAFETY: Per this function's `# Safety`, a non-null `name` is NUL-terminated and stays valid
    // and unchanged for the rest of the program, so the `'static` borrow holds.
    let name: &'static CStr = unsafe { CStr::from_ptr(name) };
    name.to_str().ok()
}

/// `type_name` as C sees it: `NULL` for `None`, else a NUL-terminated copy
/// that stays valid for the rest of the program and is never freed by the
/// caller. Each distinct name is copied once and kept; the names are type
/// names, so their number is bounded by the provider types in the program.
pub(crate) fn type_name_c_str(name: Option<&'static str>) -> *const c_char {
    static NAMES: OnceLock<Mutex<HashMap<&'static str, &'static CStr>>> = OnceLock::new();
    let Some(name) = name else {
        return std::ptr::null();
    };
    let mut names = NAMES.get_or_init(Mutex::default).lock().unwrap_or_else(PoisonError::into_inner);
    names
        .entry(name)
        .or_insert_with(|| Box::leak(owned_c_string(name).into_boxed_c_str()))
        .as_ptr()
}

/// What an interface handle points at: the implementation, shared with
/// whoever else holds it, and who owns the handle.
pub(crate) struct Interface<T: ?Sized> {
    imp: Arc<T>,
    /// `true` for a handle the C caller owns: built by the interface's
    /// `_new`, or returned owned (`*mut`) by a getter. A `*mut` parameter
    /// consumes it (the handle is freed, the implementation moves to Rust)
    /// and `_destroy` frees it.
    ///
    /// `false` for the view a class handle caches (`__as_<Interface>`,
    /// `KafkaMetric_measurable`): it lives as long as the class handle, a
    /// `*mut` parameter shares its implementation and leaves it in place,
    /// and it is never passed to `_destroy`.
    owned: bool,
}

impl<T: ?Sized> Interface<T> {
    /// A handle the C caller owns.
    pub(crate) fn owned(imp: Arc<T>) -> *mut Self {
        Box::into_raw(Box::new(Self { imp, owned: true }))
    }

    /// The view a class handle caches.
    pub(crate) fn view(imp: Arc<T>) -> Self {
        Self { imp, owned: false }
    }

    /// The implementation.
    pub(crate) fn get(&self) -> &T {
        &self.imp
    }

    /// The handle behind a pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this interface, alive for `'a`.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        // SAFETY: Per this function's `# Safety`, `ptr` is a valid handle of this interface alive
        // for `'a`: an owned one, the `Box::into_raw` of `Interface::owned`, or a view a class
        // handle caches, which lives as long as that class handle. An `Interface` is never mutated
        // once built (only `take` and `destroy` free an owned one), so the shared reference is
        // sound for `'a`.
        unsafe { &*ptr }
    }

    /// Takes the implementation a `*mut` parameter received: an owned
    /// handle is freed and its implementation moved out, a view shares its
    /// implementation and stays with its class handle.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this interface, not used again by the
    /// caller when it was owned.
    pub(crate) unsafe fn take(ptr: *mut Self) -> Arc<T> {
        // SAFETY: Per this function's `# Safety`, `ptr` is a valid handle of this interface, owned
        // or a view; the reference only reads `owned` and ends with this expression.
        if unsafe { &*ptr }.owned {
            // SAFETY: `owned` is set only by `Interface::owned`, so `ptr` is the `Box::into_raw` of
            // that function, and per this function's `# Safety` the caller does not use an owned
            // handle again, so `from_raw` reclaims that allocation, once.
            unsafe { Box::from_raw(ptr) }.imp
        } else {
            // SAFETY: Per this function's `# Safety`, `ptr` is a valid handle of this interface,
            // here a view, which its class handle keeps alive and which is not freed here; the
            // reference lasts only while the `Arc` is cloned.
            Arc::clone(&unsafe { &*ptr }.imp)
        }
    }

    /// Frees an owned handle; null is a no-op. A view is never passed here.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle not yet destroyed.
    pub(crate) unsafe fn destroy(ptr: *mut Self) {
        if !ptr.is_null() {
            // SAFETY: `ptr` is non-null (checked above) and, per this function's `# Safety`, an
            // owned handle not yet destroyed, i.e. the `Box::into_raw` of `Interface::owned` (a
            // view, which its class handle caches, is never passed here), so `from_raw` reclaims
            // that allocation, once.
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

/// The C shape of a `metrics()` snapshot (`Producer.metrics()`,
/// `Consumer.metrics()`, `Metrics.metrics()`): an owned `kafka_Map_t` of owned
/// `kafka_common_MetricName_t *` to owned `kafka_common_metrics_KafkaMetric_t
/// *`, names compared by value in `kafka_Map_get`, freed with
/// `kafka_Map_destroy`, which frees both.
///
/// Java returns a `HashMap`, which has no order; the entries are sorted by
/// group, name and tags, the whole `MetricName`, so the snapshot is
/// reproducible (per-node and per-topic metrics share group and name and
/// differ only in their tags).
pub(crate) fn box_metrics_map(metrics: HashMap<MetricName, Arc<KafkaMetric>>) -> *mut kafka_Map_t {
    let mut metrics: Vec<_> = metrics.into_iter().collect();
    metrics.sort_by(|(a, _), (b, _)| (a.group(), a.name(), a.tags()).cmp(&(b.group(), b.name(), b.tags())));
    let entries = metrics
        .into_iter()
        .map(|(name, metric)| (box_metric_name(name) as *mut c_void, box_kafka_metric(metric) as *mut c_void))
        .collect();
    box_map(
        entries,
        Some(destroy_metric_name_element),
        Some(destroy_kafka_metric_element),
        Some(metric_name_eq),
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::common::metrics::{ClosureMeasurable, Metrics};
    use crate::ffi::common::metric_name::{kafka_common_MetricName_t, metric_name_ref};
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_key, kafka_Map_size};

    /// Metrics that share group and name, as per-node ones do, are ordered by
    /// their tags too.
    #[test]
    fn a_metrics_snapshot_is_ordered_by_group_name_and_tags() {
        let metrics = Metrics::new();
        let mut expected = Vec::new();
        for (group, name) in [("g", "b"), ("g", "a"), ("f", "z")] {
            for node in (0..20).rev() {
                let tags = BTreeMap::from([("node-id".to_string(), format!("node-{node:02}"))]);
                let metric_name = metrics.metric_name_with_tags(name, group, tags.clone());
                metrics
                    .add_metric_with_measurable(metric_name, Box::new(ClosureMeasurable::new(|_, _| 0.0)))
                    .unwrap();
                expected.push((group.to_string(), name.to_string(), tags));
            }
        }
        expected.sort();
        let map = box_metrics_map(metrics.metrics());
        // SAFETY: Test code: `map` is live until its destroy below, and each key is a metric-name
        // handle it owns, read before the destroy.
        let keys: Vec<_> = unsafe {
            let keys = (0..kafka_Map_size(map))
                .map(|i| metric_name_ref(kafka_Map_key(map, i) as *const kafka_common_MetricName_t))
                .filter(|name| name.group() == "f" || name.group() == "g")
                .map(|name| (name.group().to_string(), name.name().to_string(), name.tags().clone()))
                .collect();
            kafka_Map_destroy(map);
            keys
        };
        assert_eq!(keys, expected);
    }

    #[test]
    fn owned_handles_are_consumed_and_views_are_shared() {
        let imp: Arc<str> = Arc::from("imp");
        let owned = Interface::owned(Arc::clone(&imp));
        assert_eq!(Arc::strong_count(&imp), 2);
        // SAFETY: Test code: `owned` comes from `Interface::owned` and is not used after `take`
        // frees it.
        let taken = unsafe { Interface::take(owned) };
        assert!(Arc::ptr_eq(&taken, &imp));
        assert_eq!(Arc::strong_count(&imp), 2, "the owned handle was freed");
        drop(taken);

        let mut view = Interface::view(Arc::clone(&imp));
        assert_eq!(view.get(), "imp");
        // SAFETY: Test code: `view` is a live local view, which `take` only reads and leaves in
        // place.
        let shared = unsafe { Interface::take(&mut view) };
        assert!(Arc::ptr_eq(&shared, &imp));
        assert_eq!(Arc::strong_count(&imp), 3, "the view keeps its own reference");
        drop(view);
        drop(shared);
        assert_eq!(Arc::strong_count(&imp), 1);
        // SAFETY: Test code: null is a no-op for `destroy`.
        unsafe { Interface::<str>::destroy(std::ptr::null_mut()) };
    }
}

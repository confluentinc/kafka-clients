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

use std::sync::Arc;

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
    /// `ptr` must be a valid handle of this interface.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
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
        if unsafe { &*ptr }.owned {
            unsafe { Box::from_raw(ptr) }.imp
        } else {
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
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_handles_are_consumed_and_views_are_shared() {
        let imp: Arc<str> = Arc::from("imp");
        let owned = Interface::owned(Arc::clone(&imp));
        assert_eq!(Arc::strong_count(&imp), 2);
        let taken = unsafe { Interface::take(owned) };
        assert!(Arc::ptr_eq(&taken, &imp));
        assert_eq!(Arc::strong_count(&imp), 2, "the owned handle was freed");
        drop(taken);

        let mut view = Interface::view(Arc::clone(&imp));
        assert_eq!(view.get(), "imp");
        let shared = unsafe { Interface::take(&mut view) };
        assert!(Arc::ptr_eq(&shared, &imp));
        assert_eq!(Arc::strong_count(&imp), 3, "the view keeps its own reference");
        drop(view);
        drop(shared);
        assert_eq!(Arc::strong_count(&imp), 1);
        unsafe { Interface::<str>::destroy(std::ptr::null_mut()) };
    }
}

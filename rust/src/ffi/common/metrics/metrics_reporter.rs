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

//! `kafka_common_metrics_MetricsReporter_t`: the
//! `org.apache.kafka.common.metrics.MetricsReporter` interface (CLAUDE.md
//! §4, "Traits").
//!
//! Every method has a Java default (a no-op), so every pointer passed to
//! [`kafka_common_metrics_MetricsReporter_new`] is nullable, `NULL` meaning
//! that default. A standalone `Metrics_t` has no background task: the
//! callbacks run synchronously on the thread calling the registry
//! (`Metrics_add_reporter` calls `init` before returning, `Metrics_close`
//! calls `close`). The metric handles a callback receives are borrowed for
//! the call.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::metrics::{KafkaMetric, MetricsReporter};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::kafka_metric::{
    KafkaMetricInner, kafka_common_metrics_KafkaMetric_t, kafka_metric_ref,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, kafka_List_destroy, kafka_List_t, list_elements};

/// Opaque handle to a [`MetricsReporter`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_MetricsReporter_t {
    _private: [u8; 0],
}

/// `init(List<KafkaMetric> metrics)` of a C implementation: `metrics` holds
/// `const kafka_common_metrics_KafkaMetric_t *` elements borrowed for the
/// call.
pub type kafka_common_metrics_MetricsReporter_init_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, metrics: *const kafka_List_t)>;

/// `metricChange(KafkaMetric metric)` of a C implementation: `metric` is
/// borrowed for the call.
pub type kafka_common_metrics_MetricsReporter_metric_change_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, metric: *const kafka_common_metrics_KafkaMetric_t)>;

/// `metricRemoval(KafkaMetric metric)` of a C implementation: `metric` is
/// borrowed for the call.
pub type kafka_common_metrics_MetricsReporter_metric_removal_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, metric: *const kafka_common_metrics_KafkaMetric_t)>;

/// `close()` of a C implementation.
pub type kafka_common_metrics_MetricsReporter_close_fn_t = Option<unsafe extern "C" fn(self_: *mut c_void)>;

/// A C implementation of [`MetricsReporter`] registered through
/// [`kafka_common_metrics_MetricsReporter_new`]; a null method is the Java
/// default. Every method of the Java interface has a default, so each
/// typedef is `Option<..>`: the nullable form cbindgen emits as a plain C
/// function pointer, while an `Option<alias>` in the exported signature
/// would reach the header verbatim.
struct CMetricsReporter {
    self_: *mut c_void,
    init: kafka_common_metrics_MetricsReporter_init_fn_t,
    metric_change: kafka_common_metrics_MetricsReporter_metric_change_fn_t,
    metric_removal: kafka_common_metrics_MetricsReporter_metric_removal_fn_t,
    close: kafka_common_metrics_MetricsReporter_close_fn_t,
}

// SAFETY: `self_` and the nullable function pointers are what the C caller registered through
// `kafka_common_metrics_MetricsReporter_new`: the C user is responsible for the thread-safety of
// `self_`, which that function's rustdoc states, and the nullable function pointers are trivially
// shareable. `CMetricsReporter` itself never dereferences `self_`; it only passes it back to the
// functions registered with it.
unsafe impl Send for CMetricsReporter {}
// SAFETY: `Sync` is required by the `MetricsReporter` trait, whose implementations are shared
// behind an `Arc` and called through `&self` from any thread. A `&CMetricsReporter` exposes only
// the opaque `self_` and the nullable function pointers, which the type never dereferences;
// `kafka_common_metrics_MetricsReporter_new`'s rustdoc lets them be called from any thread,
// concurrently, and the C user is responsible for the thread-safety of `self_` under those calls.
unsafe impl Sync for CMetricsReporter {}

impl MetricsReporter for CMetricsReporter {
    fn init(&self, metrics: &[Arc<KafkaMetric>]) {
        let Some(init) = self.init else { return };
        // Handles borrowed for the call: freed once C returns.
        let handles: Vec<KafkaMetricInner> =
            metrics.iter().map(|metric| KafkaMetricInner::new(Arc::clone(metric))).collect();
        let list = box_list(handles.iter().map(|handle| handle.as_ptr() as *mut c_void).collect(), None);
        // SAFETY: `init` is the non-null function the C caller registered together with
        // `self.self_` through `kafka_common_metrics_MetricsReporter_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle is destroyed or, once `Metrics_add_reporter`
        // consumed it, until that registry is, which is after this `&self` call, and allows the
        // call to come from any thread. `list` is the view list `box_list` built over the `handles`
        // local without an element destructor, and both live until after the call, so the metric
        // handles are borrowed for the call, as the function type's rustdoc states.
        unsafe { init(self.self_, list) };
        // SAFETY: `list` is the list `box_list` returned above, not yet destroyed and given no
        // element destructor, so this frees the list alone; the `KafkaMetricInner`s it pointed at
        // are dropped with `handles`.
        unsafe { kafka_List_destroy(list) };
    }

    fn metric_change(&self, metric: &Arc<KafkaMetric>) {
        let Some(metric_change) = self.metric_change else {
            return;
        };
        let handle = KafkaMetricInner::new(Arc::clone(metric));
        // SAFETY: `metric_change` is the non-null function the C caller registered together with
        // `self.self_` through `kafka_common_metrics_MetricsReporter_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle is destroyed or, once `Metrics_add_reporter`
        // consumed it, until that registry is, which is after this `&self` call, and allows the
        // call to come from any thread. `handle` is a local that outlives the call, so the metric
        // handle is borrowed for the call, as the function type's rustdoc states.
        unsafe { metric_change(self.self_, handle.as_ptr()) };
    }

    fn metric_removal(&self, metric: &Arc<KafkaMetric>) {
        let Some(metric_removal) = self.metric_removal else {
            return;
        };
        let handle = KafkaMetricInner::new(Arc::clone(metric));
        // SAFETY: `metric_removal` is the non-null function the C caller registered together with
        // `self.self_` through `kafka_common_metrics_MetricsReporter_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle is destroyed or, once `Metrics_add_reporter`
        // consumed it, until that registry is, which is after this `&self` call, and allows the
        // call to come from any thread. `handle` is a local that outlives the call, so the metric
        // handle is borrowed for the call, as the function type's rustdoc states.
        unsafe { metric_removal(self.self_, handle.as_ptr()) };
    }

    fn close(&self) {
        let Some(close) = self.close else { return };
        // SAFETY: `close` is the non-null function the C caller registered together with
        // `self.self_` through `kafka_common_metrics_MetricsReporter_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle is destroyed or, once `Metrics_add_reporter`
        // consumed it, until that registry is, which is after this `&self` call, and allows the
        // call to come from any thread. It takes no other argument.
        unsafe { close(self.self_) };
    }
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `reporter` must be a valid metrics-reporter handle, alive for `'a`.
pub(crate) unsafe fn metrics_reporter_ref<'a>(
    reporter: *const kafka_common_metrics_MetricsReporter_t,
) -> &'a dyn MetricsReporter {
    // SAFETY: Per this function's `# Safety`, `reporter` is a valid metrics-reporter handle alive
    // for `'a`, as `Interface::from_ptr` requires: every `kafka_common_metrics_MetricsReporter_t`
    // is an `Interface<dyn MetricsReporter>`, owned or a view; the reference returned lives for the
    // same `'a`.
    unsafe { Interface::<dyn MetricsReporter>::from_ptr(reporter as *const Interface<dyn MetricsReporter>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]).
///
/// # Safety
///
/// `reporter` must be a valid metrics-reporter handle, not used again by
/// the caller when it was owned.
pub(crate) unsafe fn take_metrics_reporter(
    reporter: *mut kafka_common_metrics_MetricsReporter_t,
) -> Arc<dyn MetricsReporter> {
    // SAFETY: Per this function's `# Safety`, `reporter` is a valid metrics-reporter handle, not
    // used again by the caller when it was owned, as `Interface::take` requires; every such handle
    // is an `Interface` of this trait object.
    unsafe { Interface::take(reporter as *mut Interface<dyn MetricsReporter>) }
}

/// Registers a C implementation of `MetricsReporter`; each method pointer
/// may be `NULL` for the Java default (a no-op). The caller owns `self_` and
/// keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_MetricsReporter_destroy`] or, once
/// `Metrics_add_reporter` consumed it, until that registry is destroyed.
/// The methods run on whichever thread calls the registry, possibly
/// concurrently, so the C caller is responsible for their thread-safety on
/// `self_`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_MetricsReporter_new(
    self_: *mut c_void,
    init: kafka_common_metrics_MetricsReporter_init_fn_t,
    metric_change: kafka_common_metrics_MetricsReporter_metric_change_fn_t,
    metric_removal: kafka_common_metrics_MetricsReporter_metric_removal_fn_t,
    close: kafka_common_metrics_MetricsReporter_close_fn_t,
) -> *mut kafka_common_metrics_MetricsReporter_t {
    let reporter: Arc<dyn MetricsReporter> =
        Arc::new(CMetricsReporter { self_, init, metric_change, metric_removal, close });
    Interface::owned(reporter) as *mut kafka_common_metrics_MetricsReporter_t
}

/// `init(List<KafkaMetric> metrics)`: `metrics` holds
/// `const kafka_common_metrics_KafkaMetric_t *` elements, borrowed.
///
/// # Safety
///
/// `self_` must be a valid metrics-reporter handle and `metrics` null or a
/// valid list of metric handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsReporter_init(
    self_: *const kafka_common_metrics_MetricsReporter_t,
    metrics: *const kafka_List_t,
) {
    // SAFETY: Per this function's `# Safety`, `metrics` is null or a valid list, which the caller
    // keeps alive and does not mutate for this call, as `list_elements` requires; the slice is used
    // only within this call.
    let metrics: Vec<Arc<KafkaMetric>> = unsafe { list_elements(metrics) }
        .iter()
        // SAFETY: Per this function's `# Safety`, each element of `metrics` is a valid metric
        // handle, as `kafka_metric_ref` requires; the `Arc` is cloned, so the borrow lasts for this
        // call only.
        .map(|&element| Arc::clone(unsafe { kafka_metric_ref(element as *const kafka_common_metrics_KafkaMetric_t) }))
        .collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metrics-reporter handle, as
    // `metrics_reporter_ref` requires; the reference is used only within this call.
    unsafe { metrics_reporter_ref(self_) }.init(&metrics);
}

/// `metricChange(KafkaMetric metric)`.
///
/// # Safety
///
/// `self_` must be a valid metrics-reporter handle and `metric` a valid
/// metric handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsReporter_metric_change(
    self_: *const kafka_common_metrics_MetricsReporter_t,
    metric: *const kafka_common_metrics_KafkaMetric_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metrics-reporter handle, as
    // `metrics_reporter_ref` requires, and `metric` a valid metric handle, as `kafka_metric_ref`
    // requires; both references are used only within this call.
    unsafe { metrics_reporter_ref(self_) }.metric_change(unsafe { kafka_metric_ref(metric) });
}

/// `metricRemoval(KafkaMetric metric)`.
///
/// # Safety
///
/// `self_` must be a valid metrics-reporter handle and `metric` a valid
/// metric handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsReporter_metric_removal(
    self_: *const kafka_common_metrics_MetricsReporter_t,
    metric: *const kafka_common_metrics_KafkaMetric_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metrics-reporter handle, as
    // `metrics_reporter_ref` requires, and `metric` a valid metric handle, as `kafka_metric_ref`
    // requires; both references are used only within this call.
    unsafe { metrics_reporter_ref(self_) }.metric_removal(unsafe { kafka_metric_ref(metric) });
}

/// `close()`.
///
/// # Safety
///
/// `self_` must be a valid metrics-reporter handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsReporter_close(
    self_: *const kafka_common_metrics_MetricsReporter_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metrics-reporter handle, as
    // `metrics_reporter_ref` requires; the reference is used only within this call.
    unsafe { metrics_reporter_ref(self_) }.close();
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned metrics-reporter handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricsReporter_destroy(
    self_: *mut kafka_common_metrics_MetricsReporter_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned metrics-reporter handle
    // not yet destroyed, as `Interface::destroy` requires; a view a class handle caches is never
    // passed here.
    unsafe { Interface::<dyn MetricsReporter>::destroy(self_ as *mut Interface<dyn MetricsReporter>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;
    use std::sync::Mutex;

    use super::*;
    use crate::common::metrics::stats::Avg;
    use crate::common::metrics::{Metrics, MetricsReporter};
    use crate::ffi::common::metric::kafka_common_Metric_metric_name;
    use crate::ffi::common::metric_name::kafka_common_MetricName_name;
    use crate::ffi::common::metrics::kafka_metric::{
        box_kafka_metric, kafka_common_metrics_KafkaMetric__as_Metric, kafka_common_metrics_KafkaMetric_destroy,
    };
    use crate::ffi::util::{kafka_List_get, kafka_List_size};

    // A C reporter logging the names of the metrics it sees.
    #[derive(Default)]
    struct Log {
        events: Mutex<Vec<String>>,
    }
    fn log<'a>(self_: *mut c_void) -> &'a Log {
        // SAFETY: Test code: `self_` is the `log` local the test registered, which outlives every
        // callback.
        unsafe { &*(self_ as *const Log) }
    }
    fn name_of(metric: *const kafka_common_metrics_KafkaMetric_t) -> String {
        // SAFETY: Test code: `metric` is the handle a callback received, valid for the callback;
        // the views read from it are used before it returns.
        unsafe {
            let name = kafka_common_Metric_metric_name(kafka_common_metrics_KafkaMetric__as_Metric(metric));
            CStr::from_ptr(kafka_common_MetricName_name(name)).to_str().unwrap().to_string()
        }
    }
    unsafe extern "C" fn c_init(self_: *mut c_void, metrics: *const kafka_List_t) {
        // The registry hands over every existing metric, its built-in `count`
        // included, in map order: sort to make the log deterministic.
        // SAFETY: Test code: `metrics` is the list the callback received, valid for the callback.
        let mut names: Vec<String> = (0..unsafe { kafka_List_size(metrics) })
            // SAFETY: Test code: `i` is below the list's size, and the element read is valid for
            // the callback.
            .map(|i| name_of(unsafe { kafka_List_get(metrics, i) } as *const kafka_common_metrics_KafkaMetric_t))
            .collect();
        names.sort();
        log(self_).events.lock().unwrap().push(format!("init {}", names.join(",")));
    }
    unsafe extern "C" fn c_change(self_: *mut c_void, metric: *const kafka_common_metrics_KafkaMetric_t) {
        log(self_).events.lock().unwrap().push(format!("change {}", name_of(metric)));
    }
    unsafe extern "C" fn c_removal(self_: *mut c_void, metric: *const kafka_common_metrics_KafkaMetric_t) {
        log(self_).events.lock().unwrap().push(format!("removal {}", name_of(metric)));
    }
    unsafe extern "C" fn c_close(self_: *mut c_void) {
        log(self_).events.lock().unwrap().push("close".to_string());
    }

    #[test]
    fn c_reporter_sees_registry_events_synchronously() {
        let log = Log::default();
        let handle = kafka_common_metrics_MetricsReporter_new(
            &log as *const Log as *mut c_void,
            Some(c_init),
            Some(c_change),
            Some(c_removal),
            Some(c_close),
        );
        let metrics = Metrics::new();
        let sensor = metrics.sensor("s").unwrap();
        sensor
            .add_with_metric_name(metrics.metric_name("a", "g"), Box::new(Avg::new()))
            .unwrap();
        // Consumed by the registry: `init` runs before `add_reporter` returns.
        // SAFETY: Test code: `handle` comes from `kafka_common_metrics_MetricsReporter_new` over
        // the `log` local, which outlives the registry, and `take_metrics_reporter` consumes it.
        metrics.add_reporter(unsafe { take_metrics_reporter(handle) });
        sensor
            .add_with_metric_name(metrics.metric_name("b", "g"), Box::new(Avg::new()))
            .unwrap();
        metrics.remove_metric(&metrics.metric_name("a", "g"));
        metrics.close();
        assert_eq!(
            *log.events.lock().unwrap(),
            ["init a,count", "change b", "removal a", "close"].map(str::to_string)
        );
    }

    #[test]
    fn null_methods_are_the_java_default_and_invokers_reach_a_rust_reporter() {
        let silent = kafka_common_metrics_MetricsReporter_new(ptr::null_mut(), None, None, None, None);
        let metrics = Metrics::new();
        let metric =
            box_kafka_metric(metrics.add_metric_if_absent(metrics.metric_name("m", "g"), None, Box::new(Avg::new())));
        // SAFETY: Test code: `silent` and `handle` are owned reporter handles, each destroyed once,
        // at the end of its use; `list` is a view list over `metric`, an owned metric handle; both
        // are destroyed once, after the last call reading them.
        unsafe {
            // No-ops: nothing to observe, nothing crashes.
            let list = box_list(vec![metric as *mut c_void], None);
            kafka_common_metrics_MetricsReporter_init(silent, list);
            kafka_common_metrics_MetricsReporter_init(silent, ptr::null());
            kafka_common_metrics_MetricsReporter_metric_change(silent, metric);
            kafka_common_metrics_MetricsReporter_metric_removal(silent, metric);
            kafka_common_metrics_MetricsReporter_close(silent);
            kafka_common_metrics_MetricsReporter_destroy(silent);

            // A Rust reporter driven through the invokers.
            #[derive(Default)]
            struct Counting(Mutex<Vec<&'static str>>);
            impl MetricsReporter for Counting {
                fn init(&self, metrics: &[Arc<KafkaMetric>]) {
                    assert_eq!(metrics.len(), 1);
                    self.0.lock().unwrap().push("init");
                }
                fn metric_change(&self, _metric: &Arc<KafkaMetric>) {
                    self.0.lock().unwrap().push("change");
                }
                fn metric_removal(&self, _metric: &Arc<KafkaMetric>) {
                    self.0.lock().unwrap().push("removal");
                }
                fn close(&self) {
                    self.0.lock().unwrap().push("close");
                }
            }
            let counting = Arc::new(Counting::default());
            let reporter: Arc<dyn MetricsReporter> = Arc::clone(&counting) as Arc<dyn MetricsReporter>;
            let handle = Interface::owned(reporter) as *mut kafka_common_metrics_MetricsReporter_t;
            kafka_common_metrics_MetricsReporter_init(handle, list);
            kafka_common_metrics_MetricsReporter_metric_change(handle, metric);
            kafka_common_metrics_MetricsReporter_metric_removal(handle, metric);
            kafka_common_metrics_MetricsReporter_close(handle);
            assert_eq!(*counting.0.lock().unwrap(), ["init", "change", "removal", "close"]);
            kafka_common_metrics_MetricsReporter_destroy(handle);
            kafka_common_metrics_MetricsReporter_destroy(ptr::null_mut());
            kafka_List_destroy(list);
            kafka_common_metrics_KafkaMetric_destroy(metric);
        }
    }
}

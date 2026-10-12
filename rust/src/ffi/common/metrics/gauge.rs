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

//! `kafka_common_metrics_Gauge_t`: the `org.apache.kafka.common.metrics.Gauge`
//! interface (CLAUDE.md §4, "Traits").
//!
//! Java's `Gauge<T>` produces a `T`; the Rust gauge produces the type-erased
//! `MetricValue`, so a C implementation builds its reading with one of the
//! `kafka_common_MetricValue_*` constructors and Rust takes it over.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::MetricValue;
use crate::common::metrics::{Gauge, MetricConfig};
use crate::ffi::common::metric::{box_metric_value, kafka_common_MetricValue_t, take_metric_value};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::ffi_guard;

/// Opaque handle to a [`Gauge`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_Gauge_t {
    _private: [u8; 0],
}

/// `value(MetricConfig config, long now)` of a C implementation: `config` is
/// borrowed for the call; the returned reading (never null) is owned and
/// taken over by Rust.
pub type kafka_common_metrics_Gauge_value_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> *mut kafka_common_MetricValue_t;

/// A C implementation of [`Gauge`] registered through
/// [`kafka_common_metrics_Gauge_new`].
struct CGauge {
    self_: *mut c_void,
    value: kafka_common_metrics_Gauge_value_fn_t,
}

// SAFETY: `self_` and the function pointer are what the C caller registered through
// `kafka_common_metrics_Gauge_new`: the C user is responsible for the thread-safety of `self_`,
// which that function's rustdoc states, and the function pointer is trivially shareable. `CGauge`
// itself never dereferences `self_`; it only passes it back to the function registered with it.
unsafe impl Send for CGauge {}
// SAFETY: `Sync` is required by the `Gauge` trait, whose implementations are shared behind an `Arc`
// and called through `&self` from any thread. A `&CGauge` exposes only the opaque `self_` and the
// function pointer, which the type never dereferences; `kafka_common_metrics_Gauge_new`'s rustdoc
// lets it be called from any thread, concurrently, and the C user is responsible for the
// thread-safety of `self_` under those calls.
unsafe impl Sync for CGauge {}

impl Gauge for CGauge {
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.value` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_Gauge_new`, whose rustdoc has the caller keep `self_` alive
        // until the handle is destroyed or, once consumed, until the registry it went into is,
        // which is after this `&self` call, and allows the call to come from any thread,
        // concurrently with others. `config` is the handle `MetricConfigInner::borrowed` made from
        // the `&MetricConfig` this method holds, valid for the call, which is all the function
        // type's rustdoc promises the C side. The function type's rustdoc has the C function return
        // an owned, non-null metric-value handle, which `take_metric_value` takes over, once.
        unsafe { take_metric_value((self.value)(self.self_, config.as_ptr(), now)) }
    }
}

/// A shared [`Gauge`] seen as the boxed one the Rust API takes.
pub(crate) struct ArcGauge(pub(crate) Arc<dyn Gauge>);

impl Gauge for ArcGauge {
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        self.0.value(config, now)
    }
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `gauge` must be a valid gauge handle, alive for `'a`.
pub(crate) unsafe fn gauge_ref<'a>(gauge: *const kafka_common_metrics_Gauge_t) -> &'a dyn Gauge {
    // SAFETY: Per this function's `# Safety`, `gauge` is a valid gauge handle alive for `'a`, as
    // `Interface::from_ptr` requires: every `kafka_common_metrics_Gauge_t` is an
    // `Interface<dyn Gauge>`, owned or a view; the reference returned lives for the same `'a`.
    unsafe { Interface::<dyn Gauge>::from_ptr(gauge as *const Interface<dyn Gauge>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]).
///
/// # Safety
///
/// `gauge` must be a valid gauge handle, not used again by the caller when
/// it was owned.
pub(crate) unsafe fn take_gauge(gauge: *mut kafka_common_metrics_Gauge_t) -> Arc<dyn Gauge> {
    // SAFETY: Per this function's `# Safety`, `gauge` is a valid gauge handle, not used again by
    // the caller when it was owned, as `Interface::take` requires; every such handle is an
    // `Interface` of this trait object.
    unsafe { Interface::take(gauge as *mut Interface<dyn Gauge>) }
}

/// Registers a C implementation of `Gauge`. The caller owns `self_` and
/// keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_Gauge_destroy`] or, once a `*mut` parameter
/// consumed it, until the registry it went into is destroyed. The function
/// may be called from any thread, concurrently, so the C caller is
/// responsible for its thread-safety on `self_`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Gauge_new(
    self_: *mut c_void,
    value: kafka_common_metrics_Gauge_value_fn_t,
) -> *mut kafka_common_metrics_Gauge_t {
    let gauge: Arc<dyn Gauge> = Arc::new(CGauge { self_, value });
    Interface::owned(gauge) as *mut kafka_common_metrics_Gauge_t
}

/// `value(MetricConfig config, long now)`: the reading, owned and freed with
/// `kafka_common_MetricValue_destroy`.
///
/// # Safety
///
/// `self_` must be a valid gauge handle and `config` a valid metric-config
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Gauge_value(
    self_: *const kafka_common_metrics_Gauge_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> *mut kafka_common_MetricValue_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid gauge handle, as `gauge_ref`
    // requires, and `config` a valid metric-config handle, as `metric_config_ref` requires; both
    // references are used only within this call, and the reading is boxed into an owned handle.
    box_metric_value(unsafe { gauge_ref(self_) }.value(unsafe { metric_config_ref(config) }, now))
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned gauge handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Gauge_destroy(self_: *mut kafka_common_metrics_Gauge_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned gauge handle not yet
    // destroyed, as `Interface::destroy` requires; a view a class handle caches is never passed
    // here.
    unsafe { Interface::<dyn Gauge>::destroy(self_ as *mut Interface<dyn Gauge>) }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::metric::{
        kafka_common_MetricValue__enum, kafka_common_MetricValue_destroy, kafka_common_MetricValue_e,
        kafka_common_MetricValue_long,
    };
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };

    // A C gauge reporting `now + *self` as a long.
    unsafe extern "C" fn c_value(
        self_: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        now: i64,
    ) -> *mut kafka_common_MetricValue_t {
        // SAFETY: Test code: `self_` is the `offset` local the test registered, which outlives
        // every call.
        kafka_common_MetricValue_long(now + unsafe { *(self_ as *const i64) })
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_the_invoker() {
        let mut offset = 10_i64;
        let handle = kafka_common_metrics_Gauge_new(&mut offset as *mut i64 as *mut c_void, c_value);
        let config = MetricConfig::new();
        // SAFETY: Test code: `handle` comes from `kafka_common_metrics_Gauge_new` over the `offset`
        // local, which outlives it, and `take_gauge` consumes it, after which it is not used;
        // `config_handle` comes from `kafka_common_metrics_MetricConfig_new` and `value` from the
        // invoker, each destroyed once.
        unsafe {
            assert_eq!(gauge_ref(handle).value(&config, 5), MetricValue::Long(15));
            let config_handle = kafka_common_metrics_MetricConfig_new();
            let value = kafka_common_metrics_Gauge_value(handle, config_handle, 1);
            assert_eq!(
                kafka_common_MetricValue__enum(value),
                kafka_common_MetricValue_e::kafka_common_MetricValue_LONG
            );
            kafka_common_MetricValue_destroy(value);
            kafka_common_metrics_MetricConfig_destroy(config_handle);

            let taken = take_gauge(handle);
            assert_eq!(ArcGauge(taken).value(&config, 0), MetricValue::Long(10));
            kafka_common_metrics_Gauge_destroy(ptr::null_mut());
        }
    }
}

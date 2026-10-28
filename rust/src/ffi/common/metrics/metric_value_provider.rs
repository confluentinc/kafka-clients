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

//! `kafka_common_metrics_MetricValueProvider_t`: the
//! `org.apache.kafka.common.metrics.MetricValueProvider` interface (CLAUDE.md
//! §4, "Traits").
//!
//! Java's `MetricValueProvider<T>` produces a `T`; the Rust provider produces
//! the type-erased `MetricValue`, so a C implementation builds its reading with
//! one of the `kafka_common_MetricValue_*` constructors and Rust takes it over.
//! A provider is passed to the registry by `const` pointer and copied: the copy
//! shares the implementation.

#![expect(non_camel_case_types)]

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::common::MetricValue;
use crate::common::metrics::{Measurable, MetricConfig, MetricValueProvider};
use crate::ffi::common::metric::{box_metric_value, kafka_common_MetricValue_t, take_metric_value};
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::common::metrics::{Interface, c_type_name, type_name_c_str};
use crate::ffi::ffi_guard;

/// Opaque handle to a [`MetricValueProvider`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_MetricValueProvider_t {
    _private: [u8; 0],
}

/// `value(MetricConfig config, long now)` of a C implementation: `config` is
/// borrowed for the call; the returned reading (never null) is owned and
/// taken over by Rust.
pub type kafka_common_metrics_MetricValueProvider_value_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> *mut kafka_common_MetricValue_t;

/// The Rust-only `type_name` of a C implementation, the provider's name in
/// `KafkaMetric_to_string`: a NUL-terminated string that stays valid and
/// unchanged for the rest of the program (a string literal), or `NULL` for
/// none. A `NULL` slot is the default for a C implementation: no type name,
/// as for a closure.
pub type kafka_common_metrics_MetricValueProvider_type_name_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> *const c_char>;

/// A C implementation of [`MetricValueProvider`] registered through
/// `kafka_common_metrics_MetricValueProvider_new`.
struct CMetricValueProvider {
    self_: *mut c_void,
    value: kafka_common_metrics_MetricValueProvider_value_fn_t,
    type_name: kafka_common_metrics_MetricValueProvider_type_name_fn_t,
}

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_metrics_MetricValueProvider_new`: the C user is responsible for the thread-safety
// of `self_`, which that function's rustdoc states, and the function pointers are trivially
// shareable. `CMetricValueProvider` itself never dereferences `self_`; it only passes it back to the
// functions registered with it.
unsafe impl Send for CMetricValueProvider {}
// SAFETY: `Sync` is required by the `MetricValueProvider` trait, whose implementations are shared
// behind an `Arc` and called through `&self` from any thread. A `&CMetricValueProvider` exposes only
// the opaque `self_` and the function pointers, which the type never dereferences;
// `kafka_common_metrics_MetricValueProvider_new`'s rustdoc lets them be called from any thread,
// concurrently, and the C user is responsible for the thread-safety of `self_` under those calls.
unsafe impl Sync for CMetricValueProvider {}

impl MetricValueProvider for CMetricValueProvider {
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.value` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_MetricValueProvider_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle is destroyed or, once copied into a registry, until that
        // registry is, which is after this `&self` call, and allows the call to come from any
        // thread, concurrently with others. `config` is the handle `MetricConfigInner::borrowed`
        // made from the `&MetricConfig` this method holds, valid for the call, which is all the
        // function type's rustdoc promises the C side. The function type's rustdoc has the C
        // function return an owned, non-null metric-value handle, which `take_metric_value` takes
        // over, once.
        unsafe { take_metric_value((self.value)(self.self_, config.as_ptr(), now)) }
    }

    fn type_name(&self) -> Option<&'static str> {
        // SAFETY: `self.type_name` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_MetricValueProvider_new`, whose rustdoc has the C function
        // return `NULL` or a string valid for the rest of the program and allows the call from any
        // thread, as `c_type_name` requires.
        unsafe { c_type_name(self.type_name, self.self_) }
    }
}

/// A shared [`MetricValueProvider`] seen as the boxed one the Rust API takes.
struct ArcMetricValueProvider(Arc<dyn MetricValueProvider>);

impl MetricValueProvider for ArcMetricValueProvider {
    fn value(&self, config: &MetricConfig, now: i64) -> MetricValue {
        self.0.value(config, now)
    }

    fn type_name(&self) -> Option<&'static str> {
        self.0.type_name()
    }

    fn as_measurable(&self) -> Option<&dyn Measurable> {
        self.0.as_measurable()
    }
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `provider` must be a valid metric-value-provider handle, alive for `'a`.
unsafe fn provider_ref<'a>(
    provider: *const kafka_common_metrics_MetricValueProvider_t,
) -> &'a Interface<dyn MetricValueProvider> {
    // SAFETY: Per this function's `# Safety`, `provider` is a valid metric-value-provider handle
    // alive for `'a`, as `Interface::from_ptr` requires: every
    // `kafka_common_metrics_MetricValueProvider_t` is an `Interface<dyn MetricValueProvider>`; the
    // reference returned lives for the same `'a`.
    unsafe { Interface::<dyn MetricValueProvider>::from_ptr(provider as *const Interface<dyn MetricValueProvider>) }
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]), as the boxed provider the Rust API takes.
///
/// # Safety
///
/// `provider` must be a valid metric-value-provider handle, not used again
/// by the caller when it was owned.
pub(crate) unsafe fn take_metric_value_provider(
    provider: *mut kafka_common_metrics_MetricValueProvider_t,
) -> Box<dyn MetricValueProvider> {
    // SAFETY: Per this function's `# Safety`, `provider` is a valid metric-value-provider handle,
    // not used again by the caller when it was owned, as `Interface::take` requires; every such
    // handle is an `Interface` of this trait object.
    Box::new(ArcMetricValueProvider(unsafe {
        Interface::take(provider as *mut Interface<dyn MetricValueProvider>)
    }))
}

/// Registers a C implementation of `MetricValueProvider`.
///
/// # Parameters
///
/// - `value` - Implements `kafka_common_metrics_MetricValueProvider_value`.
/// - `type_name` - Implements `kafka_common_metrics_MetricValueProvider_type_name`, or `NULL`
///   for no type name.
///
/// # Returns
///
/// The implementation's handle.
///
/// # Safety
///
/// The caller owns `self_` and keeps it alive until the handle is destroyed with
/// `kafka_common_metrics_MetricValueProvider_destroy` or, once a `*mut` parameter consumed it,
/// until the registry it went into is destroyed. The functions may be called from any thread,
/// concurrently, so the C caller is responsible for their thread-safety on `self_`. The returned
/// handle is owned.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_MetricValueProvider_new(
    self_: *mut c_void,
    value: kafka_common_metrics_MetricValueProvider_value_fn_t,
    type_name: kafka_common_metrics_MetricValueProvider_type_name_fn_t,
) -> *mut kafka_common_metrics_MetricValueProvider_t {
    let provider: Arc<dyn MetricValueProvider> = Arc::new(CMetricValueProvider { self_, value, type_name });
    Interface::owned(provider) as *mut kafka_common_metrics_MetricValueProvider_t
}

/// Returns the current value associated with this metric.
///
/// # Parameters
///
/// - `config` - The configuration for this metric
/// - `now` - The POSIX time in milliseconds the measurement is being taken
///
/// # Returns
///
/// the current metric value
///
/// # Safety
///
/// `self_` must be a valid metric-value-provider handle and `config` a valid metric-config handle.
/// The returned value is owned, freed with `kafka_common_MetricValue_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_value(
    self_: *const kafka_common_metrics_MetricValueProvider_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> *mut kafka_common_MetricValue_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-value-provider handle, as
    // `provider_ref` requires, and `config` a valid metric-config handle, as `metric_config_ref`
    // requires; both references are used only within this call, and the reading is boxed into an
    // owned handle.
    box_metric_value(
        unsafe { provider_ref(self_) }
            .get()
            .value(unsafe { metric_config_ref(config) }, now),
    )
}

/// Returns the provider's type name, as `kafka_common_metrics_KafkaMetric_to_string` reports it,
/// or `NULL` for none.
///
/// # Safety
///
/// `self_` must be a valid metric-value-provider handle. The returned string is borrowed and stays
/// valid for the rest of the program.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_type_name(
    self_: *const kafka_common_metrics_MetricValueProvider_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-value-provider handle, as
    // `provider_ref` requires; the reference is used only within this call.
    type_name_c_str(unsafe { provider_ref(self_) }.get().type_name())
}

/// Frees an owned metric-value-provider handle; null is a no-op. A registry
/// it was copied into keeps its own copy.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricValueProvider_destroy(
    self_: *mut kafka_common_metrics_MetricValueProvider_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Interface::destroy` requires.
    unsafe { Interface::<dyn MetricValueProvider>::destroy(self_ as *mut Interface<dyn MetricValueProvider>) }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::metric::kafka_common_MetricValue_string;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };

    unsafe extern "C" fn c_value(
        _self: *mut c_void,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> *mut kafka_common_MetricValue_t {
        // SAFETY: Test code: `c"up"` is a static NUL-terminated string, copied by the constructor.
        unsafe { kafka_common_MetricValue_string(c"up".as_ptr()) }
    }

    unsafe extern "C" fn c_type_name_slot(_self: *mut c_void) -> *const c_char {
        c"my.CProvider".as_ptr()
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_taken() {
        let provider = kafka_common_metrics_MetricValueProvider_new(ptr::null_mut(), c_value, Some(c_type_name_slot));
        let config = kafka_common_metrics_MetricConfig_new();
        // SAFETY: Test code: `provider` comes from `kafka_common_metrics_MetricValueProvider_new`
        // (its functions ignore the null `self_`) and `config` from
        // `kafka_common_metrics_MetricConfig_new`; `take_metric_value_provider` consumes
        // `provider` after its last use, and every other handle and value is destroyed once,
        // after its last use.
        unsafe {
            let value = kafka_common_metrics_MetricValueProvider_value(provider, config, 0);
            assert_eq!(take_metric_value(value), MetricValue::String("up".to_string()));
            assert_eq!(
                std::ffi::CStr::from_ptr(kafka_common_metrics_MetricValueProvider_type_name(provider)),
                c"my.CProvider"
            );
            // The provider the registry takes is the C implementation, and a C provider is not a
            // measurable.
            let taken = take_metric_value_provider(provider);
            assert_eq!(taken.value(&MetricConfig::new(), 8), MetricValue::String("up".to_string()));
            assert_eq!(taken.type_name(), Some("my.CProvider"));
            assert!(taken.as_measurable().is_none());
            kafka_common_metrics_MetricValueProvider_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}

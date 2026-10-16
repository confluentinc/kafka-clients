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

//! `kafka_common_metrics_Measurable_t`: the
//! `org.apache.kafka.common.metrics.Measurable` interface (CLAUDE.md §4,
//! "Traits").
//!
//! A `Measurable_t` is either a C implementation registered with
//! [`kafka_common_metrics_Measurable_new`] (what `Metrics_add_metric_with_measurable`
//! and `CompoundStat_NamedMeasurable_new` take), the owned handle
//! `CompoundStat_NamedMeasurable_stat` returns, or the view a stat class
//! (`Avg__as_Measurable`) or `KafkaMetric_measurable` hands out.

#![expect(non_camel_case_types)]

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::common::metrics::{Measurable, MetricConfig};
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::common::metrics::{Interface, c_type_name, type_name_c_str};
use crate::ffi::ffi_guard;

/// Opaque handle to a [`Measurable`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_Measurable_t {
    _private: [u8; 0],
}

/// `measure(MetricConfig config, long now)` of a C implementation: `config`
/// is borrowed for the call.
pub type kafka_common_metrics_Measurable_measure_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, config: *const kafka_common_metrics_MetricConfig_t, now: i64) -> f64;

/// The Rust-only `type_name` of a C implementation, the provider's name in
/// `KafkaMetric_to_string`: a NUL-terminated string that stays valid and
/// unchanged for the rest of the program (a string literal), or `NULL` for
/// none. A `NULL` slot is the default for a C implementation: no type name,
/// as for a closure.
pub type kafka_common_metrics_Measurable_type_name_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> *const c_char>;

/// A C implementation of [`Measurable`] registered through
/// [`kafka_common_metrics_Measurable_new`].
struct CMeasurable {
    self_: *mut c_void,
    measure: kafka_common_metrics_Measurable_measure_fn_t,
    type_name: kafka_common_metrics_Measurable_type_name_fn_t,
}

// SAFETY: `self_` and the function pointer are what the C caller registered through
// `kafka_common_metrics_Measurable_new`: the C user is responsible for the thread-safety of
// `self_`, which that function's rustdoc states, and the function pointer is trivially shareable.
// `CMeasurable` itself never dereferences `self_`; it only passes it back to the function
// registered with it.
unsafe impl Send for CMeasurable {}
// SAFETY: `Sync` is required by the `Measurable` trait, whose implementations are shared behind an
// `Arc` and called through `&self` from any thread. A `&CMeasurable` exposes only the opaque
// `self_` and the function pointer, which the type never dereferences;
// `kafka_common_metrics_Measurable_new`'s rustdoc lets it be called from any thread, concurrently,
// and the C user is responsible for the thread-safety of `self_` under those calls.
unsafe impl Sync for CMeasurable {}

impl Measurable for CMeasurable {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.measure` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_Measurable_new`, whose rustdoc has the caller keep `self_`
        // alive until the handle is destroyed or, once consumed, until the registry or sensor it
        // went into is, which is after this `&self` call, and allows the call to come from any
        // thread, concurrently with others. `config` is the handle `MetricConfigInner::borrowed`
        // made from the `&MetricConfig` this method holds, valid for the call, which is all the
        // function type's rustdoc promises the C side.
        unsafe { (self.measure)(self.self_, config.as_ptr(), now) }
    }

    fn type_name(&self) -> Option<&'static str> {
        // SAFETY: `self.type_name` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_Measurable_new`, whose rustdoc has the C function return
        // `NULL` or a string valid for the rest of the program and allows the call from any
        // thread, as `c_type_name` requires.
        unsafe { c_type_name(self.type_name, self.self_) }
    }
}

/// A shared [`Measurable`] seen as the boxed one the Rust API takes.
pub(crate) struct ArcMeasurable(pub(crate) Arc<dyn Measurable>);

impl Measurable for ArcMeasurable {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.0.measure(config, now)
    }

    fn type_name(&self) -> Option<&'static str> {
        self.0.type_name()
    }
}

/// Hands `measurable` to C as an owned handle, freed with
/// [`kafka_common_metrics_Measurable_destroy`] or consumed by a `*mut`
/// parameter.
pub(crate) fn box_measurable(measurable: Arc<dyn Measurable>) -> *mut kafka_common_metrics_Measurable_t {
    Interface::owned(measurable) as *mut kafka_common_metrics_Measurable_t
}

/// The implementation behind a handle.
///
/// # Safety
///
/// `measurable` must be a valid measurable handle, alive for `'a`.
pub(crate) unsafe fn measurable_ref<'a>(measurable: *const kafka_common_metrics_Measurable_t) -> &'a dyn Measurable {
    // SAFETY: Per this function's `# Safety`, `measurable` is a valid measurable handle alive for
    // `'a`, as `Interface::from_ptr` requires: every `kafka_common_metrics_Measurable_t` is an
    // `Interface<dyn Measurable>`, owned or a view; the reference returned lives for the same `'a`.
    unsafe { Interface::<dyn Measurable>::from_ptr(measurable as *const Interface<dyn Measurable>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]).
///
/// # Safety
///
/// `measurable` must be a valid measurable handle, not used again by the
/// caller when it was owned.
pub(crate) unsafe fn take_measurable(measurable: *mut kafka_common_metrics_Measurable_t) -> Arc<dyn Measurable> {
    // SAFETY: Per this function's `# Safety`, `measurable` is a valid measurable handle, not used
    // again by the caller when it was owned, as `Interface::take` requires; every such handle is an
    // `Interface` of this trait object.
    unsafe { Interface::take(measurable as *mut Interface<dyn Measurable>) }
}

/// Registers a C implementation of `Measurable`. The caller owns `self_` and
/// keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_Measurable_destroy`] or, once a `*mut` parameter
/// consumed it, until the registry or sensor it went into is destroyed.
/// The function may be called from any thread, concurrently, so the C
/// caller is responsible for its thread-safety on `self_`. `type_name` may be `NULL`
/// (see [`kafka_common_metrics_Measurable_type_name_fn_t`]).
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Measurable_new(
    self_: *mut c_void,
    measure: kafka_common_metrics_Measurable_measure_fn_t,
    type_name: kafka_common_metrics_Measurable_type_name_fn_t,
) -> *mut kafka_common_metrics_Measurable_t {
    box_measurable(Arc::new(CMeasurable { self_, measure, type_name }))
}

/// `measure(MetricConfig config, long now)`.
///
/// # Safety
///
/// `self_` must be a valid measurable handle and `config` a valid
/// metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Measurable_measure(
    self_: *const kafka_common_metrics_Measurable_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid measurable handle, as
    // `measurable_ref` requires, and `config` a valid metric-config handle, as `metric_config_ref`
    // requires; both references are used only within this call.
    unsafe { measurable_ref(self_) }.measure(unsafe { metric_config_ref(config) }, now)
}

/// The Rust-only `type_name()`: the provider's type name as
/// `KafkaMetric_to_string` reports it, or `NULL` for none (a closure). The
/// string is borrowed and stays valid for the rest of the program.
///
/// # Safety
///
/// `self_` must be a valid measurable handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Measurable_type_name(
    self_: *const kafka_common_metrics_Measurable_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid measurable handle, as `measurable_ref`
    // requires; the reference is used only within this call.
    type_name_c_str(unsafe { measurable_ref(self_) }.type_name())
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned measurable handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Measurable_destroy(self_: *mut kafka_common_metrics_Measurable_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned measurable handle not yet
    // destroyed, as `Interface::destroy` requires; a view a class handle caches is never passed
    // here.
    unsafe { Interface::<dyn Measurable>::destroy(self_ as *mut Interface<dyn Measurable>) }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
        kafka_common_metrics_MetricConfig_samples,
    };

    // A C measurable returning `samples * now + *self`.
    unsafe extern "C" fn c_measure(
        self_: *mut c_void,
        config: *const kafka_common_metrics_MetricConfig_t,
        now: i64,
    ) -> f64 {
        // SAFETY: Test code: `self_` is the `offset` local the test registered, which outlives
        // every call.
        let offset = unsafe { *(self_ as *const f64) };
        // SAFETY: Test code: `config` is the handle the invoker passes, valid for the call.
        let samples = unsafe { kafka_common_metrics_MetricConfig_samples(config) };
        samples as f64 * now as f64 + offset
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_the_invoker() {
        let mut offset = 0.5_f64;
        let handle = kafka_common_metrics_Measurable_new(&mut offset as *mut f64 as *mut c_void, c_measure, None);
        let config = MetricConfig::new().set_samples(3);
        // SAFETY: Test code: `handle` comes from `kafka_common_metrics_Measurable_new` over the
        // `offset` local, which outlives it, and `take_measurable` consumes it, after which it is
        // not used; `config_handle` comes from `kafka_common_metrics_MetricConfig_new` and is
        // destroyed once.
        unsafe {
            assert_eq!(measurable_ref(handle).measure(&config, 2), 6.5);
            let config_handle = kafka_common_metrics_MetricConfig_new();
            assert_eq!(
                kafka_common_metrics_Measurable_measure(handle, config_handle, 2),
                f64::from(MetricConfig::DEFAULT_NUM_SAMPLES) * 2.0 + 0.5
            );
            kafka_common_metrics_MetricConfig_destroy(config_handle);

            // Consuming an owned handle frees it and moves the implementation.
            let taken = take_measurable(handle);
            assert_eq!(ArcMeasurable(taken).measure(&config, 1), 3.5);
            kafka_common_metrics_Measurable_destroy(ptr::null_mut());
        }
    }
    // A C `type_name` slot reporting a string literal.
    unsafe extern "C" fn c_type_name_slot(_self: *mut c_void) -> *const c_char {
        c"my.CMeasurable".as_ptr()
    }

    #[test]
    fn type_name_is_the_c_slot_or_none_when_the_slot_is_null() {
        let named = kafka_common_metrics_Measurable_new(ptr::null_mut(), c_measure, Some(c_type_name_slot));
        let unnamed = kafka_common_metrics_Measurable_new(ptr::null_mut(), c_measure, None);
        // SAFETY: Test code: both handles come from `kafka_common_metrics_Measurable_new` over a null
        // `self_` the C functions never read; the strings the invoker returns stay valid for the
        // rest of the program; `measurable_ref` borrows each handle within the block, and each handle is
        // destroyed once, after its last use.
        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(kafka_common_metrics_Measurable_type_name(named)).to_str(),
                Ok("my.CMeasurable")
            );
            assert!(kafka_common_metrics_Measurable_type_name(unnamed).is_null());
            assert_eq!(measurable_ref(named).type_name(), Some("my.CMeasurable"));
            assert_eq!(measurable_ref(unnamed).type_name(), None);
            // The shared-measurable adapter reports the measurable it wraps, not its own type.
            assert_eq!(ArcMeasurable(take_measurable(named)).type_name(), Some("my.CMeasurable"));
            kafka_common_metrics_Measurable_destroy(unnamed);
        }
    }
}

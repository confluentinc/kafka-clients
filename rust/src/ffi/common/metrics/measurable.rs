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

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::metrics::{Measurable, MetricConfig};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};

/// Opaque handle to a [`Measurable`] implementation.
#[repr(C)]
pub struct kafka_common_metrics_Measurable_t {
    _private: [u8; 0],
}

/// `measure(MetricConfig config, long now)` of a C implementation: `config`
/// is borrowed for the call.
pub type kafka_common_metrics_Measurable_measure_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, config: *const kafka_common_metrics_MetricConfig_t, now: i64) -> f64;

/// A C implementation of [`Measurable`] registered through
/// [`kafka_common_metrics_Measurable_new`].
struct CMeasurable {
    self_: *mut c_void,
    measure: kafka_common_metrics_Measurable_measure_fn_t,
}

// SAFETY: `self_` is what the C caller registered, whose thread-safety is
// the caller's responsibility as for every interface implementation
// (CLAUDE.md §4).
unsafe impl Send for CMeasurable {}
unsafe impl Sync for CMeasurable {}

impl Measurable for CMeasurable {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        let config = MetricConfigInner::borrowed(config);
        unsafe { (self.measure)(self.self_, config.as_ptr(), now) }
    }
}

/// A shared [`Measurable`] seen as the boxed one the Rust API takes.
pub(crate) struct ArcMeasurable(pub(crate) Arc<dyn Measurable>);

impl Measurable for ArcMeasurable {
    fn measure(&self, config: &MetricConfig, now: i64) -> f64 {
        self.0.measure(config, now)
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
/// `measurable` must be a valid measurable handle.
pub(crate) unsafe fn measurable_ref<'a>(measurable: *const kafka_common_metrics_Measurable_t) -> &'a dyn Measurable {
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
    unsafe { Interface::take(measurable as *mut Interface<dyn Measurable>) }
}

/// Registers a C implementation of `Measurable`. The caller owns `self_` and
/// keeps it alive until the handle is destroyed with
/// [`kafka_common_metrics_Measurable_destroy`] or, once a `*mut` parameter
/// consumed it, until the registry or sensor it went into is destroyed.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Measurable_new(
    self_: *mut c_void,
    measure: kafka_common_metrics_Measurable_measure_fn_t,
) -> *mut kafka_common_metrics_Measurable_t {
    box_measurable(Arc::new(CMeasurable { self_, measure }))
}

/// `measure(MetricConfig config, long now)`.
///
/// # Safety
///
/// `self_` must be a valid measurable handle and `config` a valid
/// metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Measurable_measure(
    self_: *const kafka_common_metrics_Measurable_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64 {
    unsafe { measurable_ref(self_) }.measure(unsafe { metric_config_ref(config) }, now)
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned measurable handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Measurable_destroy(self_: *mut kafka_common_metrics_Measurable_t) {
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
        let offset = unsafe { *(self_ as *const f64) };
        let samples = unsafe { kafka_common_metrics_MetricConfig_samples(config) };
        samples as f64 * now as f64 + offset
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait_and_the_invoker() {
        let mut offset = 0.5_f64;
        let handle = kafka_common_metrics_Measurable_new(&mut offset as *mut f64 as *mut c_void, c_measure);
        let config = MetricConfig::new().set_samples(3);
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
}

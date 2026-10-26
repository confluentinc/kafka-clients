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

//! `kafka_common_metrics_stats_SampledStat_t`:
//! `org.apache.kafka.common.metrics.stats.SampledStat` (CLAUDE.md §4), the
//! windowed-sample stat the concrete classes (`Avg`, `Max`, `WindowedSum`,
//! ...) specialise, plus:
//!
//!   - `kafka_common_metrics_stats_SampledStat_Sample_t`, Java's nested
//!     `SampledStat.Sample` (§4, "Nested types");
//!   - `kafka_common_metrics_stats_SampledStatKind_t`, the Rust-only trait
//!     standing for Java's abstract `update` / `combine` methods, which a C
//!     caller implements to define its own sampled stat (§4, "Traits").
//!
//! A `SampledStat_t` built by `_new` consumes the kind handle it is given.
//! It implements `Stat` and `Measurable`, so it hands out the three views
//! described in the module docs of `stats`; `Rate_with_stat` and
//! `Meter_with_rate_stat` share it rather than consume it.

#![expect(non_camel_case_types)]

use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::common::metrics::MetricConfig;
use crate::common::metrics::stats::{Sample, SampledStat, SampledStatKind};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_t;
use crate::ffi::common::metrics::measurable_stat::kafka_common_metrics_MeasurableStat_t;
use crate::ffi::common::metrics::metric_config::{
    MetricConfigInner, kafka_common_metrics_MetricConfig_t, metric_config_ref,
};
use crate::ffi::common::metrics::metric_value_provider::kafka_common_metrics_MetricValueProvider_t;
use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_t;
use crate::ffi::common::metrics::stats::StatHandle;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, into_c_string, kafka_List_destroy, kafka_List_t, list_elements};

// ---------------------------------------------------------------------------
// SampledStat
// ---------------------------------------------------------------------------

/// Opaque handle to a [`SampledStat`].
#[repr(C)]
pub struct kafka_common_metrics_stats_SampledStat_t {
    _private: [u8; 0],
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid handle of this class, alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_common_metrics_stats_SampledStat_t) -> &'a StatHandle<SampledStat> {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class alive for
    // `'a`, so it points at the `StatHandle<SampledStat>` one of this class's constructors boxed,
    // as `StatHandle::from_ptr` requires.
    unsafe { StatHandle::from_ptr(self_ as *const StatHandle<SampledStat>) }
}

/// The shared stat behind a handle, for the constructors that share it
/// (`Rate`, `Meter`).
///
/// # Safety
///
/// `stat` must be a valid sampled-stat handle, alive for `'a`.
pub(crate) unsafe fn sampled_stat_ref<'a>(
    stat: *const kafka_common_metrics_stats_SampledStat_t,
) -> &'a Arc<SampledStat> {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid sampled-stat handle alive for `'a`,
    // as `handle` requires; the reference returned borrows the handle's `Arc` for the same `'a`.
    unsafe { handle(stat) }.stat()
}

/// `new SampledStat(double initialValue)` plus the subclass behaviour.
///
/// # Parameters
///
/// - `initial_value` - The value each sample starts from.
/// - `kind` - The subclass behaviour; taken over (an owned handle is consumed, a view is shared).
///
/// # Safety
///
/// `kind` must be a valid kind handle, not used again by the caller when it
/// was owned. The returned handle is owned, freed with
/// `kafka_common_metrics_stats_SampledStat_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_new(
    initial_value: f64,
    kind: *mut kafka_common_metrics_stats_SampledStatKind_t,
) -> *mut kafka_common_metrics_stats_SampledStat_t {
    // SAFETY: Per this function's `# Safety`, `kind` is a valid kind handle, not used again by the
    // caller when it was owned, as `Interface::take` requires; every
    // `kafka_common_metrics_stats_SampledStatKind_t` is an `Interface<dyn SampledStatKind>`.
    let kind = unsafe { Interface::take(kind as *mut Interface<dyn SampledStatKind>) };
    StatHandle::boxed(SampledStat::new(initial_value, Box::new(ArcSampledStatKind(kind))))
        as *mut kafka_common_metrics_stats_SampledStat_t
}

/// Returns the stat as the `Stat` interface.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
///
/// The returned view is borrowed, valid as long as `self_`, and is never passed to an interface
/// `_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_Stat(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_Stat_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_stat()
}

/// Returns the stat as the `Measurable` interface.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
///
/// The returned view is borrowed, valid as long as `self_`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_Measurable(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_Measurable_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_measurable()
}

/// Returns the stat as the `MetricValueProvider` interface (Java's `Measurable` extends it).
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
///
/// The returned view is borrowed, valid as long as `self_`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_MetricValueProvider(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_MetricValueProvider_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_metric_value_provider()
}

/// Returns the stat as the `MeasurableStat` interface; a sensor it is added to shares the stat.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
///
/// The returned view is borrowed, valid as long as `self_`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat__as_MeasurableStat(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *const kafka_common_metrics_MeasurableStat_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle of this class, as `handle`
    // requires. The view returned is cached in the handle's `OnceLock`, so it lives as long as the
    // handle; that is the rustdoc's "valid as long as the handle".
    unsafe { handle(self_) }.as_measurable_stat()
}

/// Returns `toString()`, as a string.
///
/// # Safety
///
/// `self_` must be a valid handle of this class.
///
/// The returned string is owned by the caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_to_string(
    self_: *const kafka_common_metrics_stats_SampledStat_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, as `handle` requires; the string is built during the call.
    into_c_string(&unsafe { handle(self_) }.stat().to_string())
}

/// Frees an owned handle; null is a no-op. A sensor, rate or meter the stat
/// was shared with keeps it alive.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_destroy(
    self_: *mut kafka_common_metrics_stats_SampledStat_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // a `StatHandle<SampledStat>` from this class's constructors, as `StatHandle::destroy`
    // requires; a sensor the stat was added to keeps its own `Arc` of the stat.
    unsafe { StatHandle::destroy(self_ as *mut StatHandle<SampledStat>) }
}

// ---------------------------------------------------------------------------
// SampledStatKind: the C-implementable behaviour of a sampled stat
// ---------------------------------------------------------------------------

/// Opaque handle to a [`SampledStatKind`] implementation: the abstract
/// `update` / `combine` half of Java's `SampledStat`, with no Java class of
/// its own.
// the subclass behaviour of Java's abstract SampledStat (CLAUDE.md §4)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_metrics_stats_SampledStatKind_t {
    _private: [u8; 0],
}

/// `update(Sample sample, MetricConfig config, double value, long timeMs)`:
/// folds `value` into the current sample. `sample` is borrowed for the call
/// and mutated in place.
pub type kafka_common_metrics_stats_SampledStatKind_update_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    sample: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
);

/// `combine(List<Sample> samples, MetricConfig config, long now)`: the
/// measurement over every sample. `samples` holds
/// `const kafka_common_metrics_stats_SampledStat_Sample_t *` elements
/// borrowed for the call.
pub type kafka_common_metrics_stats_SampledStatKind_combine_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    samples: *const kafka_List_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64;

/// `isWindowedCount()`: whether this kind counts events like
/// `WindowedCount`, which `Meter` consults; a Java default (`false`) when
/// the pointer is null, hence the `Option<..>` typedef (the form cbindgen
/// emits as a plain C function pointer).
pub type kafka_common_metrics_stats_SampledStatKind_is_windowed_count_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> i8>;

/// `isWindowedSum()`: whether this kind is a `WindowedSum`, which `Meter`
/// requires of its rate stat; a Java default (`false`) when the pointer is
/// null.
pub type kafka_common_metrics_stats_SampledStatKind_is_windowed_sum_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void) -> i8>;

/// A kind implemented in C.
struct CSampledStatKind {
    self_: *mut c_void,
    update: kafka_common_metrics_stats_SampledStatKind_update_fn_t,
    combine: kafka_common_metrics_stats_SampledStatKind_combine_fn_t,
    is_windowed_count: kafka_common_metrics_stats_SampledStatKind_is_windowed_count_fn_t,
    is_windowed_sum: kafka_common_metrics_stats_SampledStatKind_is_windowed_sum_fn_t,
}

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_metrics_stats_SampledStatKind_new`: the C user is responsible for the thread-safety
// of `self_`, which that function's rustdoc states, and the function pointers are trivially
// shareable. `CSampledStatKind` itself never dereferences `self_`; it only passes it back to the
// functions registered with it.
unsafe impl Send for CSampledStatKind {}
// SAFETY: `Sync` is required by the `SampledStatKind` trait, whose implementations are shared
// behind an `Arc` and called through `&self` from any thread. A `&CSampledStatKind` exposes only
// the opaque `self_` and the function pointers, which the type never dereferences;
// `kafka_common_metrics_stats_SampledStatKind_new`'s rustdoc lets them be called from any thread,
// concurrently, and the C user is responsible for the thread-safety of `self_` under those calls.
unsafe impl Sync for CSampledStatKind {}

impl SampledStatKind for CSampledStatKind {
    fn update(&self, sample: &mut Sample, config: &MetricConfig, value: f64, time_ms: i64) {
        let config = MetricConfigInner::borrowed(config);
        // SAFETY: `self.update` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_stats_SampledStatKind_new`, whose rustdoc has the caller
        // keep `self_` alive until the kind handle is destroyed or, once consumed, until the stat
        // built over it is destroyed together with every sensor, rate or meter it was shared with,
        // which is after this `&self` call, and allows the call to come from any thread,
        // concurrently with others. `sample` is the `&mut Sample` this method holds, cast to the
        // sample handle the function type's rustdoc lends for the call, and `config` is the handle
        // `MetricConfigInner::borrowed` made from the `&MetricConfig` this method holds, valid for
        // the call, which is all the function type's rustdoc promises the C side.
        unsafe {
            (self.update)(
                self.self_,
                sample as *mut Sample as *mut kafka_common_metrics_stats_SampledStat_Sample_t,
                config.as_ptr(),
                value,
                time_ms,
            )
        }
    }

    fn combine(&self, samples: &[Sample], config: &MetricConfig, now: i64) -> f64 {
        let config = MetricConfigInner::borrowed(config);
        let list = box_list(
            samples.iter().map(|sample| sample as *const Sample as *mut c_void).collect(),
            None,
        );
        // SAFETY: `self.combine` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_metrics_stats_SampledStatKind_new`, whose rustdoc has the caller
        // keep `self_` alive until the kind handle is destroyed or, once consumed, until the stat
        // built over it is destroyed together with every sensor, rate or meter it was shared with,
        // which is after this `&self` call, and allows the call to come from any thread,
        // concurrently with others. `list` is the view `box_list(.., None)` built over this
        // method's `&[Sample]`, with no element destructor and destroyed only after the call, which
        // is the function type's "borrowed for the call"; `config` is the handle
        // `MetricConfigInner::borrowed` made from the `&MetricConfig` this method holds, valid for
        // the call, which is all the function type's rustdoc promises the C side.
        let combined = unsafe { (self.combine)(self.self_, list, config.as_ptr(), now) };
        // SAFETY: `list` is the `box_list` handle built above, not yet destroyed and used by
        // nothing else; it has no element destructor, so destroying it frees the list only, leaving
        // the borrowed samples alone.
        unsafe { kafka_List_destroy(list) };
        combined
    }

    fn is_windowed_count(&self) -> bool {
        match self.is_windowed_count {
            // SAFETY: `is_windowed_count` is the non-null function the C caller registered with
            // `self.self_` through `kafka_common_metrics_stats_SampledStatKind_new`, whose rustdoc
            // has the caller keep `self_` alive until the kind handle is destroyed or, once
            // consumed, until the stat built over it is destroyed together with every sensor, rate
            // or meter it was shared with, which is after this `&self` call, and allows the call to
            // come from any thread, concurrently with others.
            Some(is_windowed_count) => unsafe { is_windowed_count(self.self_) != 0 },
            None => false,
        }
    }

    fn is_windowed_sum(&self) -> bool {
        match self.is_windowed_sum {
            // SAFETY: `is_windowed_sum` is the non-null function the C caller registered with
            // `self.self_` through `kafka_common_metrics_stats_SampledStatKind_new`, whose rustdoc
            // has the caller keep `self_` alive until the kind handle is destroyed or, once
            // consumed, until the stat built over it is destroyed together with every sensor, rate
            // or meter it was shared with, which is after this `&self` call, and allows the call to
            // come from any thread, concurrently with others.
            Some(is_windowed_sum) => unsafe { is_windowed_sum(self.self_) != 0 },
            None => false,
        }
    }
}

/// A shared kind as the boxed kind [`SampledStat::new`] takes.
pub(crate) struct ArcSampledStatKind(pub(crate) Arc<dyn SampledStatKind>);

impl SampledStatKind for ArcSampledStatKind {
    fn update(&self, sample: &mut Sample, config: &MetricConfig, value: f64, time_ms: i64) {
        self.0.update(sample, config, value, time_ms)
    }

    fn combine(&self, samples: &[Sample], config: &MetricConfig, now: i64) -> f64 {
        self.0.combine(samples, config, now)
    }

    fn is_windowed_count(&self) -> bool {
        self.0.is_windowed_count()
    }

    fn is_windowed_sum(&self) -> bool {
        self.0.is_windowed_sum()
    }
}

/// The kind behind a handle.
///
/// # Safety
///
/// `kind` must be a valid kind handle, alive for `'a`.
pub(crate) unsafe fn sampled_stat_kind_ref<'a>(
    kind: *const kafka_common_metrics_stats_SampledStatKind_t,
) -> &'a dyn SampledStatKind {
    // SAFETY: Per this function's `# Safety`, `kind` is a valid kind handle alive for `'a`, as
    // `Interface::from_ptr` requires: every `kafka_common_metrics_stats_SampledStatKind_t` is an
    // `Interface<dyn SampledStatKind>`; the reference returned lives for the same `'a`.
    unsafe { Interface::from_ptr(kind as *const Interface<dyn SampledStatKind>) }.get()
}

/// Builds a kind from a C implementation. The caller owns `self_` and keeps
/// it alive until the handle is destroyed with
/// `kafka_common_metrics_stats_SampledStatKind_destroy` or, once
/// `kafka_common_metrics_stats_SampledStat_new` consumed it, until the stat
/// built over it is destroyed together with every sensor, rate or meter it
/// was shared with. The two `is_*` methods may be null for the Java default
/// (`false`). The functions may be called from any thread, concurrently, so
/// the C caller is responsible for their thread-safety on `self_`.
///
/// # Parameters
///
/// - `self_` - The implementation's state, passed back to every function.
/// - `update` - Its `update(Sample, MetricConfig, double, long)`.
/// - `combine` - Its `combine(List<Sample>, MetricConfig, long)`.
/// - `is_windowed_count` - Its `isWindowedCount()`, or `NULL` for the Java default.
/// - `is_windowed_sum` - Its `isWindowedSum()`, or `NULL` for the Java default.
///
/// # Returns
///
/// A kind handle.
///
/// # Safety
///
/// The returned handle is owned by the caller: consumed by
/// `kafka_common_metrics_stats_SampledStat_new` or freed with
/// `kafka_common_metrics_stats_SampledStatKind_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SampledStatKind_new(
    self_: *mut c_void,
    update: kafka_common_metrics_stats_SampledStatKind_update_fn_t,
    combine: kafka_common_metrics_stats_SampledStatKind_combine_fn_t,
    is_windowed_count: kafka_common_metrics_stats_SampledStatKind_is_windowed_count_fn_t,
    is_windowed_sum: kafka_common_metrics_stats_SampledStatKind_is_windowed_sum_fn_t,
) -> *mut kafka_common_metrics_stats_SampledStatKind_t {
    Interface::owned(
        Arc::new(CSampledStatKind { self_, update, combine, is_windowed_count, is_windowed_sum })
            as Arc<dyn SampledStatKind>,
    ) as *mut kafka_common_metrics_stats_SampledStatKind_t
}

/// Invokes `update` on the kind behind `self_`.
///
/// # Parameters
///
/// - `sample` - The current sample, mutated in place.
/// - `config` - The metric configuration.
/// - `value` - The value to record.
/// - `time_ms` - The time of the recording in milliseconds.
///
/// # Safety
///
/// `self_` must be a valid kind handle, `sample` a valid sample handle that
/// no other call uses at the same time and that is not one of the read-only
/// samples a kind's `combine` receives, and `config` a valid metric-config
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_update(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
    sample: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    value: f64,
    time_ms: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid kind handle, as
    // `sampled_stat_kind_ref` requires; the reference is used only within this call.
    unsafe { sampled_stat_kind_ref(self_) }.update(
        // SAFETY: Per this function's `# Safety`, `sample` is a valid sample handle that no other
        // call uses at the same time and not one of the read-only samples `combine` lends: an owned
        // one, the `Box::into_raw` of a `Sample`, or the one a kind's `update` lends for its call,
        // a `&mut Sample` cast; so the exclusive reference aliases nothing for this call.
        unsafe { &mut *(sample as *mut Sample) },
        // SAFETY: Per this function's `# Safety`, `config` is a valid metric-config handle, as
        // `metric_config_ref` requires; the reference is used only within this call.
        unsafe { metric_config_ref(config) },
        value,
        time_ms,
    )
}

/// Invokes `combine` on the kind behind `self_`.
///
/// # Parameters
///
/// - `samples` - The samples, `const kafka_common_metrics_stats_SampledStat_Sample_t *`
///   elements read during the call; `NULL` reads as empty.
/// - `config` - The metric configuration.
/// - `now` - The current time in milliseconds.
///
/// # Returns
///
/// The measurement over every sample.
///
/// # Safety
///
/// `self_` must be a valid kind handle, `samples` null or a valid list of
/// sample handles and `config` a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_combine(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
    samples: *const kafka_List_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    now: i64,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `samples` is null or a valid list handle, alive and
    // not mutated during this call, as `list_elements` requires; the slice is used only within this
    // call, its samples being cloned before return.
    let samples: Vec<Sample> = unsafe { list_elements(samples) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `samples` is a valid sample
        // handle: an owned one, the `Box::into_raw` of a `Sample`, or one a kind's `update` or
        // `combine` lends for its call; the reference lives only until `clone` copies the sample.
        .map(|&sample| unsafe { &*(sample as *const Sample) }.clone())
        .collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid kind handle, as
    // `sampled_stat_kind_ref` requires, and `config` a valid metric-config handle, as
    // `metric_config_ref` requires; both references are used only within this call.
    unsafe { sampled_stat_kind_ref(self_) }.combine(&samples, unsafe { metric_config_ref(config) }, now)
}

/// Invokes `isWindowedCount` on the kind behind `self_`.
///
/// # Returns
///
/// 1 if the kind counts events like `WindowedCount`, 0 otherwise.
///
/// # Safety
///
/// `self_` must be a valid kind handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_is_windowed_count(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid kind handle, as
    // `sampled_stat_kind_ref` requires; the reference is used only within this call.
    i8::from(unsafe { sampled_stat_kind_ref(self_) }.is_windowed_count())
}

/// Invokes `isWindowedSum` on the kind behind `self_`.
///
/// # Returns
///
/// 1 if the kind is a `WindowedSum`, 0 otherwise.
///
/// # Safety
///
/// `self_` must be a valid kind handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_is_windowed_sum(
    self_: *const kafka_common_metrics_stats_SampledStatKind_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid kind handle, as
    // `sampled_stat_kind_ref` requires; the reference is used only within this call.
    i8::from(unsafe { sampled_stat_kind_ref(self_) }.is_windowed_sum())
}

/// Frees an owned kind handle that was not consumed; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet consumed or destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStatKind_destroy(
    self_: *mut kafka_common_metrics_stats_SampledStatKind_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned kind handle not yet
    // consumed or destroyed, as `Interface::destroy` requires;
    // `kafka_common_metrics_stats_SampledStatKind_new` hands out only owned kind handles.
    unsafe { Interface::destroy(self_ as *mut Interface<dyn SampledStatKind>) }
}

// ---------------------------------------------------------------------------
// SampledStat.Sample
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Sample`]: Java's nested `SampledStat.Sample`.
#[repr(C)]
pub struct kafka_common_metrics_stats_SampledStat_Sample_t {
    _private: [u8; 0],
}

/// The sample behind a handle.
///
/// # Safety
///
/// `self_` must be a valid sample handle, alive and not mutated for `'a`.
unsafe fn sample_ref<'a>(self_: *const kafka_common_metrics_stats_SampledStat_Sample_t) -> &'a Sample {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle alive for `'a` and
    // not mutated during it: an owned one, the `Box::into_raw` of a `Sample` in its constructors,
    // or one a kind's `update` or `combine` lends for its call, a `&mut Sample` or `&Sample` cast;
    // so the shared reference is sound for `'a`.
    unsafe { &*(self_ as *const Sample) }
}

/// The sample behind a handle, for a setter.
///
/// # Safety
///
/// `self_` must be a valid sample handle, alive for `'a` and used by no other
/// call during it, and not one of the read-only samples a kind's `combine`
/// receives.
unsafe fn sample_mut<'a>(self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t) -> &'a mut Sample {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle alive for `'a`, used
    // by no other call during it and not one of the read-only samples `combine` lends: an owned
    // one, the `Box::into_raw` of a `Sample` in its constructors, or the one a kind's `update`
    // lends for its call, a `&mut Sample` cast; so the exclusive reference aliases nothing for
    // `'a`.
    unsafe { &mut *(self_ as *mut Sample) }
}

/// `new Sample(double initialValue, long now)`.
///
/// # Parameters
///
/// - `initial_value` - The value the sample starts from and resets to.
/// - `now` - The time the sample starts, in milliseconds.
///
/// # Safety
///
/// The returned handle is owned, freed with
/// `kafka_common_metrics_stats_SampledStat_Sample_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_new(
    initial_value: f64,
    now: i64,
) -> *mut kafka_common_metrics_stats_SampledStat_Sample_t {
    Box::into_raw(Box::new(Sample::new(initial_value, now))) as *mut kafka_common_metrics_stats_SampledStat_Sample_t
}

/// `new Sample(double initialValue, long now, long timeWindowMs)`.
///
/// # Parameters
///
/// - `initial_value` - The value the sample starts from and resets to.
/// - `now` - The time the sample starts, in milliseconds.
/// - `time_window_ms` - The sample's own time window, -1 for the config's.
///
/// # Safety
///
/// The returned handle is owned, freed with
/// `kafka_common_metrics_stats_SampledStat_Sample_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_with_time_window_ms(
    initial_value: f64,
    now: i64,
    time_window_ms: i64,
) -> *mut kafka_common_metrics_stats_SampledStat_Sample_t {
    Box::into_raw(Box::new(Sample::with_time_window_ms(initial_value, now, time_window_ms)))
        as *mut kafka_common_metrics_stats_SampledStat_Sample_t
}

/// `reset(long now)`: clears the sample to start recording again at `now`.
///
/// # Parameters
///
/// - `now` - The time the sample restarts, in milliseconds.
///
/// # Safety
///
/// `self_` must be a valid sample handle that no other call uses at the
/// same time, and not one of the read-only samples a kind's `combine`
/// receives.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_reset(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    now: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle that no other call
    // uses at the same time and not one of the read-only samples `combine` lends, as `sample_mut`
    // requires; the exclusive reference is used only within this call.
    unsafe { sample_mut(self_) }.reset(now)
}

/// `isComplete(long timeMs, MetricConfig config)`.
///
/// # Parameters
///
/// - `time_ms` - The current time in milliseconds.
/// - `config` - The metric configuration.
///
/// # Returns
///
/// 1 if the sample's event or time window has elapsed, 0 otherwise.
///
/// # Safety
///
/// `self_` must be a valid sample handle and `config` a valid metric-config
/// handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_is_complete(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
    time_ms: i64,
    config: *const kafka_common_metrics_MetricConfig_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires, and `config` a valid metric-config handle, as `metric_config_ref` requires; both
    // references are used only within this call.
    i8::from(unsafe { sample_ref(self_) }.is_complete(time_ms, unsafe { metric_config_ref(config) }))
}

/// Returns the value the sample resets to.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_initial_value(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires; the value is copied out.
    unsafe { sample_ref(self_) }.initial_value()
}

/// Returns the number of events recorded into the sample.
///
/// A caught Rust panic returns -1, which no count is.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_event_count(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires; the value is copied out.
    unsafe { sample_ref(self_) }.event_count()
}

/// Returns when the sample started, in milliseconds.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_start_time_ms(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires; the value is copied out.
    unsafe { sample_ref(self_) }.start_time_ms()
}

/// Returns when the last event was recorded into the sample, in milliseconds.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_last_event_ms(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires; the value is copied out.
    unsafe { sample_ref(self_) }.last_event_ms()
}

/// Returns the accumulated value of the sample.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_value(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> f64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires; the value is copied out.
    unsafe { sample_ref(self_) }.value()
}

/// Returns the sample's own time window, `-1` for the config's.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_time_window_ms(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref`
    // requires; the value is copied out.
    unsafe { sample_ref(self_) }.time_window_ms()
}

/// Sets the number of events recorded into the sample.
///
/// # Parameters
///
/// - `event_count` - The number of events.
///
/// # Safety
///
/// `self_` must be a valid sample handle that no other call uses at the
/// same time, and not one of the read-only samples a kind's `combine`
/// receives.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_set_event_count(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    event_count: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle that no other call
    // uses at the same time and not one of the read-only samples `combine` lends, as `sample_mut`
    // requires; the exclusive reference is used only within this call.
    unsafe { sample_mut(self_) }.set_event_count(event_count)
}

/// Sets when the last event was recorded into the sample.
///
/// # Parameters
///
/// - `last_event_ms` - The time of the last event, in milliseconds.
///
/// # Safety
///
/// `self_` must be a valid sample handle that no other call uses at the
/// same time, and not one of the read-only samples a kind's `combine`
/// receives.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_set_last_event_ms(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    last_event_ms: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle that no other call
    // uses at the same time and not one of the read-only samples `combine` lends, as `sample_mut`
    // requires; the exclusive reference is used only within this call.
    unsafe { sample_mut(self_) }.set_last_event_ms(last_event_ms)
}

/// Sets the accumulated value of the sample.
///
/// # Parameters
///
/// - `value` - The accumulated value.
///
/// # Safety
///
/// `self_` must be a valid sample handle that no other call uses at the
/// same time, and not one of the read-only samples a kind's `combine`
/// receives.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_set_value(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
    value: f64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle that no other call
    // uses at the same time and not one of the read-only samples `combine` lends, as `sample_mut`
    // requires; the exclusive reference is used only within this call.
    unsafe { sample_mut(self_) }.set_value(value)
}

/// Returns `toString()`, as a string.
///
/// # Safety
///
/// `self_` must be a valid sample handle.
///
/// The returned string is owned by the caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_to_string(
    self_: *const kafka_common_metrics_stats_SampledStat_Sample_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sample handle, as `sample_ref` requires; the string is built during the call.
    into_c_string(&unsafe { sample_ref(self_) }.to_string())
}

/// Frees an owned sample handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_stats_SampledStat_Sample_destroy(
    self_: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // sample handle not yet destroyed, i.e. the `Box::into_raw` of a `Sample` in its
        // constructors, so `from_raw` reclaims that allocation, once; a sample a kind lends is not
        // owned, so it is never passed here.
        drop(unsafe { Box::from_raw(self_ as *mut Sample) });
    }
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::metrics::measurable::kafka_common_metrics_Measurable_measure;
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::stat::kafka_common_metrics_Stat_record;
    use crate::ffi::util::{kafka_List_get, kafka_List_size};

    /// A C "max" kind: the sample keeps the largest value, `combine` takes
    /// the largest sample.
    unsafe extern "C" fn max_update(
        _self: *mut c_void,
        sample: *mut kafka_common_metrics_stats_SampledStat_Sample_t,
        _config: *const kafka_common_metrics_MetricConfig_t,
        value: f64,
        _time_ms: i64,
    ) {
        // SAFETY: Test code: `sample` is the `&mut Sample` cast `CSampledStatKind::update` lends
        // for this call, used by nothing else meanwhile.
        unsafe {
            let current = kafka_common_metrics_stats_SampledStat_Sample_value(sample);
            kafka_common_metrics_stats_SampledStat_Sample_set_value(sample, current.max(value));
        }
    }

    unsafe extern "C" fn max_combine(
        self_: *mut c_void,
        samples: *const kafka_List_t,
        _config: *const kafka_common_metrics_MetricConfig_t,
        _now: i64,
    ) -> f64 {
        let mut max = f64::NEG_INFINITY;
        // SAFETY: Test code: `self_` is the `combines` counter the test registered, which outlives
        // the stat and is read by nothing else during the call; `samples` is the borrowed list
        // `CSampledStatKind::combine` lends, whose read-only samples are only read here.
        unsafe {
            *(self_ as *mut i32) += 1;
            for i in 0..kafka_List_size(samples) {
                let sample = kafka_List_get(samples, i) as *const kafka_common_metrics_stats_SampledStat_Sample_t;
                max = max.max(kafka_common_metrics_stats_SampledStat_Sample_value(sample));
            }
        }
        max
    }

    unsafe extern "C" fn yes(_self: *mut c_void) -> i8 {
        1
    }

    /// A Rust kind summing values, standing for the crate-private
    /// `WindowedSumKind`.
    struct SumKind;

    impl SampledStatKind for SumKind {
        fn update(&self, sample: &mut Sample, _config: &MetricConfig, value: f64, _time_ms: i64) {
            sample.set_value(sample.value() + value);
        }

        fn combine(&self, samples: &[Sample], _config: &MetricConfig, _now: i64) -> f64 {
            samples.iter().map(Sample::value).sum()
        }

        fn is_windowed_sum(&self) -> bool {
            true
        }
    }

    #[test]
    fn a_c_kind_drives_a_sampled_stat_and_null_predicates_are_the_java_default() {
        let mut combines = 0i32;
        let config = kafka_common_metrics_MetricConfig_new();
        // SAFETY: Test code: `kind` comes from `kafka_common_metrics_stats_SampledStatKind_new`
        // over the `combines` local, which outlives it, and
        // `kafka_common_metrics_stats_SampledStat_new` consumes it, after which it is not used;
        // `stat` and `config` come from their constructors and are destroyed once, after their last
        // use.
        unsafe {
            let kind = kafka_common_metrics_stats_SampledStatKind_new(
                &mut combines as *mut i32 as *mut c_void,
                max_update,
                max_combine,
                None,
                Some(yes),
            );
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_count(kind), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_sum(kind), 1);

            let stat = kafka_common_metrics_stats_SampledStat_new(f64::NEG_INFINITY, kind);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SampledStat__as_Stat(stat), config, 3.0, 0);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SampledStat__as_Stat(stat), config, 9.0, 1);
            kafka_common_metrics_Stat_record(kafka_common_metrics_stats_SampledStat__as_Stat(stat), config, 5.0, 2);
            assert_eq!(
                kafka_common_metrics_Measurable_measure(
                    kafka_common_metrics_stats_SampledStat__as_Measurable(stat),
                    config,
                    2
                ),
                9.0
            );
            assert!(!kafka_common_metrics_stats_SampledStat__as_MeasurableStat(stat).is_null());
            assert_eq!(combines, 1);
            kafka_common_metrics_stats_SampledStat_destroy(stat);
            kafka_common_metrics_stats_SampledStat_destroy(ptr::null_mut());
            kafka_common_metrics_stats_SampledStatKind_destroy(ptr::null_mut());
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }

    #[test]
    fn invokers_reach_a_rust_kind_through_sample_handles() {
        let config = kafka_common_metrics_MetricConfig_new();
        // SAFETY: Test code: `kind` is an owned `Interface` from `Interface::owned` and is
        // destroyed once, near the end; `first` and `second` come from the `Sample` constructors,
        // are used by one call at a time and are destroyed once; `samples` is a
        // `box_list(.., None)` view over them, destroyed before them; `config` comes from
        // `kafka_common_metrics_MetricConfig_new` and is destroyed once.
        unsafe {
            let kind = Interface::owned(Arc::new(SumKind) as Arc<dyn SampledStatKind>)
                as *mut kafka_common_metrics_stats_SampledStatKind_t;
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_sum(kind), 1);
            assert_eq!(kafka_common_metrics_stats_SampledStatKind_is_windowed_count(kind), 0);

            let first = kafka_common_metrics_stats_SampledStat_Sample_new(0.0, 0);
            let second = kafka_common_metrics_stats_SampledStat_Sample_with_time_window_ms(0.0, 10, 500);
            kafka_common_metrics_stats_SampledStatKind_update(kind, first, config, 2.0, 1);
            kafka_common_metrics_stats_SampledStatKind_update(kind, first, config, 3.0, 2);
            kafka_common_metrics_stats_SampledStatKind_update(kind, second, config, 4.0, 11);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_value(first), 5.0);
            // The kind folds values only; `SampledStat.record` keeps the
            // event bookkeeping, so a direct `update` leaves it untouched.
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_event_count(first), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_last_event_ms(first), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_start_time_ms(second), 10);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_time_window_ms(second), 500);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_time_window_ms(first), -1);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_initial_value(first), 0.0);

            let samples = box_list(vec![first as *mut c_void, second as *mut c_void], None);
            assert_eq!(
                kafka_common_metrics_stats_SampledStatKind_combine(kind, samples, config, 11),
                9.0
            );
            kafka_List_destroy(samples);

            // The 500 ms window of `second` elapses at 510.
            assert_eq!(
                kafka_common_metrics_stats_SampledStat_Sample_is_complete(second, 509, config),
                0
            );
            assert_eq!(
                kafka_common_metrics_stats_SampledStat_Sample_is_complete(second, 510, config),
                1
            );

            kafka_common_metrics_stats_SampledStat_Sample_set_event_count(first, 7);
            kafka_common_metrics_stats_SampledStat_Sample_set_last_event_ms(first, 70);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_event_count(first), 7);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_last_event_ms(first), 70);
            kafka_common_metrics_stats_SampledStat_Sample_reset(first, 100);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_value(first), 0.0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_event_count(first), 0);
            assert_eq!(kafka_common_metrics_stats_SampledStat_Sample_start_time_ms(first), 100);

            kafka_common_metrics_stats_SampledStat_Sample_destroy(first);
            kafka_common_metrics_stats_SampledStat_Sample_destroy(second);
            kafka_common_metrics_stats_SampledStat_Sample_destroy(ptr::null_mut());
            kafka_common_metrics_stats_SampledStatKind_destroy(kind);
            kafka_common_metrics_MetricConfig_destroy(config);
        }
    }
}

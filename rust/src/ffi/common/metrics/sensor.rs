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

//! `kafka_common_metrics_Sensor_t`: `org.apache.kafka.common.metrics.Sensor`
//! (CLAUDE.md §4), and `kafka_common_metrics_Sensor_RecordingLevel_t`, its
//! nested `RecordingLevel` enum (§4, "Enums" and "Nested types").
//!
//! A sensor is only ever created by its registry (`Metrics_sensor*`), which
//! keeps it alive: the handle a registry method returns is owned by the C
//! caller and freed with [`kafka_common_metrics_Sensor_destroy`], but the
//! sensor itself stays registered until `Metrics_remove_sensor` or the
//! registry is destroyed, as in Java.

#![expect(non_camel_case_types)]

use crate::ffi::util::into_c_string;
use std::ffi::{CString, c_char};
use std::sync::Arc;

use crate::common::metrics::{RecordingLevel, Sensor};
use crate::ffi::common::deliver_value;
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::metric_name::{kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metrics::compound_stat::{kafka_common_metrics_CompoundStat_t, take_compound_stat};
use crate::ffi::common::metrics::measurable_stat::{kafka_common_metrics_MeasurableStat_t, take_measurable_stat};
use crate::ffi::common::metrics::metric_config::{kafka_common_metrics_MetricConfig_t, metric_config_option};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_List_t, list_elements, owned_c_string};

// ---------------------------------------------------------------------------
// Sensor.RecordingLevel
// ---------------------------------------------------------------------------

/// Opaque handle to a [`RecordingLevel`] value: a static singleton per value,
/// never freed, comparable with `==`.
#[repr(C)]
pub struct kafka_common_metrics_Sensor_RecordingLevel_t {
    _private: [u8; 0],
}

/// The values of [`RecordingLevel`], for a `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_metrics_Sensor_RecordingLevel_e {
    kafka_common_metrics_Sensor_RecordingLevel_INFO,
    kafka_common_metrics_Sensor_RecordingLevel_DEBUG,
    kafka_common_metrics_Sensor_RecordingLevel_TRACE,
}

/// One instance per value; the singletons point into this array.
static VARIANTS: [RecordingLevel; 3] = [RecordingLevel::Info, RecordingLevel::Debug, RecordingLevel::Trace];

fn enum_of(level: RecordingLevel) -> kafka_common_metrics_Sensor_RecordingLevel_e {
    match level {
        RecordingLevel::Info => {
            kafka_common_metrics_Sensor_RecordingLevel_e::kafka_common_metrics_Sensor_RecordingLevel_INFO
        },
        RecordingLevel::Debug => {
            kafka_common_metrics_Sensor_RecordingLevel_e::kafka_common_metrics_Sensor_RecordingLevel_DEBUG
        },
        RecordingLevel::Trace => {
            kafka_common_metrics_Sensor_RecordingLevel_e::kafka_common_metrics_Sensor_RecordingLevel_TRACE
        },
    }
}

/// The singleton standing for `level`.
pub(crate) fn recording_level_singleton(level: RecordingLevel) -> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    &VARIANTS[enum_of(level) as usize] as *const RecordingLevel as *const kafka_common_metrics_Sensor_RecordingLevel_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `level` must be a singleton returned by this module.
pub(crate) unsafe fn recording_level_of(level: *const kafka_common_metrics_Sensor_RecordingLevel_t) -> RecordingLevel {
    // SAFETY: Per this function's `# Safety`, `level` is a singleton returned by this module, i.e.
    // a pointer to an element of the `static` `VARIANTS` array (`recording_level_singleton`), so it
    // points at a live, initialised `RecordingLevel`, which is `Copy` and read out here.
    unsafe { *(level as *const RecordingLevel) }
}

/// Returns `RecordingLevel.INFO`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_info()
-> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    recording_level_singleton(RecordingLevel::Info)
}

/// Returns `RecordingLevel.DEBUG`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_debug()
-> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    recording_level_singleton(RecordingLevel::Debug)
}

/// Returns `RecordingLevel.TRACE`, a static singleton never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_trace()
-> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    recording_level_singleton(RecordingLevel::Trace)
}

/// Returns the enumerator of a singleton, for a `switch`.
///
/// A caught Rust panic returns `kafka_common_metrics_Sensor_RecordingLevel_INFO`, the first
/// enumerator: the enum has no `UNKNOWN` constant, so the panic cannot be told apart from that
/// value, and is only logged.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard(fallback = kafka_common_metrics_Sensor_RecordingLevel_e::kafka_common_metrics_Sensor_RecordingLevel_INFO)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel__enum(
    self_: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) -> kafka_common_metrics_Sensor_RecordingLevel_e {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `recording_level_of` requires; the value is copied out.
    enum_of(unsafe { recording_level_of(self_) })
}

/// Returns the constant's name (Java's inherited `Enum.toString()`).
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The returned string is owned by the
/// caller and freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_to_string(
    self_: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `recording_level_of` requires; the value is copied out and its name copied into an owned string.
    into_c_string(&unsafe { recording_level_of(self_) }.to_string())
}

/// Returns the permanent and immutable id of the level: `0` for `INFO`, `1` for `DEBUG`, `2` for
/// `TRACE`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_id(
    self_: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) -> i16 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `recording_level_of` requires; the value is copied out.
    unsafe { recording_level_of(self_) }.id()
}

/// Returns an English description of the level.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module. The returned string is static, never
/// freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_name(
    self_: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `recording_level_of` requires; the value is copied out. The names returned are C string
    // literals, never freed, as the rustdoc states.
    match unsafe { recording_level_of(self_) } {
        RecordingLevel::Info => c"INFO".as_ptr(),
        RecordingLevel::Debug => c"DEBUG".as_ptr(),
        RecordingLevel::Trace => c"TRACE".as_ptr(),
    }
}

/// Returns the level with the id `id`.
///
/// # Parameters
///
/// - `id` - The level's id.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if `id` is not between `0` and `2`;
///   `NULL` is returned.
///
/// # Safety
///
/// `out_err` must be a valid slot pointing at `NULL`. The returned singleton is static, never
/// freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_for_id(
    id: i16,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    match RecordingLevel::for_id(id) {
        Ok(level) => recording_level_singleton(level),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Case insensitive lookup by protocol name
///
/// # Parameters
///
/// - `name` - The level's name, in any case.
///
/// # Returns
///
/// The level with that name.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if no level has that name; `NULL` is
///   returned.
///
/// # Safety
///
/// `name` must be a NUL-terminated string and `out_err` a valid slot pointing at `NULL`. The
/// returned singleton is static, never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_for_name(
    name: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    match RecordingLevel::for_name(&unsafe { c_str_to_string(name) }) {
        Ok(level) => recording_level_singleton(level),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Returns `1` if a sensor at this level records under a config at the level `config_id`, `0`
/// otherwise.
///
/// # Parameters
///
/// - `config_id` - The id of the configured recording level.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_should_record(
    self_: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    config_id: i16,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a singleton returned by this module, as
    // `recording_level_of` requires; the value is copied out.
    i8::from(unsafe { recording_level_of(self_) }.should_record(config_id))
}

// ---------------------------------------------------------------------------
// Sensor
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Sensor`], shared with its registry.
#[repr(C)]
pub struct kafka_common_metrics_Sensor_t {
    _private: [u8; 0],
}

/// What a `kafka_common_metrics_Sensor_t` points at: the registry's sensor
/// plus the C copy of its name the borrowed getter hands out.
struct SensorInner {
    sensor: Arc<Sensor>,
    name_c: CString,
}

/// Hands `sensor` to C as an owned handle, freed with
/// `kafka_common_metrics_Sensor_destroy`.
pub(crate) fn box_sensor(sensor: Arc<Sensor>) -> *mut kafka_common_metrics_Sensor_t {
    let name_c = owned_c_string(sensor.name());
    Box::into_raw(Box::new(SensorInner { sensor, name_c })) as *mut kafka_common_metrics_Sensor_t
}

/// What a handle points at.
///
/// # Safety
///
/// `self_` must be a valid sensor handle, alive for `'a`.
unsafe fn inner<'a>(self_: *const kafka_common_metrics_Sensor_t) -> &'a SensorInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle alive for `'a`, i.e.
    // the `Box::into_raw` of a `SensorInner` in `box_sensor` (every sensor handle is owned: no view
    // of this type is handed out). No call mutates a `SensorInner`, and its `Sensor` is used only
    // through `&self`, so the shared reference is sound for `'a`.
    unsafe { &*(self_ as *const SensorInner) }
}

/// The sensor behind a handle.
///
/// # Safety
///
/// `sensor` must be a valid sensor handle, alive for `'a`.
pub(crate) unsafe fn sensor_ref<'a>(sensor: *const kafka_common_metrics_Sensor_t) -> &'a Arc<Sensor> {
    // SAFETY: Per this function's `# Safety`, `sensor` is a valid sensor handle alive for `'a`, as
    // `inner` requires; the reference returned borrows the handle's `Arc` for the same `'a`.
    &unsafe { inner(sensor) }.sensor
}

/// The sensors behind a list of `const kafka_common_metrics_Sensor_t *`
/// elements; null reads as empty.
///
/// # Safety
///
/// `sensors` must be null or a valid list of sensor handles.
pub(crate) unsafe fn list_sensors(sensors: *const kafka_List_t) -> Vec<Arc<Sensor>> {
    // SAFETY: Per this function's `# Safety`, `sensors` is null or a valid list handle, alive and
    // not mutated during this call, as `list_elements` requires; the slice is used only within this
    // call, its sensors being cloned into owned `Arc`s before return.
    unsafe { list_elements(sensors) }
        .iter()
        // SAFETY: Per this function's `# Safety`, every element of `sensors` is a valid sensor
        // handle, as `sensor_ref` requires; the reference lives only until `Arc::clone` copies the
        // `Arc`.
        .map(|&element| Arc::clone(unsafe { sensor_ref(element as *const kafka_common_metrics_Sensor_t) }))
        .collect()
}

/// Returns the name this sensor is registered with. This name will be unique among all registered
/// sensors.
///
/// # Safety
///
/// `self_` must be a valid sensor handle. The returned string is borrowed from `self_`, valid
/// until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_name(
    self_: *const kafka_common_metrics_Sensor_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `inner`
    // requires. The string returned is the handle's own `name_c`, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner(self_) }.name_c.as_ptr()
}

/// `shouldRecord()`.
///
/// # Returns
///
/// `1` if the sensor's record level indicates that the metric will be recorded, `0` otherwise
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_should_record(self_: *const kafka_common_metrics_Sensor_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { sensor_ref(self_) }.should_record())
}

/// Record an occurrence, this is just short-hand for
/// `kafka_common_metrics_Sensor_record_with_value(1.0)`.
///
/// # Errors
///
/// - `kafka_common_Error_is_quota_violation_error` if recording this value moves a metric beyond
///   its configured maximum or minimum bound
///
/// # Safety
///
/// `self_` must be a valid sensor handle. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record(
    self_: *const kafka_common_metrics_Sensor_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.record() {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Record a value with this sensor
///
/// # Parameters
///
/// - `value` - The value to record
///
/// # Errors
///
/// - `kafka_common_Error_is_quota_violation_error` if recording this value moves a metric beyond
///   its configured maximum or minimum bound
///
/// # Safety
///
/// `self_` must be a valid sensor handle. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record_with_value(
    self_: *const kafka_common_metrics_Sensor_t,
    value: f64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.record_with_value(value) {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Record a value at a known time. This method is slightly faster than
/// `kafka_common_metrics_Sensor_record_with_value` since it will reuse the time stamp.
///
/// # Parameters
///
/// - `value` - The value we are recording
/// - `time_ms` - The current POSIX time in milliseconds
///
/// # Errors
///
/// - `kafka_common_Error_is_quota_violation_error` if recording this value moves a metric beyond
///   its configured maximum or minimum bound
///
/// # Safety
///
/// `self_` must be a valid sensor handle. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record_with_value_time_ms(
    self_: *const kafka_common_metrics_Sensor_t,
    value: f64,
    time_ms: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.record_with_value_time_ms(value, time_ms) {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Record a value at a known time. This method is slightly faster than
/// `kafka_common_metrics_Sensor_record_with_value` since it will reuse the time stamp.
///
/// # Parameters
///
/// - `value` - The value we are recording
/// - `time_ms` - The current POSIX time in milliseconds
/// - `check_quotas` - Indicate if quota must be enforced (`1`) or not (`0`)
///
/// # Errors
///
/// - `kafka_common_Error_is_quota_violation_error` if recording this value moves a metric beyond
///   its configured maximum or minimum bound
///
/// # Safety
///
/// `self_` must be a valid sensor handle. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record_with_value_time_ms_check_quotas(
    self_: *const kafka_common_metrics_Sensor_t,
    value: f64,
    time_ms: i64,
    check_quotas: i8,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.record_with_value_time_ms_check_quotas(value, time_ms, check_quotas != 0) {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Check if we have violated our quota for any metric that has a configured quota
///
/// # Errors
///
/// - `kafka_common_Error_is_quota_violation_error` if a metric's value is outside its quota.
///
/// # Safety
///
/// `self_` must be a valid sensor handle. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_check_quotas(
    self_: *const kafka_common_metrics_Sensor_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.check_quotas() {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Checks if we have violated our quota for any metric that has a configured quota, as of
/// `time_ms`.
///
/// # Parameters
///
/// - `time_ms` - The POSIX time in milliseconds to measure the metrics at.
///
/// # Errors
///
/// - `kafka_common_Error_is_quota_violation_error` if a metric's value is outside its quota.
///
/// # Safety
///
/// `self_` must be a valid sensor handle. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_check_quotas_with_time_ms(
    self_: *const kafka_common_metrics_Sensor_t,
    time_ms: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.check_quotas_with_time_ms(time_ms) {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Returns the outcome of an `add` call as `0` or `1`, or stores the owned
/// translation of its error in `*out_err`.
///
/// # Safety
///
/// `out_err` must be valid for one write and point at null.
unsafe fn deliver_added(added: Result<bool, crate::common::Error>, out_err: *mut *mut kafka_common_Error_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(added, out_err, i8::from) }
}

/// Register a metric with this sensor
///
/// # Parameters
///
/// - `metric_name` - The name of the metric
/// - `stat` - The statistic to keep
///
/// # Returns
///
/// `1` if metric is added to sensor, `0` if sensor is expired
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if a metric of the same name is already
///   registered in the registry by another sensor; `0` is returned.
///
/// # Safety
///
/// `self_` must be a valid sensor handle, `metric_name` a valid metric-name handle (copied), and
/// `stat` a valid measurable-stat handle: an owned one is consumed and not used again by the
/// caller, the view of a stat class is shared. `out_err` must be a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add_with_metric_name(
    self_: *const kafka_common_metrics_Sensor_t,
    metric_name: *const kafka_common_MetricName_t,
    stat: *mut kafka_common_metrics_MeasurableStat_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
    // `metric_name_ref` requires; the name is cloned, so nothing borrows the handle after this
    // call.
    let metric_name = unsafe { metric_name_ref(metric_name) }.clone();
    // SAFETY: Per this function's `# Safety`, `stat` is a valid measurable-stat handle that the
    // caller does not use again when it was owned, as `take_measurable_stat` requires; an owned one
    // is consumed here and a view's implementation is shared through its `Arc`.
    let stat = unsafe { take_measurable_stat(stat) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    let added = unsafe { sensor_ref(self_) }.add_with_metric_name(metric_name, stat);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer, as
    // `deliver_added` requires.
    unsafe { deliver_added(added, out_err) }
}

/// Register a metric with this sensor
///
/// # Parameters
///
/// - `metric_name` - The name of the metric
/// - `stat` - The statistic to keep
/// - `config` - A special configuration for this metric. If `NULL` use the sensor default
///   configuration.
///
/// # Returns
///
/// `1` if metric is added to sensor, `0` if sensor is expired
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if a metric of the same name is already
///   registered in the registry by another sensor; `0` is returned.
///
/// # Safety
///
/// As `kafka_common_metrics_Sensor_add_with_metric_name`; `config` must be `NULL` or a valid
/// metric-config handle, copied.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add_with_metric_name_config(
    self_: *const kafka_common_metrics_Sensor_t,
    metric_name: *const kafka_common_MetricName_t,
    stat: *mut kafka_common_metrics_MeasurableStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
    // `metric_name_ref` requires; the name is cloned, so nothing borrows the handle after this
    // call.
    let metric_name = unsafe { metric_name_ref(metric_name) }.clone();
    // SAFETY: Per this function's `# Safety`, `stat` is a valid measurable-stat handle that the
    // caller does not use again when it was owned, as `take_measurable_stat` requires; an owned one
    // is consumed here and a view's implementation is shared through its `Arc`.
    let stat = unsafe { take_measurable_stat(stat) };
    // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config handle, as
    // `metric_config_option` requires; it returns an owned `Arc`, so nothing borrows the handle
    // after this call.
    let config = unsafe { metric_config_option(config) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    let added = unsafe { sensor_ref(self_) }.add_with_metric_name_config(metric_name, stat, config);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer,
    // as `deliver_added` requires.
    unsafe { deliver_added(added, out_err) }
}

/// Register a compound statistic with this sensor with no config override
///
/// # Parameters
///
/// - `stat` - The stat to register
///
/// # Returns
///
/// `1` if stat is added to sensor, `0` if sensor is expired
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if one of its metric names is already
///   registered in the registry by another sensor; `0` is returned.
///
/// # Safety
///
/// `self_` must be a valid sensor handle and `stat` a valid compound-stat handle: an owned one is
/// consumed and not used again by the caller, the view of a stat class is shared. `out_err` must be
/// a valid slot pointing at `NULL`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add(
    self_: *const kafka_common_metrics_Sensor_t,
    stat: *mut kafka_common_metrics_CompoundStat_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid compound-stat handle that the
    // caller does not use again when it was owned, as `take_compound_stat` requires; an owned one
    // is consumed here and a view's implementation is shared through its `Arc`.
    let stat = unsafe { take_compound_stat(stat) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    let added = unsafe { sensor_ref(self_) }.add(stat);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer, as `deliver_added`
    // requires.
    unsafe { deliver_added(added, out_err) }
}

/// Register a compound statistic with this sensor which yields multiple measurable quantities (like
/// a histogram)
///
/// # Parameters
///
/// - `stat` - The stat to register
/// - `config` - The configuration for this stat. If `NULL` then the stat will use the default
///   configuration for this sensor.
///
/// # Returns
///
/// `1` if stat is added to sensor, `0` if sensor is expired
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if one of its metric names is already
///   registered in the registry by another sensor; `0` is returned.
///
/// # Safety
///
/// As `kafka_common_metrics_Sensor_add`; `config` must be `NULL` or a valid metric-config handle,
/// copied.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add_with_config(
    self_: *const kafka_common_metrics_Sensor_t,
    stat: *mut kafka_common_metrics_CompoundStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid compound-stat handle that the
    // caller does not use again when it was owned, as `take_compound_stat` requires; an owned one
    // is consumed here and a view's implementation is shared through its `Arc`.
    let stat = unsafe { take_compound_stat(stat) };
    // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config handle, as
    // `metric_config_option` requires; it returns an owned `Arc`, so nothing borrows the handle
    // after this call.
    let config = unsafe { metric_config_option(config) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    let added = unsafe { sensor_ref(self_) }.add_with_config(stat, config);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer, as
    // `deliver_added` requires.
    unsafe { deliver_added(added, out_err) }
}

/// Return if metrics were registered with this sensor.
///
/// # Returns
///
/// `1` if metrics were registered, `0` otherwise
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_has_metrics(self_: *const kafka_common_metrics_Sensor_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { sensor_ref(self_) }.has_metrics())
}

/// Returns `1` if the Sensor is eligible for removal due to inactivity, `0` otherwise.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_has_expired(self_: *const kafka_common_metrics_Sensor_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { sensor_ref(self_) }.has_expired())
}

/// Frees a sensor handle; null is a no-op. The sensor stays registered.
///
/// # Safety
///
/// `self_` must be null or an owned sensor handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_destroy(self_: *mut kafka_common_metrics_Sensor_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // sensor handle not yet destroyed, i.e. the `Box::into_raw` of a `SensorInner` in
        // `box_sensor`, so `from_raw` reclaims that allocation, once. The registry keeps its own
        // `Arc` of the sensor.
        drop(unsafe { Box::from_raw(self_ as *mut SensorInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::common::metric_name::{box_metric_name, kafka_common_MetricName_destroy};
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
    };
    use crate::ffi::common::metrics::metrics::{
        kafka_common_metrics_Metrics_destroy, kafka_common_metrics_Metrics_new, metrics_ref,
    };
    use crate::ffi::common::metrics::stats::avg::{
        kafka_common_metrics_stats_Avg__as_MeasurableStat, kafka_common_metrics_stats_Avg_destroy,
        kafka_common_metrics_stats_Avg_new,
    };
    use crate::ffi::common::metrics::stats::meter::{
        kafka_common_metrics_stats_Meter__as_CompoundStat, kafka_common_metrics_stats_Meter_destroy,
        kafka_common_metrics_stats_Meter_new,
    };
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;

    #[test]
    fn recording_level_singletons_round_trip_and_match_their_enumerator() {
        for (index, &level) in VARIANTS.iter().enumerate() {
            let singleton = recording_level_singleton(level);
            // SAFETY: Test code: every handle passed is a singleton from
            // `recording_level_singleton`, which points into the static `VARIANTS` array and is
            // never freed; the names read are static; `err` is a valid slot pointing at null
            // before each call, and `name` outlives the call reading it.
            unsafe {
                assert_eq!(recording_level_of(singleton), level);
                assert_eq!(kafka_common_metrics_Sensor_RecordingLevel__enum(singleton) as usize, index);
                assert_eq!(kafka_common_metrics_Sensor_RecordingLevel_id(singleton), level.id());
                assert_eq!(
                    CStr::from_ptr(kafka_common_metrics_Sensor_RecordingLevel_name(singleton))
                        .to_str()
                        .unwrap(),
                    level.name()
                );
                let mut err = ptr::null_mut();
                let by_id = kafka_common_metrics_Sensor_RecordingLevel_for_id(level.id(), &mut err);
                assert!(err.is_null());
                assert_eq!(by_id, singleton);
                let name = CString::new(level.name()).unwrap();
                let by_name = kafka_common_metrics_Sensor_RecordingLevel_for_name(name.as_ptr(), &mut err);
                assert!(err.is_null());
                assert_eq!(by_name, singleton);
            }
        }
        assert_eq!(
            kafka_common_metrics_Sensor_RecordingLevel_info(),
            recording_level_singleton(RecordingLevel::Info)
        );
        assert_eq!(
            kafka_common_metrics_Sensor_RecordingLevel_debug(),
            recording_level_singleton(RecordingLevel::Debug)
        );
        assert_eq!(
            kafka_common_metrics_Sensor_RecordingLevel_trace(),
            recording_level_singleton(RecordingLevel::Trace)
        );
        // SAFETY: Test code: `debug` is a singleton from this module, never freed; `err` is a valid
        // slot pointing at null before each call, `unknown` outlives the call reading it, and each
        // error stored in `err` is destroyed once.
        unsafe {
            // DEBUG records under a DEBUG config, not under an INFO one.
            let debug = kafka_common_metrics_Sensor_RecordingLevel_debug();
            assert_eq!(
                kafka_common_metrics_Sensor_RecordingLevel_should_record(debug, RecordingLevel::Debug.id()),
                1
            );
            assert_eq!(
                kafka_common_metrics_Sensor_RecordingLevel_should_record(debug, RecordingLevel::Info.id()),
                0
            );

            let mut err = ptr::null_mut();
            let out = kafka_common_metrics_Sensor_RecordingLevel_for_id(7, &mut err);
            assert!(out.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
            let mut err = ptr::null_mut();
            let unknown = CString::new("VERBOSE").unwrap();
            let out = kafka_common_metrics_Sensor_RecordingLevel_for_name(unknown.as_ptr(), &mut err);
            assert!(out.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
        }
    }

    #[test]
    fn sensor_adds_stats_records_and_shares_the_registry_sensor() {
        let registry = kafka_common_metrics_Metrics_new();
        // SAFETY: Test code: `registry` comes from `kafka_common_metrics_Metrics_new` and is
        // destroyed once, last; each sensor handle comes from `box_sensor` and is destroyed once,
        // after its last use; `name`, `rate_name` and `total_name` come from `box_metric_name` and
        // are destroyed once, after the calls that copy them; the stats passed are the views
        // `__as_MeasurableStat` and `__as_CompoundStat` cache on `avg` and `meter`, which outlive
        // those calls and are destroyed once, after them; `config` comes from
        // `kafka_common_metrics_MetricConfig_new` and is destroyed once; `err` is a valid slot
        // pointing at null before each call, and each error stored in it is destroyed once.
        unsafe {
            let sensor = box_sensor(metrics_ref(registry).sensor("s").unwrap());
            assert_eq!(CStr::from_ptr(kafka_common_metrics_Sensor_name(sensor)).to_str().unwrap(), "s");
            assert_eq!(kafka_common_metrics_Sensor_should_record(sensor), 1);
            assert_eq!(kafka_common_metrics_Sensor_has_metrics(sensor), 0);
            assert_eq!(kafka_common_metrics_Sensor_has_expired(sensor), 0);
            assert!(list_sensors(ptr::null()).is_empty());

            // A stat class's view is shared with the sensor: the class handle stays valid.
            let avg = kafka_common_metrics_stats_Avg_new();
            let name = box_metric_name(metrics_ref(registry).metric_name("avg", "g"));
            let mut err = ptr::null_mut();
            let added = kafka_common_metrics_Sensor_add_with_metric_name(
                sensor,
                name,
                kafka_common_metrics_stats_Avg__as_MeasurableStat(avg) as *mut _,
                &mut err,
            );
            assert!(err.is_null());
            assert_eq!(added, 1);
            assert_eq!(kafka_common_metrics_Sensor_has_metrics(sensor), 1);

            // Re-adding a name this sensor already has is Java's
            // `metrics.containsKey(metricName)` branch: true, nothing changes.
            let config = kafka_common_metrics_MetricConfig_new();
            let added = kafka_common_metrics_Sensor_add_with_metric_name_config(
                sensor,
                name,
                kafka_common_metrics_stats_Avg__as_MeasurableStat(avg) as *mut _,
                config,
                &mut err,
            );
            assert!(err.is_null());
            assert_eq!(added, 1);
            // A name another sensor registered is Java's IllegalArgumentException
            // from the registry.
            let other = box_sensor(metrics_ref(registry).sensor("other").unwrap());
            let added = kafka_common_metrics_Sensor_add_with_metric_name_config(
                other,
                name,
                kafka_common_metrics_stats_Avg__as_MeasurableStat(avg) as *mut _,
                config,
                &mut err,
            );
            assert_eq!(added, 0);
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();
            kafka_common_metrics_MetricConfig_destroy(config);
            kafka_common_MetricName_destroy(name);

            kafka_common_metrics_Sensor_record(sensor, &mut err);
            assert!(err.is_null());
            kafka_common_metrics_Sensor_record_with_value(sensor, 3.0, &mut err);
            assert!(err.is_null());
            kafka_common_metrics_Sensor_record_with_value_time_ms(sensor, 5.0, 10, &mut err);
            assert!(err.is_null());
            let avg_config = metrics_ref(registry).config().clone();
            let metric = metrics_ref(registry)
                .metric(&metrics_ref(registry).metric_name("avg", "g"))
                .unwrap();
            assert_eq!(metric.measurable().unwrap().measure(&avg_config, 10), 3.0);
            kafka_common_metrics_stats_Avg_destroy(avg);

            // No quota configured: checkQuotas passes.
            kafka_common_metrics_Sensor_check_quotas(sensor, &mut err);
            assert!(err.is_null());
            kafka_common_metrics_Sensor_check_quotas_with_time_ms(sensor, 10, &mut err);
            assert!(err.is_null());

            // A compound stat registers both of its metrics.
            let rate_name = box_metric_name(metrics_ref(registry).metric_name("rate", "g"));
            let total_name = box_metric_name(metrics_ref(registry).metric_name("total", "g"));
            let meter = kafka_common_metrics_stats_Meter_new(rate_name, total_name);
            kafka_common_MetricName_destroy(rate_name);
            kafka_common_MetricName_destroy(total_name);
            let added = kafka_common_metrics_Sensor_add(
                sensor,
                kafka_common_metrics_stats_Meter__as_CompoundStat(meter) as *mut _,
                &mut err,
            );
            assert!(err.is_null());
            assert_eq!(added, 1);
            // Re-adding it to this sensor skips the names it already has and
            // returns true; on another sensor the registry rejects them.
            let added = kafka_common_metrics_Sensor_add_with_config(
                sensor,
                kafka_common_metrics_stats_Meter__as_CompoundStat(meter) as *mut _,
                ptr::null(),
                &mut err,
            );
            assert!(err.is_null());
            assert_eq!(added, 1);
            let added = kafka_common_metrics_Sensor_add_with_config(
                other,
                kafka_common_metrics_stats_Meter__as_CompoundStat(meter) as *mut _,
                ptr::null(),
                &mut err,
            );
            assert_eq!(added, 0);
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
            kafka_common_metrics_Sensor_destroy(other);
            kafka_common_metrics_stats_Meter_destroy(meter);
            // The registry's built-in `count` metric plus avg, rate and total.
            assert_eq!(metrics_ref(registry).metrics().len(), 4);

            // Destroying the handle leaves the sensor registered.
            kafka_common_metrics_Sensor_destroy(sensor);
            kafka_common_metrics_Sensor_destroy(ptr::null_mut());
            assert!(metrics_ref(registry).get_sensor("s").is_some());
            kafka_common_metrics_Metrics_destroy(registry);
        }
    }

    /// `record` checks the quotas and reports a violation through `out_err`;
    /// `check_quotas = 0` records without checking.
    #[test]
    fn sensor_record_reports_a_quota_violation() {
        use crate::common::metrics::stats::Value;
        use crate::common::metrics::{MetricConfig, Metrics, Quota};
        use crate::ffi::error_predicates::kafka_common_Error_is_quota_violation_error;
        let metrics = Metrics::with_default_config(Arc::new(MetricConfig::new().set_quota(Quota::upper_bound(5.0))));
        let rust_sensor = metrics.sensor("s").unwrap();
        rust_sensor
            .add_with_metric_name(metrics.metric_name("value", "g"), Box::new(Value::new()))
            .unwrap();
        let sensor = box_sensor(rust_sensor);
        // SAFETY: Test code: `sensor` comes from `box_sensor` and is destroyed once, last; `err` is
        // a valid slot pointing at null before each call, and each error stored in it is destroyed
        // once.
        unsafe {
            let mut err = ptr::null_mut();
            kafka_common_metrics_Sensor_record_with_value_time_ms(sensor, 4.0, 1, &mut err);
            assert!(err.is_null());
            kafka_common_metrics_Sensor_record_with_value_time_ms_check_quotas(sensor, 10.0, 2, 0, &mut err);
            assert!(err.is_null());
            kafka_common_metrics_Sensor_record_with_value_time_ms(sensor, 10.0, 3, &mut err);
            assert_eq!(kafka_common_Error_is_quota_violation_error(err), 1);
            kafka_common_Error_destroy(err);
            let mut err = ptr::null_mut();
            kafka_common_metrics_Sensor_record_with_value_time_ms_check_quotas(sensor, 10.0, 4, 1, &mut err);
            assert_eq!(kafka_common_Error_is_quota_violation_error(err), 1);
            kafka_common_Error_destroy(err);
            kafka_common_metrics_Sensor_destroy(sensor);
        }
    }
}

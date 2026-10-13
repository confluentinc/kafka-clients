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

use std::ffi::{CString, c_char};
use std::ptr;
use std::sync::Arc;

use crate::common::metrics::{RecordingLevel, Sensor};
use crate::ffi::common::metric_name::{kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metrics::compound_stat::{kafka_common_metrics_CompoundStat_t, take_compound_stat};
use crate::ffi::common::metrics::measurable_stat::{kafka_common_metrics_MeasurableStat_t, take_measurable_stat};
use crate::ffi::common::metrics::metric_config::{kafka_common_metrics_MetricConfig_t, metric_config_option};
use crate::ffi::common::{box_error, kafka_common_Error_t};
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

/// `RecordingLevel.INFO`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_info()
-> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    recording_level_singleton(RecordingLevel::Info)
}

/// `RecordingLevel.DEBUG`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_debug()
-> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    recording_level_singleton(RecordingLevel::Debug)
}

/// `RecordingLevel.TRACE`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_trace()
-> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    recording_level_singleton(RecordingLevel::Trace)
}

/// The enumerator of a singleton, for a `switch`.
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

/// `id`: the wire value, `0` for `INFO`, `1` for `DEBUG`, `2` for `TRACE`.
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

/// `name`: the Java constant name, a static string never freed.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
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

/// `RecordingLevel.forId(short id)`: stores the singleton in `out_for_id`,
/// or returns the owned `IllegalArgumentException` translation for an
/// unknown id.
///
/// # Safety
///
/// `out_for_id` must be a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_for_id(
    id: i16,
    out_for_id: *mut *const kafka_common_metrics_Sensor_RecordingLevel_t,
) -> *mut kafka_common_Error_t {
    match RecordingLevel::for_id(id) {
        Ok(level) => {
            // SAFETY: Per this function's `# Safety`, `out_for_id` is a valid pointer, so the write
            // is sound; the singleton written points into the static `VARIANTS` array and is never
            // freed.
            unsafe { *out_for_id = recording_level_singleton(level) };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `RecordingLevel.forName(String name)`: stores the singleton in
/// `out_for_name`, or returns the owned `IllegalArgumentException`
/// translation for an unknown name.
///
/// # Safety
///
/// `name` must be a NUL-terminated string and `out_for_name` a valid
/// pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_RecordingLevel_for_name(
    name: *const c_char,
    out_for_name: *mut *const kafka_common_metrics_Sensor_RecordingLevel_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    match RecordingLevel::for_name(&unsafe { c_str_to_string(name) }) {
        Ok(level) => {
            // SAFETY: Per this function's `# Safety`, `out_for_name` is a valid pointer, so the
            // write is sound; the singleton written points into the static `VARIANTS` array and is
            // never freed.
            unsafe { *out_for_name = recording_level_singleton(level) };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `shouldRecord(int configId)`: whether a sensor at this level records
/// under a config at level `config_id` (one of the `id`s).
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

/// What a [`kafka_common_metrics_Sensor_t`] points at: the registry's sensor
/// plus the C copy of its name the borrowed getter hands out.
struct SensorInner {
    sensor: Arc<Sensor>,
    name_c: CString,
}

/// Hands `sensor` to C as an owned handle, freed with
/// [`kafka_common_metrics_Sensor_destroy`].
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

/// `name()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
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

/// `record()`: an occurrence, as the value `1.0`.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record(self_: *const kafka_common_metrics_Sensor_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    unsafe { sensor_ref(self_) }.record();
}

/// `record(double value)`.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record_with_value(
    self_: *const kafka_common_metrics_Sensor_t,
    value: f64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    unsafe { sensor_ref(self_) }.record_with_value(value);
}

/// `record(double value, long timeMs)`.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_record_with_value_time_ms(
    self_: *const kafka_common_metrics_Sensor_t,
    value: f64,
    time_ms: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    unsafe { sensor_ref(self_) }.record_with_value_time_ms(value, time_ms);
}

/// `checkQuotas()`: returns the owned `QuotaViolationException` translation
/// when a quota is violated, null otherwise.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_check_quotas(
    self_: *const kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.check_quotas() {
        Ok(()) => ptr::null_mut(),
        Err(error) => box_error(error),
    }
}

/// `checkQuotas(long timeMs)`.
///
/// # Safety
///
/// `self_` must be a valid sensor handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_check_quotas_with_time_ms(
    self_: *const kafka_common_metrics_Sensor_t,
    time_ms: i64,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    match unsafe { sensor_ref(self_) }.check_quotas_with_time_ms(time_ms) {
        Ok(()) => ptr::null_mut(),
        Err(error) => box_error(error),
    }
}

/// Stores the outcome of an `add` call in `out` as `0` or `1`, or returns
/// the owned translation of its error.
///
/// # Safety
///
/// `out` must be a valid pointer.
unsafe fn deliver_added(added: Result<bool, crate::common::Error>, out: *mut i8) -> *mut kafka_common_Error_t {
    match added {
        Ok(added) => {
            // SAFETY: Per this function's `# Safety`, `out` is a valid pointer, so the write is
            // sound.
            unsafe { *out = i8::from(added) };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `add(MetricName metricName, MeasurableStat stat)`: `metric_name` is
/// copied, `stat` is consumed (an owned handle is freed, the view of a stat
/// class is shared). Stores whether the metric was added (`0` when the
/// sensor has expired) in `out_add_with_metric_name`, or returns the owned
/// `IllegalArgumentException` translation when the name is already
/// registered.
///
/// # Safety
///
/// `self_` must be a valid sensor handle, `metric_name` a valid metric-name
/// handle, `stat` a valid measurable-stat handle, not used again by the
/// caller when it was owned, and `out_add_with_metric_name` a valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add_with_metric_name(
    self_: *const kafka_common_metrics_Sensor_t,
    metric_name: *const kafka_common_MetricName_t,
    stat: *mut kafka_common_metrics_MeasurableStat_t,
    out_add_with_metric_name: *mut i8,
) -> *mut kafka_common_Error_t {
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
    // SAFETY: Per this function's `# Safety`, `out_add_with_metric_name` is a valid pointer, as
    // `deliver_added` requires.
    unsafe { deliver_added(added, out_add_with_metric_name) }
}

/// `add(MetricName metricName, MeasurableStat stat, MetricConfig config)`:
/// `config` is copied, null meaning the sensor's own, as Java's null.
///
/// # Safety
///
/// As [`kafka_common_metrics_Sensor_add_with_metric_name`]; `config` must be
/// null or a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add_with_metric_name_config(
    self_: *const kafka_common_metrics_Sensor_t,
    metric_name: *const kafka_common_MetricName_t,
    stat: *mut kafka_common_metrics_MeasurableStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    out_add_with_metric_name_config: *mut i8,
) -> *mut kafka_common_Error_t {
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
    // SAFETY: Per this function's `# Safety`, `out_add_with_metric_name_config` is a valid pointer,
    // as `deliver_added` requires.
    unsafe { deliver_added(added, out_add_with_metric_name_config) }
}

/// `add(CompoundStat stat)`: `stat` is consumed (an owned handle is freed,
/// the view of a stat class is shared). Stores whether the stat was added
/// in `out_add`, or returns the owned `IllegalArgumentException` translation
/// when one of its metric names is already registered.
///
/// # Safety
///
/// `self_` must be a valid sensor handle, `stat` a valid compound-stat
/// handle, not used again by the caller when it was owned, and `out_add` a
/// valid pointer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add(
    self_: *const kafka_common_metrics_Sensor_t,
    stat: *mut kafka_common_metrics_CompoundStat_t,
    out_add: *mut i8,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `stat` is a valid compound-stat handle that the
    // caller does not use again when it was owned, as `take_compound_stat` requires; an owned one
    // is consumed here and a view's implementation is shared through its `Arc`.
    let stat = unsafe { take_compound_stat(stat) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid sensor handle, as `sensor_ref`
    // requires; the reference is used only within this call.
    let added = unsafe { sensor_ref(self_) }.add(stat);
    // SAFETY: Per this function's `# Safety`, `out_add` is a valid pointer, as `deliver_added`
    // requires.
    unsafe { deliver_added(added, out_add) }
}

/// `add(CompoundStat stat, MetricConfig config)`: `config` is copied, null
/// meaning the sensor's own.
///
/// # Safety
///
/// As [`kafka_common_metrics_Sensor_add`]; `config` must be null or a valid
/// metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Sensor_add_with_config(
    self_: *const kafka_common_metrics_Sensor_t,
    stat: *mut kafka_common_metrics_CompoundStat_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    out_add_with_config: *mut i8,
) -> *mut kafka_common_Error_t {
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
    // SAFETY: Per this function's `# Safety`, `out_add_with_config` is a valid pointer, as
    // `deliver_added` requires.
    unsafe { deliver_added(added, out_add_with_config) }
}

/// `hasMetrics()`.
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

/// `hasExpired()`.
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
            // never freed; the names read are static; `by_id` and `by_name` are valid out pointers
            // and `name` outlives the call reading it.
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
                let mut by_id = ptr::null();
                assert!(kafka_common_metrics_Sensor_RecordingLevel_for_id(level.id(), &mut by_id).is_null());
                assert_eq!(by_id, singleton);
                let mut by_name = ptr::null();
                let name = CString::new(level.name()).unwrap();
                assert!(kafka_common_metrics_Sensor_RecordingLevel_for_name(name.as_ptr(), &mut by_name).is_null());
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
        // SAFETY: Test code: `debug` is a singleton from this module, never freed; `out` is a valid
        // out pointer, `unknown` outlives the call reading it, and each error returned is destroyed
        // once.
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

            let mut out = ptr::null();
            let error = kafka_common_metrics_Sensor_RecordingLevel_for_id(7, &mut out);
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
            let unknown = CString::new("VERBOSE").unwrap();
            let error = kafka_common_metrics_Sensor_RecordingLevel_for_name(unknown.as_ptr(), &mut out);
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
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
        // `kafka_common_metrics_MetricConfig_new` and is destroyed once; each error returned is
        // destroyed once, and `added` is a valid out pointer.
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
            let mut added = -1;
            assert!(
                kafka_common_metrics_Sensor_add_with_metric_name(
                    sensor,
                    name,
                    kafka_common_metrics_stats_Avg__as_MeasurableStat(avg) as *mut _,
                    &mut added
                )
                .is_null()
            );
            assert_eq!(added, 1);
            assert_eq!(kafka_common_metrics_Sensor_has_metrics(sensor), 1);

            // Re-adding a name this sensor already has is Java's
            // `metrics.containsKey(metricName)` branch: true, nothing changes.
            let config = kafka_common_metrics_MetricConfig_new();
            added = -1;
            assert!(
                kafka_common_metrics_Sensor_add_with_metric_name_config(
                    sensor,
                    name,
                    kafka_common_metrics_stats_Avg__as_MeasurableStat(avg) as *mut _,
                    config,
                    &mut added,
                )
                .is_null()
            );
            assert_eq!(added, 1);
            // A name another sensor registered is Java's IllegalArgumentException
            // from the registry.
            let other = box_sensor(metrics_ref(registry).sensor("other").unwrap());
            let error = kafka_common_metrics_Sensor_add_with_metric_name_config(
                other,
                name,
                kafka_common_metrics_stats_Avg__as_MeasurableStat(avg) as *mut _,
                config,
                &mut added,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
            kafka_common_metrics_MetricConfig_destroy(config);
            kafka_common_MetricName_destroy(name);

            kafka_common_metrics_Sensor_record(sensor);
            kafka_common_metrics_Sensor_record_with_value(sensor, 3.0);
            kafka_common_metrics_Sensor_record_with_value_time_ms(sensor, 5.0, 10);
            let avg_config = metrics_ref(registry).config().clone();
            let metric = metrics_ref(registry)
                .metric(&metrics_ref(registry).metric_name("avg", "g"))
                .unwrap();
            assert_eq!(metric.measurable().unwrap().measure(&avg_config, 10), 3.0);
            kafka_common_metrics_stats_Avg_destroy(avg);

            // No quota configured: checkQuotas passes.
            assert!(kafka_common_metrics_Sensor_check_quotas(sensor).is_null());
            assert!(kafka_common_metrics_Sensor_check_quotas_with_time_ms(sensor, 10).is_null());

            // A compound stat registers both of its metrics.
            let rate_name = box_metric_name(metrics_ref(registry).metric_name("rate", "g"));
            let total_name = box_metric_name(metrics_ref(registry).metric_name("total", "g"));
            let meter = kafka_common_metrics_stats_Meter_new(rate_name, total_name);
            kafka_common_MetricName_destroy(rate_name);
            kafka_common_MetricName_destroy(total_name);
            assert!(
                kafka_common_metrics_Sensor_add(
                    sensor,
                    kafka_common_metrics_stats_Meter__as_CompoundStat(meter) as *mut _,
                    &mut added
                )
                .is_null()
            );
            assert_eq!(added, 1);
            // Re-adding it to this sensor skips the names it already has and
            // returns true; on another sensor the registry rejects them.
            added = -1;
            assert!(
                kafka_common_metrics_Sensor_add_with_config(
                    sensor,
                    kafka_common_metrics_stats_Meter__as_CompoundStat(meter) as *mut _,
                    ptr::null(),
                    &mut added,
                )
                .is_null()
            );
            assert_eq!(added, 1);
            let error = kafka_common_metrics_Sensor_add_with_config(
                other,
                kafka_common_metrics_stats_Meter__as_CompoundStat(meter) as *mut _,
                ptr::null(),
                &mut added,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
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
}

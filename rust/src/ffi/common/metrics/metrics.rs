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

//! `kafka_common_metrics_Metrics_t`: `org.apache.kafka.common.metrics.Metrics`
//! (CLAUDE.md §4), the registry of sensors and metrics, plus the Rust-only
//! `kafka_common_metrics_SensorOptions_t` / `_SensorOptionsBuilder_t` pair
//! behind `Metrics_sensor_with_options` (CLAUDE.md §2's `Options` rule for
//! the `sensor` overloads).
//!
//! Ownership follows the §4 defaults: `*mut` returns are owned handles on
//! shared registry state (a sensor or metric handle outlives its removal
//! from the registry), `*const` parameters are read during the call, and
//! the interface handles `add_metric_with_measurable` / `add_reporter` take
//! are consumed when owned and shared when they are class views.
//!
//! The registry itself runs no background task: a reporter added with
//! `add_reporter` is called synchronously on the thread that mutates the
//! registry (`init` during `add_reporter`, `metric_change` / `metric_removal`
//! during `add_metric*` / `remove_*`, `close` during `close`).

use std::collections::BTreeMap;
use std::ffi::c_char;
use std::sync::Arc;

use crate::common::Error;
use crate::common::metrics::{MetricConfig, Metrics, RecordingLevel, Sensor, SensorOptions, SensorOptionsBuilder};
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::metric_name::{box_metric_name, kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metric_name_template::{kafka_common_MetricNameTemplate_t, metric_name_template_ref};
use crate::ffi::common::metrics::box_metrics_map;
use crate::ffi::common::metrics::kafka_metric::{box_kafka_metric, kafka_common_metrics_KafkaMetric_t};
use crate::ffi::common::metrics::measurable::{ArcMeasurable, kafka_common_metrics_Measurable_t, take_measurable};
use crate::ffi::common::metrics::metric_config::{
    box_metric_config, kafka_common_metrics_MetricConfig_t, metric_config_arc, metric_config_option,
};
use crate::ffi::common::metrics::metric_value_provider::{
    kafka_common_metrics_MetricValueProvider_t, take_metric_value_provider,
};
use crate::ffi::common::metrics::metrics_reporter::{kafka_common_metrics_MetricsReporter_t, take_metrics_reporter};
use crate::ffi::common::metrics::sensor::{
    box_sensor, kafka_common_metrics_Sensor_RecordingLevel_t, kafka_common_metrics_Sensor_t, list_sensors,
    recording_level_of,
};
use crate::ffi::common::{FailValue, deliver_value};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, kafka_List_t, kafka_Map_t, list_strings, map_strings};

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Metrics`] registry.
#[repr(C)]
pub struct kafka_common_metrics_Metrics_t {
    _private: [u8; 0],
}

/// The registry behind a handle.
///
/// # Safety
///
/// `metrics` must be a valid registry handle, alive for `'a`.
pub(crate) unsafe fn metrics_ref<'a>(metrics: *const kafka_common_metrics_Metrics_t) -> &'a Metrics {
    // SAFETY: Per this function's `# Safety`, `metrics` is a valid registry handle alive for `'a`,
    // i.e. the `Box::into_raw` of a `Metrics` in `boxed` (every registry handle is owned: no view
    // of this type is handed out). Only `kafka_common_metrics_Metrics_destroy` takes the `Metrics`
    // back, and the registry is otherwise used only through `&self`, so the shared reference is
    // sound for `'a`.
    unsafe { &*(metrics as *const Metrics) }
}

fn boxed(metrics: Metrics) -> *mut kafka_common_metrics_Metrics_t {
    Box::into_raw(Box::new(metrics)) as *mut kafka_common_metrics_Metrics_t
}

/// Returns a fallible value, or stores its error in `*out_err` and returns
/// the failure value (CLAUDE.md §4).
///
/// # Safety
///
/// `out_err` must be valid for one write and point at null.
unsafe fn deliver<T: FailValue>(result: Result<T, Error>, out_err: *mut *mut kafka_common_Error_t) -> T {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |value| value) }
}

/// `new Metrics()`: a registry with a default config and no reporters.
/// Owned, freed with [`kafka_common_metrics_Metrics_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Metrics_new() -> *mut kafka_common_metrics_Metrics_t {
    boxed(Metrics::new())
}

/// `new Metrics(MetricConfig defaultConfig)`: the config is shared.
///
/// # Safety
///
/// `default_config` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_with_default_config(
    default_config: *const kafka_common_metrics_MetricConfig_t,
) -> *mut kafka_common_metrics_Metrics_t {
    // SAFETY: Per this function's `# Safety`, `default_config` is a valid metric-config handle, as
    // `metric_config_arc` requires; it returns an owned `Arc`, which the registry shares, so
    // nothing borrows the handle after this call.
    boxed(Metrics::with_default_config(unsafe { metric_config_arc(default_config) }))
}

/// `metricName(String name, String group)`: name and group plus the
/// config's default tags. Owned, freed with `kafka_common_MetricName_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and the strings NUL-terminated.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
) -> *mut kafka_common_MetricName_t {
    box_metric_name(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as
        // `metrics_ref` requires, and `name` and `group` are NUL-terminated strings, as
        // `c_str_to_string` requires; the reference is used only within this call and the strings
        // are copied into owned `String`s.
        unsafe { metrics_ref(self_) }.metric_name(unsafe { c_str_to_string(name) }, unsafe { c_str_to_string(group) }),
    )
}

/// `metricName(String name, String group, String description)`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and the strings NUL-terminated.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_description(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    box_metric_name(unsafe { metrics_ref(self_) }.metric_name_with_description(
        // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(name) },
        // SAFETY: Per this function's `# Safety`, `group` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(group) },
        // SAFETY: Per this function's `# Safety`, `description` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(description) },
    ))
}

/// `metricName(String name, String group, Map<String, String> tags)`: `tags`
/// is a string-to-string map, copied, taking precedence over the default
/// tags.
///
/// # Safety
///
/// `self_` must be a valid registry handle, the strings NUL-terminated and
/// `tags` null or a valid map of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_tags(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    tags: *const kafka_Map_t,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    box_metric_name(unsafe { metrics_ref(self_) }.metric_name_with_tags(
        // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(name) },
        // SAFETY: Per this function's `# Safety`, `group` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(group) },
        // SAFETY: Per this function's `# Safety`, `tags` is null or a valid map of NUL-terminated
        // strings, as `map_strings` requires; the entries are copied into owned `String`s, so
        // nothing borrows the map after this call.
        unsafe { map_strings(tags) }.into_iter().collect::<BTreeMap<_, _>>(),
    ))
}

/// `metricName(String name, String group, String description, Map<String, String> tags)`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, the strings NUL-terminated and
/// `tags` null or a valid map of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_description_tags(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    tags: *const kafka_Map_t,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    box_metric_name(unsafe { metrics_ref(self_) }.metric_name_with_description_tags(
        // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(name) },
        // SAFETY: Per this function's `# Safety`, `group` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(group) },
        // SAFETY: Per this function's `# Safety`, `description` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(description) },
        // SAFETY: Per this function's `# Safety`, `tags` is null or a valid map of NUL-terminated
        // strings, as `map_strings` requires; the entries are copied into owned `String`s, so
        // nothing borrows the map after this call.
        unsafe { map_strings(tags) }.into_iter().collect::<BTreeMap<_, _>>(),
    ))
}

/// `metricName(String name, String group, String description, String... keyValue)`:
/// `key_value` holds `const char *` elements in key, value pairs; an odd
/// count is Java's `IllegalArgumentException`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, the strings NUL-terminated,
/// `key_value` null or a valid list of NUL-terminated strings and `out_err` a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_description_key_value(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    key_value: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `key_value` is null or a valid list of NUL-terminated
    // strings, as `list_strings` requires; the strings are copied into owned `String`s, so nothing
    // borrows the list after this call.
    let key_value = unsafe { list_strings(key_value) };
    let key_value: Vec<&str> = key_value.iter().map(String::as_str).collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .metric_name_with_description_key_value(
            // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            unsafe { c_str_to_string(name) },
            // SAFETY: Per this function's `# Safety`, `group` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            unsafe { c_str_to_string(group) },
            // SAFETY: Per this function's `# Safety`, `description` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            unsafe { c_str_to_string(description) },
            &key_value,
        )
        .map(box_metric_name);
    // SAFETY: Per this function's `# Safety`, `out_err` is
    // writable, as `deliver` requires; on success the owned handle written there passes to the C
    // caller.
    unsafe { deliver(result, out_err) }
}

/// `config()`: the registry's default config, shared. Owned handle, freed
/// with `kafka_common_metrics_MetricConfig_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_config(
    self_: *const kafka_common_metrics_Metrics_t,
) -> *mut kafka_common_metrics_MetricConfig_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call. The `Arc` is cloned, so the config
    // handle returned shares the registry's config.
    box_metric_config(Arc::clone(unsafe { metrics_ref(self_) }.config()))
}

/// `getSensor(String name)`: the sensor registered under `name`, `NULL` when
/// there is none. Owned handle, freed with `kafka_common_metrics_Sensor_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `name` NUL-terminated.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_get_sensor(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires, and `name` a NUL-terminated string, as `c_str_to_string` requires; the reference is
    // used only within this call and the name is copied into an owned `String`.
    match unsafe { metrics_ref(self_) }.get_sensor(&unsafe { c_str_to_string(name) }) {
        Some(sensor) => box_sensor(sensor),
        None => std::ptr::null_mut(),
    }
}

/// `sensor(String name)`: gets or creates the sensor. The handle is owned,
/// freed with `kafka_common_metrics_Sensor_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `name` NUL-terminated and `out_err`
/// a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        .sensor(&unsafe { c_str_to_string(name) })
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as `deliver` requires; on
    // success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor(String name, Sensor.RecordingLevel recordingLevel)`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `name` NUL-terminated,
/// `recording_level` a recording-level singleton and `out_err` a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_recording_level(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
        // `c_str_to_string` requires, and `recording_level` a recording-level singleton, as
        // `recording_level_of` requires; the name is copied into an owned `String` and the level
        // copied out.
        .sensor_with_recording_level(&unsafe { c_str_to_string(name) }, unsafe {
            recording_level_of(recording_level)
        })
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as
    // `deliver` requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor(String name, Sensor... parents)`: `parents` holds
/// `const kafka_common_metrics_Sensor_t *` elements, shared with the new
/// sensor.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `name` NUL-terminated, `parents`
/// null or a valid list of sensor handles and `out_err` a valid slot pointing
/// at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    parents: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
        // `c_str_to_string` requires, and `parents` null or a valid list of sensor handles, as
        // `list_sensors` requires; the name is copied into an owned `String` and the sensors cloned
        // into owned `Arc`s, so nothing borrows either after this call.
        .sensor_with_parents(&unsafe { c_str_to_string(name) }, &unsafe { list_sensors(parents) })
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as `deliver`
    // requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents)`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_parents`], with
/// `recording_level` a recording-level singleton. `out_err` must be a valid
/// slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_recording_level_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    parents: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_recording_level_parents(
            // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            &unsafe { c_str_to_string(name) },
            // SAFETY: Per this function's `# Safety`, `recording_level` is a recording-level
            // singleton, as `recording_level_of` requires; the value is copied out.
            unsafe { recording_level_of(recording_level) },
            // SAFETY: Per this function's `# Safety`, `parents` is null or a valid list of sensor
            // handles, as `list_sensors` requires; the sensors are cloned into owned `Arc`s, so
            // nothing borrows the list after this call.
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is
    // writable, as `deliver` requires; on success the owned handle written there passes to the C
    // caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor(String name, MetricConfig config, Sensor... parents)`: a `NULL`
/// config means the registry's own.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_parents`], with `config` null
/// or a valid metric-config handle. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_config_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    config: *const kafka_common_metrics_MetricConfig_t,
    parents: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_config_parents(
            // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            &unsafe { c_str_to_string(name) },
            // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config
            // handle, as `metric_config_option` requires; it returns an owned `Arc`, so nothing
            // borrows the handle after this call.
            unsafe { metric_config_option(config) },
            // SAFETY: Per this function's `# Safety`, `parents` is null or a valid list of sensor
            // handles, as `list_sensors` requires; the sensors are cloned into owned `Arc`s, so
            // nothing borrows the list after this call.
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as
    // `deliver` requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor(String name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents)`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_config_parents`], with
/// `recording_level` a recording-level singleton. `out_err` must be a valid
/// slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_config_recording_level_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    config: *const kafka_common_metrics_MetricConfig_t,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    parents: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_config_recording_level_parents(
            // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            &unsafe { c_str_to_string(name) },
            // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config
            // handle, as `metric_config_option` requires; it returns an owned `Arc`, so nothing
            // borrows the handle after this call.
            unsafe { metric_config_option(config) },
            // SAFETY: Per this function's `# Safety`, `recording_level` is a recording-level
            // singleton, as `recording_level_of` requires; the value is copied out.
            unsafe { recording_level_of(recording_level) },
            // SAFETY: Per this function's `# Safety`, `parents` is null or a valid list of sensor
            // handles, as `list_sensors` requires; the sensors are cloned into owned `Arc`s, so
            // nothing borrows the list after this call.
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is
    // writable, as `deliver` requires; on success the owned handle written there passes to the C
    // caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor... parents)`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_config_parents`]. `out_err`
/// must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_config_inactive_sensor_expiration_time_seconds_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    config: *const kafka_common_metrics_MetricConfig_t,
    inactive_sensor_expiration_time_seconds: i64,
    parents: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_config_inactive_sensor_expiration_time_seconds_parents(
            // SAFETY: Per this function's `# Safety`, `name` is a NUL-terminated string, as
            // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows
            // it after this call.
            &unsafe { c_str_to_string(name) },
            // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config
            // handle, as `metric_config_option` requires; it returns an owned `Arc`, so nothing
            // borrows the handle after this call.
            unsafe { metric_config_option(config) },
            inactive_sensor_expiration_time_seconds,
            // SAFETY: Per this function's `# Safety`, `parents` is null or a valid list of sensor
            // handles, as `list_sensors` requires; the sensors are cloned into owned `Arc`s, so
            // nothing borrows the list after this call.
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`,
    // `out_err` is writable, as
    // `deliver` requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `sensor_with_options(SensorOptions)`: the full-parameter form behind the
/// overloads above, taking the options built by
/// [`kafka_common_metrics_SensorOptionsBuilder_build`].
///
/// # Safety
///
/// `self_` must be a valid registry handle, `options` a valid options handle
/// and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_options(
    self_: *const kafka_common_metrics_Metrics_t,
    options: *const kafka_common_metrics_SensorOptions_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_Sensor_t {
    // SAFETY: Per this function's `# Safety`, `options` is a valid options handle, i.e. the
    // `Box::into_raw` of a `SensorOptionsInner` in
    // `kafka_common_metrics_SensorOptionsBuilder_build` (every options handle is owned); nothing
    // mutates a `SensorOptionsInner`, and the reference is used only within this call.
    let options = unsafe { &*(options as *const SensorOptionsInner) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_options(options.options())
        .map(box_sensor);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as `deliver`
    // requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `removeSensor(String name)`: removes the sensor, its metrics and its
/// child sensors; a handle on the sensor stays valid.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `name` NUL-terminated.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_remove_sensor(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires, and `name` a NUL-terminated string, as `c_str_to_string` requires; the reference is
    // used only within this call and the name is copied into an owned `String`.
    unsafe { metrics_ref(self_) }.remove_sensor(&unsafe { c_str_to_string(name) })
}

/// `addMetric(MetricName metricName, Measurable measurable)`: the name is
/// copied; `measurable` is consumed when owned and shared when it is a class
/// view. A metric already registered under the name is Java's
/// `IllegalArgumentException`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `metric_name` a valid metric-name
/// handle and `measurable` a valid measurable handle, not used again by the
/// caller when it was owned. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_with_measurable(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    measurable: *mut kafka_common_metrics_Measurable_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `measurable` is a valid measurable handle that the
    // caller does not use again when it was owned, as `take_measurable` requires; an owned one is
    // consumed here and a view's implementation is shared through its `Arc`.
    let measurable = Box::new(ArcMeasurable(unsafe { take_measurable(measurable) }));
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    match unsafe { metrics_ref(self_) }
        // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
        // `metric_name_ref` requires; the name is cloned, so nothing borrows the handle after this
        // call.
        .add_metric_with_measurable(unsafe { metric_name_ref(metric_name) }.clone(), measurable)
    {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider)`:
/// `metric_name` is copied, `metric_value_provider` is consumed (an owned
/// handle is freed, the view of a stat class is shared), whatever the
/// outcome.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `metric_name` a valid metric-name
/// handle and `metric_value_provider` a valid provider handle, not used again
/// by the caller when it was owned. `out_err` must be a valid slot pointing at
/// null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_with_metric_value_provider(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    metric_value_provider: *mut kafka_common_metrics_MetricValueProvider_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    match unsafe { metrics_ref(self_) }
        // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
        // `metric_name_ref` requires, and `metric_value_provider` a valid metric-value-provider
        // handle not used again when owned, as `take_metric_value_provider` requires; the name is
        // cloned, so nothing borrows either handle after this call.
        .add_metric_with_metric_value_provider(unsafe { metric_name_ref(metric_name) }.clone(), unsafe {
            take_metric_value_provider(metric_value_provider)
        }) {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider)`:
/// a `NULL` config means the registry's own.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_add_metric_with_metric_value_provider`],
/// with `config` null or a valid metric-config handle. `out_err` must be a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_with_config_metric_value_provider(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    provider: *mut kafka_common_metrics_MetricValueProvider_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    match unsafe { metrics_ref(self_) }.add_metric_with_config_metric_value_provider(
        // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
        // `metric_name_ref` requires; the name is cloned, so nothing borrows the handle after this
        // call.
        unsafe { metric_name_ref(metric_name) }.clone(),
        // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config handle,
        // as `metric_config_option` requires; it returns an owned `Arc`, so nothing borrows the
        // handle after this call.
        unsafe { metric_config_option(config) },
        // SAFETY: Per this function's `# Safety`, `provider` is a valid metric-value-provider
        // handle not used again when owned, as `take_metric_value_provider` requires.
        unsafe { take_metric_value_provider(provider) },
    ) {
        Ok(()) => {},
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `addMetricIfAbsent(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider)`:
/// registers the metric unless one exists under the name, and returns the
/// registered one either way. Owned handle, freed with
/// `kafka_common_metrics_KafkaMetric_destroy`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_add_metric_with_config_metric_value_provider`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_if_absent(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    provider: *mut kafka_common_metrics_MetricValueProvider_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call. The metric returned is an owned `Arc`.
    box_kafka_metric(unsafe { metrics_ref(self_) }.add_metric_if_absent(
        // SAFETY: Per this function's `# Safety`, `metric_name` is a valid metric-name handle, as
        // `metric_name_ref` requires; the name is cloned, so nothing borrows the handle after this
        // call.
        unsafe { metric_name_ref(metric_name) }.clone(),
        // SAFETY: Per this function's `# Safety`, `config` is null or a valid metric-config handle,
        // as `metric_config_option` requires; it returns an owned `Arc`, so nothing borrows the
        // handle after this call.
        unsafe { metric_config_option(config) },
        // SAFETY: Per this function's `# Safety`, `provider` is a valid metric-value-provider
        // handle not used again when owned, as `take_metric_value_provider` requires.
        unsafe { take_metric_value_provider(provider) },
    ))
}

/// `removeMetric(MetricName metricName)`: the removed metric, `NULL` when
/// none was registered. Owned handle, freed with
/// `kafka_common_metrics_KafkaMetric_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `metric_name` a valid
/// metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_remove_metric(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires, and `metric_name` a valid metric-name handle, as `metric_name_ref` requires; both
    // references are used only within this call.
    match unsafe { metrics_ref(self_) }.remove_metric(unsafe { metric_name_ref(metric_name) }) {
        Some(metric) => box_kafka_metric(metric),
        None => std::ptr::null_mut(),
    }
}

/// `addReporter(MetricsReporter reporter)`: consumes an owned reporter
/// handle (shares a class view) and calls its `init` with the current
/// metrics before returning.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `reporter` a valid reporter
/// handle, not used again by the caller when it was owned.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_reporter(
    self_: *const kafka_common_metrics_Metrics_t,
    reporter: *mut kafka_common_metrics_MetricsReporter_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires, and `reporter` a valid reporter handle that the caller does not use again when it
    // was owned, as `take_metrics_reporter` requires; the registry reference is used only within
    // this call, and an owned reporter is consumed here while a view's implementation is shared
    // through its `Arc`.
    unsafe { metrics_ref(self_) }.add_reporter(unsafe { take_metrics_reporter(reporter) })
}

/// `metrics()`: a snapshot of every registered metric as an owned map of
/// owned `kafka_common_MetricName_t *` keys to owned
/// `kafka_common_metrics_KafkaMetric_t *` values (see `box_metrics_map`);
/// freed with `kafka_Map_destroy`, which frees both.
///
/// # Safety
///
/// `self_` must be a valid registry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metrics(
    self_: *const kafka_common_metrics_Metrics_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call. The snapshot is collected into owned
    // values.
    box_metrics_map(unsafe { metrics_ref(self_) }.metrics())
}

/// `metric(MetricName metricName)`: the metric registered under the name,
/// `NULL` when none is. Owned handle, freed with
/// `kafka_common_metrics_KafkaMetric_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `metric_name` a valid
/// metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires, and `metric_name` a valid metric-name handle, as `metric_name_ref` requires; both
    // references are used only within this call.
    match unsafe { metrics_ref(self_) }.metric(unsafe { metric_name_ref(metric_name) }) {
        Some(metric) => box_kafka_metric(metric),
        None => std::ptr::null_mut(),
    }
}

/// `metricInstance(MetricNameTemplate template, String... keyValue)`:
/// `key_value` holds `const char *` elements in key, value pairs; an odd
/// count, or tags not matching the template's, is Java's
/// `IllegalArgumentException`. The name is owned, freed with
/// `kafka_common_MetricName_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `template` a valid template handle,
/// `key_value` null or a valid list of NUL-terminated strings and `out_err` a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_instance_with_key_value(
    self_: *const kafka_common_metrics_Metrics_t,
    template: *const kafka_common_MetricNameTemplate_t,
    key_value: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `key_value` is null or a valid list of NUL-terminated
    // strings, as `list_strings` requires; the strings are copied into owned `String`s, so nothing
    // borrows the list after this call.
    let key_value = unsafe { list_strings(key_value) };
    let key_value: Vec<&str> = key_value.iter().map(String::as_str).collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        // SAFETY: Per this function's `# Safety`, `template` is a valid metric-name-template
        // handle, as `metric_name_template_ref` requires; the reference is used only within this
        // call.
        .metric_instance_with_key_value(unsafe { metric_name_template_ref(template) }, &key_value)
        .map(box_metric_name);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as
    // `deliver` requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `metricInstance(MetricNameTemplate template, Map<String, String> tags)`:
/// `tags` is a string-to-string map, copied.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `template` a valid template handle,
/// `tags` null or a valid map of NUL-terminated strings and `out_err` a valid
/// slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_instance_with_tags(
    self_: *const kafka_common_metrics_Metrics_t,
    template: *const kafka_common_MetricNameTemplate_t,
    tags: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    let result = unsafe { metrics_ref(self_) }
        .metric_instance_with_tags(
            // SAFETY: Per this function's `# Safety`, `template` is a valid metric-name-template
            // handle, as `metric_name_template_ref` requires; the reference is used only within
            // this call.
            unsafe { metric_name_template_ref(template) },
            // SAFETY: Per this function's `# Safety`, `tags` is null or a valid map of
            // NUL-terminated strings, as `map_strings` requires; the entries are copied into owned
            // `String`s, so nothing borrows the map after this call.
            unsafe { map_strings(tags) }.into_iter().collect::<BTreeMap<_, _>>(),
        )
        .map(box_metric_name);
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as
    // `deliver` requires; on success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// `close()`: closes every reporter. The handle stays valid until
/// [`kafka_common_metrics_Metrics_destroy`].
///
/// # Safety
///
/// `self_` must be a valid registry handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_close(self_: *const kafka_common_metrics_Metrics_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid registry handle, as `metrics_ref`
    // requires; the reference is used only within this call.
    unsafe { metrics_ref(self_) }.close()
}

/// Frees an owned registry handle; null is a no-op. Sensor and metric
/// handles taken from it stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_destroy(self_: *mut kafka_common_metrics_Metrics_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // registry handle not yet destroyed, i.e. the `Box::into_raw` of a `Metrics` in `boxed`, so
        // `from_raw` reclaims that allocation, once. Sensor and metric handles hold their own
        // `Arc`s, so they stay valid.
        drop(unsafe { Box::from_raw(self_ as *mut Metrics) });
    }
}

// ---------------------------------------------------------------------------
// SensorOptions and its builder
// ---------------------------------------------------------------------------

/// Opaque handle to a [`SensorOptions`]: the parameters of the
/// `Metrics.sensor` overloads, built by
/// [`kafka_common_metrics_SensorOptionsBuilder_build`].
// the options of the Metrics.sensor overloads (CLAUDE.md §2)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_metrics_SensorOptions_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_metrics_SensorOptions_t`] points at: the Rust
/// options own nothing (they borrow the name and parents), so the handle
/// owns copies and lends them out as [`SensorOptions`] per call.
struct SensorOptionsInner {
    name: String,
    config: Option<Arc<MetricConfig>>,
    inactive_sensor_expiration_time_seconds: i64,
    recording_level: RecordingLevel,
    parents: Vec<Arc<Sensor>>,
}

impl SensorOptionsInner {
    fn options(&self) -> SensorOptions<'_> {
        SensorOptions {
            name: &self.name,
            config: self.config.clone(),
            inactive_sensor_expiration_time_seconds: self.inactive_sensor_expiration_time_seconds,
            recording_level: self.recording_level,
            parents: &self.parents,
        }
    }
}

/// Frees an owned options handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptions_destroy(self_: *mut kafka_common_metrics_SensorOptions_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // options handle not yet destroyed, i.e. the `Box::into_raw` of a `SensorOptionsInner` in
        // `kafka_common_metrics_SensorOptionsBuilder_build`, so `from_raw` reclaims that
        // allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut SensorOptionsInner) });
    }
}

/// Opaque handle to a [`SensorOptionsBuilder`]: `name` is mandatory, the
/// other parameters default as in Java's `Metrics.sensor` overloads (the
/// registry's config, no expiration, `INFO`, no parents).
// builds SensorOptions (CLAUDE.md §2)
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_common_metrics_SensorOptionsBuilder_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_metrics_SensorOptionsBuilder_t`] points at. The
/// Rust builder borrows its name and parents and is consumed by `build`;
/// the handle owns copies so the C builder can be mutated in place and
/// reused after `_build`.
struct SensorOptionsBuilderInner {
    name: Option<String>,
    config: Option<Arc<MetricConfig>>,
    inactive_sensor_expiration_time_seconds: i64,
    recording_level: RecordingLevel,
    parents: Vec<Arc<Sensor>>,
}

/// What a builder handle points at, for a setter or `build`.
///
/// # Safety
///
/// `self_` must be a valid builder handle, alive for `'a` and used by no
/// other call during it.
unsafe fn builder_mut<'a>(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
) -> &'a mut SensorOptionsBuilderInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle, i.e. the
    // `Box::into_raw` of a `SensorOptionsBuilderInner` in
    // `kafka_common_metrics_SensorOptionsBuilder_new` (every builder handle is owned), alive for
    // `'a` and used by no other call during it, so the exclusive reference aliases nothing for
    // `'a`.
    unsafe { &mut *(self_ as *mut SensorOptionsBuilderInner) }
}

/// `SensorOptionsBuilder::new()`: every parameter unset or at its default.
/// Owned, freed with [`kafka_common_metrics_SensorOptionsBuilder_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_SensorOptionsBuilder_new() -> *mut kafka_common_metrics_SensorOptionsBuilder_t {
    Box::into_raw(Box::new(SensorOptionsBuilderInner {
        name: None,
        config: None,
        inactive_sensor_expiration_time_seconds: i64::MAX,
        recording_level: RecordingLevel::Info,
        parents: Vec::new(),
    })) as *mut kafka_common_metrics_SensorOptionsBuilder_t
}

/// `set_name(name)`: the mandatory sensor name, copied.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time, and `name` NUL-terminated.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_name(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    name: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `builder_mut` requires, and `name` a NUL-terminated string, as
    // `c_str_to_string` requires; the exclusive reference is used only within this call. The name
    // is copied into an owned `String`.
    unsafe { builder_mut(self_) }.name = Some(unsafe { c_str_to_string(name) });
}

/// `set_config(config)`: the sensor's config, shared; `NULL` restores the
/// default of using the registry's.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time, and `config` null or a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_config(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    config: *const kafka_common_metrics_MetricConfig_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `builder_mut` requires, and `config` null or a valid metric-config
    // handle, as `metric_config_option` requires; the exclusive reference is used only within this
    // call. The config is held as an owned `Arc`.
    unsafe { builder_mut(self_) }.config = unsafe { metric_config_option(config) };
}

/// `set_inactive_sensor_expiration_time_seconds(seconds)`: how long the
/// sensor may go unrecorded before the registry expires it.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_inactive_sensor_expiration_time_seconds(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    inactive_sensor_expiration_time_seconds: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `builder_mut` requires; the exclusive reference is used only within
    // this call.
    unsafe { builder_mut(self_) }.inactive_sensor_expiration_time_seconds = inactive_sensor_expiration_time_seconds;
}

/// `set_recording_level(recording_level)`.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time, and `recording_level` a recording-level singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_recording_level(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `builder_mut` requires, and `recording_level` a recording-level
    // singleton, as `recording_level_of` requires; the exclusive reference is used only within this
    // call. The level is copied out.
    unsafe { builder_mut(self_) }.recording_level = unsafe { recording_level_of(recording_level) };
}

/// `set_parents(parents)`: `parents` holds
/// `const kafka_common_metrics_Sensor_t *` elements, shared; the list is
/// read during the call.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time, and `parents` null or a valid list of sensor handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_parents(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    parents: *const kafka_List_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `builder_mut` requires, and `parents` null or a valid list of
    // sensor handles, as `list_sensors` requires; the exclusive reference is used only within this
    // call. The sensors are cloned into owned `Arc`s.
    unsafe { builder_mut(self_) }.parents = unsafe { list_sensors(parents) };
}

/// `build()`: validates the mandatory parameters through the Rust builder
/// (an unset `name` is Java's `IllegalArgumentException`) and delivers an
/// owned options handle, freed with
/// [`kafka_common_metrics_SensorOptions_destroy`]. The builder stays usable.
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_build(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_metrics_SensorOptions_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `builder_mut` requires; the exclusive reference is used only within
    // this call: the options built copy what they take from it.
    let inner = unsafe { builder_mut(self_) };
    let mut builder = SensorOptionsBuilder::new()
        .set_config(inner.config.clone())
        .set_inactive_sensor_expiration_time_seconds(inner.inactive_sensor_expiration_time_seconds)
        .set_recording_level(inner.recording_level)
        .set_parents(&inner.parents);
    if let Some(name) = &inner.name {
        builder = builder.set_name(name);
    }
    let result = builder.build().map(|options| {
        Box::into_raw(Box::new(SensorOptionsInner {
            name: options.name.to_string(),
            config: options.config,
            inactive_sensor_expiration_time_seconds: options.inactive_sensor_expiration_time_seconds,
            recording_level: options.recording_level,
            parents: options.parents.to_vec(),
        })) as *mut kafka_common_metrics_SensorOptions_t
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is writable, as `deliver` requires; on
    // success the owned handle written there passes to the C caller.
    unsafe { deliver(result, out_err) }
}

/// Frees an owned builder handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_destroy(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // builder handle not yet destroyed, i.e. the `Box::into_raw` of a
        // `SensorOptionsBuilderInner` in `kafka_common_metrics_SensorOptionsBuilder_new`, so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut SensorOptionsBuilderInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString, c_void};
    use std::ptr;

    use indexmap::IndexSet;

    use super::*;
    use crate::common::MetricNameTemplate;
    use crate::ffi::common::metric_name::{kafka_common_MetricName_destroy, kafka_common_MetricName_name};
    use crate::ffi::common::metric_name_template::{
        kafka_common_MetricNameTemplate_destroy, kafka_common_MetricNameTemplate_new,
    };
    use crate::ffi::common::metrics::kafka_metric::{
        kafka_common_metrics_KafkaMetric_destroy, kafka_common_metrics_KafkaMetric_is_measurable, kafka_metric_ref,
    };
    use crate::ffi::common::metrics::metric_config::{
        kafka_common_metrics_MetricConfig_destroy, kafka_common_metrics_MetricConfig_new,
        kafka_common_metrics_MetricConfig_samples, kafka_common_metrics_MetricConfig_set_samples,
    };
    use crate::ffi::common::metrics::sensor::{
        kafka_common_metrics_Sensor_RecordingLevel_debug, kafka_common_metrics_Sensor_destroy,
        kafka_common_metrics_Sensor_name, kafka_common_metrics_Sensor_should_record, sensor_ref,
    };
    use crate::ffi::common::metrics::stats::avg::{
        kafka_common_metrics_stats_Avg__as_Measurable, kafka_common_metrics_stats_Avg__as_MetricValueProvider,
        kafka_common_metrics_stats_Avg_destroy, kafka_common_metrics_stats_Avg_new,
    };
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_new, kafka_Map_destroy, kafka_Map_get, kafka_Map_new,
        kafka_Map_put, kafka_Map_size,
    };

    fn c(s: &str) -> CString {
        CString::new(s).unwrap()
    }

    /// Asserts a `sensor*` overload returned the existing INFO sensor and
    /// frees the handle; `plain` is the overload's result and `error` what it
    /// left in its `out_err`.
    unsafe fn existing_info_sensor(plain: *mut kafka_common_metrics_Sensor_t, error: *mut kafka_common_Error_t) {
        assert!(error.is_null());
        assert_eq!(
            // SAFETY: Test code: callers pass as `plain` the sensor handle the overload just
            // returned, owned by the caller and destroyed only below.
            unsafe { kafka_common_metrics_Sensor_should_record(plain) },
            1,
            "the existing INFO sensor is returned"
        );
        // SAFETY: Test code: `plain` is that owned handle, destroyed once, here; callers do not use
        // it again.
        unsafe { kafka_common_metrics_Sensor_destroy(plain) };
    }

    #[test]
    fn names_sensors_and_metrics_round_trip_through_the_registry() {
        let registry = kafka_common_metrics_Metrics_new();
        let (name, group, description) = (c("n"), c("g"), c("d"));
        let (tag_key, tag_value) = (c("k"), c("v"));
        // SAFETY: Test code: `registry` comes from `kafka_common_metrics_Metrics_new` and is
        // destroyed once, last; the `CString`s outlive every call reading them, and the lists and
        // maps from `kafka_List_new` and `kafka_Map_new` borrow their elements and are destroyed
        // before them; the `parents` list borrows `sensor` and is destroyed before it; every owned
        // handle returned (metric names, sensors, metrics, the snapshot map, errors) is destroyed
        // once, after its last use; the measurable passed is the view `__as_Measurable` caches on
        // `avg`, which outlives the calls and is destroyed after them; `err` is a valid slot
        // pointing at null before each call, reset after each error is destroyed.
        unsafe {
            // Metric names: every overload adds the config's (empty) default tags.
            let tags = kafka_Map_new();
            kafka_Map_put(tags, tag_key.as_ptr() as *mut c_void, tag_value.as_ptr() as *mut c_void);
            let tagged = kafka_common_metrics_Metrics_metric_name_with_description_tags(
                registry,
                name.as_ptr(),
                group.as_ptr(),
                description.as_ptr(),
                tags,
            );
            let expected = metrics_ref(registry).metric_name_with_description_tags(
                "n",
                "g",
                "d",
                BTreeMap::from([("k".to_string(), "v".to_string())]),
            );
            assert_eq!(*metric_name_ref(tagged), expected);
            let tagged_only =
                kafka_common_metrics_Metrics_metric_name_with_tags(registry, name.as_ptr(), group.as_ptr(), tags);
            assert_eq!(metric_name_ref(tagged_only).tags(), expected.tags());
            assert_eq!(metric_name_ref(tagged_only).description(), "");
            kafka_common_MetricName_destroy(tagged_only);
            kafka_Map_destroy(tags);
            let described = kafka_common_metrics_Metrics_metric_name_with_description(
                registry,
                name.as_ptr(),
                group.as_ptr(),
                description.as_ptr(),
            );
            assert_eq!(metric_name_ref(described).description(), "d");
            kafka_common_MetricName_destroy(described);

            // Key/value pairs: an odd list is Java's IllegalArgumentException.
            let pairs = kafka_List_new();
            kafka_List_add(pairs, tag_key.as_ptr() as *mut c_void);
            let mut err = ptr::null_mut();
            let from_pairs = kafka_common_metrics_Metrics_metric_name_with_description_key_value(
                registry,
                name.as_ptr(),
                group.as_ptr(),
                description.as_ptr(),
                pairs,
                &mut err,
            );
            assert!(from_pairs.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();
            kafka_List_add(pairs, tag_value.as_ptr() as *mut c_void);
            let from_pairs = kafka_common_metrics_Metrics_metric_name_with_description_key_value(
                registry,
                name.as_ptr(),
                group.as_ptr(),
                description.as_ptr(),
                pairs,
                &mut err,
            );
            assert!(err.is_null());
            assert_eq!(*metric_name_ref(from_pairs), expected);
            kafka_common_MetricName_destroy(from_pairs);
            kafka_List_destroy(pairs);

            // Sensors: get-or-create, lookup, parents, removal.
            let sensor_name = c("s");
            assert!(kafka_common_metrics_Metrics_get_sensor(registry, sensor_name.as_ptr()).is_null());
            let sensor = kafka_common_metrics_Metrics_sensor_with_recording_level(
                registry,
                sensor_name.as_ptr(),
                kafka_common_metrics_Sensor_RecordingLevel_debug(),
                &mut err,
            );
            assert!(err.is_null());
            assert_eq!(CStr::from_ptr(kafka_common_metrics_Sensor_name(sensor)).to_str().unwrap(), "s");
            assert_eq!(
                kafka_common_metrics_Sensor_should_record(sensor),
                0,
                "a DEBUG sensor under an INFO config"
            );
            let again = kafka_common_metrics_Metrics_get_sensor(registry, sensor_name.as_ptr());
            assert!(Arc::ptr_eq(sensor_ref(sensor), sensor_ref(again)));
            kafka_common_metrics_Sensor_destroy(again);
            let parents = kafka_List_new();
            kafka_List_add(parents, sensor as *mut c_void);
            let child_name = c("child");
            let child =
                kafka_common_metrics_Metrics_sensor_with_parents(registry, child_name.as_ptr(), parents, &mut err);
            assert!(err.is_null());
            kafka_List_destroy(parents);
            // Every other overload finds the INFO sensor the first call created.
            let plain_name = c("plain");
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor(registry, plain_name.as_ptr(), &mut err),
                err,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_recording_level_parents(
                    registry,
                    plain_name.as_ptr(),
                    kafka_common_metrics_Sensor_RecordingLevel_debug(),
                    ptr::null(),
                    &mut err,
                ),
                err,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_config_parents(
                    registry,
                    plain_name.as_ptr(),
                    ptr::null(),
                    ptr::null(),
                    &mut err,
                ),
                err,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_config_recording_level_parents(
                    registry,
                    plain_name.as_ptr(),
                    ptr::null(),
                    kafka_common_metrics_Sensor_RecordingLevel_debug(),
                    ptr::null(),
                    &mut err,
                ),
                err,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_config_inactive_sensor_expiration_time_seconds_parents(
                    registry,
                    plain_name.as_ptr(),
                    ptr::null(),
                    60,
                    ptr::null(),
                    &mut err,
                ),
                err,
            );

            // Metrics: add through a stat's Measurable view, look up, snapshot, remove.
            let avg = kafka_common_metrics_stats_Avg_new();
            kafka_common_metrics_Metrics_add_metric_with_measurable(
                registry,
                tagged,
                kafka_common_metrics_stats_Avg__as_Measurable(avg) as *mut _,
                &mut err,
            );
            assert!(err.is_null());
            kafka_common_metrics_Metrics_add_metric_with_measurable(
                registry,
                tagged,
                kafka_common_metrics_stats_Avg__as_Measurable(avg) as *mut _,
                &mut err,
            );
            let duplicate = err;
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(duplicate), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(duplicate)).to_str().unwrap(),
                format!("A metric named '{expected}' already exists, can't register another one.")
            );
            kafka_common_Error_destroy(duplicate);
            let metric = kafka_common_metrics_Metrics_metric(registry, tagged);
            assert_eq!(kafka_common_metrics_KafkaMetric_is_measurable(metric), 1);
            let snapshot = kafka_common_metrics_Metrics_metrics(registry);
            assert_eq!(kafka_Map_size(snapshot), 2, "the registry's built-in `count` metric and ours");
            let found = kafka_Map_get(snapshot, tagged as *mut c_void) as *const kafka_common_metrics_KafkaMetric_t;
            assert!(Arc::ptr_eq(kafka_metric_ref(found), kafka_metric_ref(metric)));
            kafka_Map_destroy(snapshot);
            let removed = kafka_common_metrics_Metrics_remove_metric(registry, tagged);
            assert!(Arc::ptr_eq(kafka_metric_ref(removed), kafka_metric_ref(metric)));
            assert!(kafka_common_metrics_Metrics_remove_metric(registry, tagged).is_null());
            assert!(kafka_common_metrics_Metrics_metric(registry, tagged).is_null());
            kafka_common_metrics_KafkaMetric_destroy(removed);
            kafka_common_metrics_KafkaMetric_destroy(metric);
            kafka_common_metrics_stats_Avg_destroy(avg);

            kafka_common_metrics_Metrics_remove_sensor(registry, sensor_name.as_ptr());
            assert!(kafka_common_metrics_Metrics_get_sensor(registry, sensor_name.as_ptr()).is_null());
            assert!(
                kafka_common_metrics_Metrics_get_sensor(registry, child_name.as_ptr()).is_null(),
                "children go too"
            );
            kafka_common_metrics_Sensor_destroy(child);
            kafka_common_metrics_Sensor_destroy(sensor);
            kafka_common_MetricName_destroy(tagged);
            kafka_common_metrics_Metrics_close(registry);
            kafka_common_metrics_Metrics_destroy(registry);
            kafka_common_metrics_Metrics_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn default_config_providers_and_templates_follow_the_rust_registry() {
        let (name, group, description) = (c("n"), c("g"), c("d"));
        // SAFETY: Test code: `config`, `registry`, `metric_name`, `provider`, `template` and the
        // handles returned each come from their constructor or getter and are destroyed once, after
        // their last use; the `Avg` behind the provider's measurable view is never destroyed, a
        // leak that keeps the view valid; the `CString`s outlive the lists and maps from
        // `kafka_List_new` and `kafka_Map_new` that borrow them, which are destroyed first; each
        // error is destroyed once and `err` is a valid slot pointing at null before each call.
        unsafe {
            let config = kafka_common_metrics_MetricConfig_new();
            kafka_common_metrics_MetricConfig_set_samples(config, 7);
            let registry = kafka_common_metrics_Metrics_with_default_config(config);
            let shared = kafka_common_metrics_Metrics_config(registry);
            assert_eq!(kafka_common_metrics_MetricConfig_samples(shared), 7);
            kafka_common_metrics_MetricConfig_destroy(shared);

            let metric_name = kafka_common_metrics_Metrics_metric_name(registry, name.as_ptr(), group.as_ptr());
            let avg = kafka_common_metrics_stats_Avg_new();
            // A stat class's view is shared, not consumed, by each `*mut` parameter.
            let provider = kafka_common_metrics_stats_Avg__as_MetricValueProvider(avg) as *mut _;
            let mut err = ptr::null_mut();
            kafka_common_metrics_Metrics_add_metric_with_metric_value_provider(
                registry,
                metric_name,
                provider,
                &mut err,
            );
            assert!(err.is_null());
            kafka_common_metrics_Metrics_add_metric_with_config_metric_value_provider(
                registry,
                metric_name,
                config,
                provider,
                &mut err,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();
            let existing =
                kafka_common_metrics_Metrics_add_metric_if_absent(registry, metric_name, ptr::null(), provider);
            let registered = kafka_common_metrics_Metrics_metric(registry, metric_name);
            assert!(Arc::ptr_eq(kafka_metric_ref(existing), kafka_metric_ref(registered)));
            kafka_common_metrics_KafkaMetric_destroy(existing);
            kafka_common_metrics_KafkaMetric_destroy(registered);
            kafka_common_metrics_stats_Avg_destroy(avg);
            kafka_common_MetricName_destroy(metric_name);

            // Templates: tags must match the template's tag names.
            let template_tags = kafka_List_new();
            let tag_key = c("k");
            kafka_List_add(template_tags, tag_key.as_ptr() as *mut c_void);
            let template =
                kafka_common_MetricNameTemplate_new(name.as_ptr(), group.as_ptr(), description.as_ptr(), template_tags);
            kafka_List_destroy(template_tags);
            let rust_template = MetricNameTemplate::new("n", "g", "d", IndexSet::from(["k".to_string()]));
            let pairs = kafka_List_new();
            kafka_List_add(pairs, tag_key.as_ptr() as *mut c_void);
            let tag_value = c("v");
            kafka_List_add(pairs, tag_value.as_ptr() as *mut c_void);
            let instance =
                kafka_common_metrics_Metrics_metric_instance_with_key_value(registry, template, pairs, &mut err);
            assert!(err.is_null());
            assert_eq!(
                *metric_name_ref(instance),
                metrics_ref(registry)
                    .metric_instance_with_key_value(&rust_template, &["k", "v"])
                    .unwrap()
            );
            assert_eq!(CStr::from_ptr(kafka_common_MetricName_name(instance)).to_str().unwrap(), "n");
            kafka_common_MetricName_destroy(instance);
            kafka_List_destroy(pairs);
            let wrong_tags = kafka_Map_new();
            let other_key = c("other");
            kafka_Map_put(wrong_tags, other_key.as_ptr() as *mut c_void, tag_value.as_ptr() as *mut c_void);
            let instance =
                kafka_common_metrics_Metrics_metric_instance_with_tags(registry, template, wrong_tags, &mut err);
            assert!(instance.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            kafka_common_Error_destroy(err);
            kafka_Map_destroy(wrong_tags);
            kafka_common_MetricNameTemplate_destroy(template);
            kafka_common_metrics_MetricConfig_destroy(config);
            kafka_common_metrics_Metrics_destroy(registry);
        }
    }

    #[test]
    fn the_options_builder_validates_the_name_and_feeds_sensor_with_options() {
        let registry = kafka_common_metrics_Metrics_new();
        // SAFETY: Test code: `registry`, `builder`, the options, the sensors and `config` each come
        // from their constructor and are destroyed once, after their last use; `parents` from
        // `kafka_List_new` borrows `parent` and is destroyed before it, after `set_parents` cloned
        // the sensors; `name` and `parent_name` outlive the calls reading them; the casts read the
        // `SensorOptionsInner` that `build` boxed while its handle is alive; `err` is a valid slot
        // pointing at null before each call and each error is destroyed once.
        unsafe {
            let builder = kafka_common_metrics_SensorOptionsBuilder_new();
            let mut err = ptr::null_mut();
            let options = kafka_common_metrics_SensorOptionsBuilder_build(builder, &mut err);
            assert!(options.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(err), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "SensorOptionsBuilder::build: mandatory parameter `name` was not set"
            );
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();

            let parent_name = c("parent");
            let parent = kafka_common_metrics_Metrics_sensor(registry, parent_name.as_ptr(), &mut err);
            assert!(err.is_null());
            let parents = kafka_List_new();
            kafka_List_add(parents, parent as *mut c_void);
            let config = kafka_common_metrics_MetricConfig_new();
            let name = c("child");
            kafka_common_metrics_SensorOptionsBuilder_set_name(builder, name.as_ptr());
            kafka_common_metrics_SensorOptionsBuilder_set_config(builder, config);
            kafka_common_metrics_SensorOptionsBuilder_set_inactive_sensor_expiration_time_seconds(builder, 30);
            kafka_common_metrics_SensorOptionsBuilder_set_recording_level(
                builder,
                kafka_common_metrics_Sensor_RecordingLevel_debug(),
            );
            kafka_common_metrics_SensorOptionsBuilder_set_parents(builder, parents);
            kafka_List_destroy(parents);
            let options = kafka_common_metrics_SensorOptionsBuilder_build(builder, &mut err);
            assert!(err.is_null());
            let inner = &*(options as *const SensorOptionsInner);
            assert_eq!(inner.name, "child");
            assert!(inner.config.is_some());
            assert_eq!(inner.inactive_sensor_expiration_time_seconds, 30);
            assert_eq!(inner.recording_level, RecordingLevel::Debug);
            assert_eq!(inner.parents.len(), 1);

            let child = kafka_common_metrics_Metrics_sensor_with_options(registry, options, &mut err);
            assert!(err.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_metrics_Sensor_name(child)).to_str().unwrap(),
                "child"
            );
            assert_eq!(
                kafka_common_metrics_Sensor_should_record(child),
                0,
                "DEBUG under the INFO config"
            );
            // The builder is reusable: a second build yields equal options.
            let rebuilt = kafka_common_metrics_SensorOptionsBuilder_build(builder, &mut err);
            assert!(err.is_null());
            assert_eq!((*(rebuilt as *const SensorOptionsInner)).name, "child");
            kafka_common_metrics_SensorOptions_destroy(rebuilt);
            kafka_common_metrics_SensorOptions_destroy(options);
            kafka_common_metrics_SensorOptions_destroy(ptr::null_mut());
            kafka_common_metrics_SensorOptionsBuilder_destroy(builder);
            kafka_common_metrics_SensorOptionsBuilder_destroy(ptr::null_mut());
            kafka_common_metrics_Sensor_destroy(child);
            kafka_common_metrics_Sensor_destroy(parent);
            kafka_common_metrics_MetricConfig_destroy(config);
            kafka_common_metrics_Metrics_destroy(registry);
        }
    }
}

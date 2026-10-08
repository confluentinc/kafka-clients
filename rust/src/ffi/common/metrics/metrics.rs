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
use std::ffi::{c_char, c_void};
use std::sync::Arc;

use crate::common::Error;
use crate::common::metrics::{MetricConfig, Metrics, RecordingLevel, Sensor, SensorOptions, SensorOptionsBuilder};
use crate::ffi::common::metric_name::{MetricNameInner, box_metric_name, kafka_common_MetricName_t, metric_name_ref};
use crate::ffi::common::metric_name_template::{kafka_common_MetricNameTemplate_t, metric_name_template_ref};
use crate::ffi::common::metrics::kafka_metric::{
    box_kafka_metric, destroy_kafka_metric_element, kafka_common_metrics_KafkaMetric_t,
};
use crate::ffi::common::metrics::measurable::{ArcMeasurable, kafka_common_metrics_Measurable_t, take_measurable};
use crate::ffi::common::metrics::metric_config::{
    box_metric_config, kafka_common_metrics_MetricConfig_t, metric_config_arc, metric_config_option,
};
use crate::ffi::common::metrics::metric_value_provider::{
    kafka_common_metrics_MetricValueProvider_t, metric_value_provider_of,
};
use crate::ffi::common::metrics::metrics_reporter::{kafka_common_metrics_MetricsReporter_t, take_metrics_reporter};
use crate::ffi::common::metrics::sensor::{
    box_sensor, kafka_common_metrics_Sensor_RecordingLevel_t, kafka_common_metrics_Sensor_t, list_sensors,
    recording_level_of,
};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{box_map, c_str_to_string, destroy_boxed, kafka_List_t, kafka_Map_t, list_strings, map_strings};

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
/// `metrics` must be a valid registry handle.
pub(crate) unsafe fn metrics_ref<'a>(metrics: *const kafka_common_metrics_Metrics_t) -> &'a Metrics {
    unsafe { &*(metrics as *const Metrics) }
}

fn boxed(metrics: Metrics) -> *mut kafka_common_metrics_Metrics_t {
    Box::into_raw(Box::new(metrics)) as *mut kafka_common_metrics_Metrics_t
}

/// Delivers a fallible value through its `out_` slot (CLAUDE.md §4): the
/// error is returned, `NULL` meaning success.
unsafe fn deliver<T>(result: Result<T, Error>, out: *mut T) -> *mut kafka_common_Error_t {
    match result {
        Ok(value) => {
            unsafe { out.write(value) };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `new Metrics()`: a registry with a default config and no reporters.
/// Owned, freed with [`kafka_common_metrics_Metrics_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_Metrics_new() -> *mut kafka_common_metrics_Metrics_t {
    boxed(Metrics::new())
}

/// `new Metrics(MetricConfig defaultConfig)`: the config is shared.
///
/// # Safety
///
/// `default_config` must be a valid metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_with_default_config(
    default_config: *const kafka_common_metrics_MetricConfig_t,
) -> *mut kafka_common_metrics_Metrics_t {
    boxed(Metrics::with_default_config(unsafe { metric_config_arc(default_config) }))
}

/// `metricName(String name, String group)`: name and group plus the
/// config's default tags. Owned, freed with `kafka_common_MetricName_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and the strings NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
) -> *mut kafka_common_MetricName_t {
    box_metric_name(
        unsafe { metrics_ref(self_) }.metric_name(unsafe { c_str_to_string(name) }, unsafe { c_str_to_string(group) }),
    )
}

/// `metricName(String name, String group, String description)`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and the strings NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_description(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
) -> *mut kafka_common_MetricName_t {
    box_metric_name(unsafe { metrics_ref(self_) }.metric_name_with_description(
        unsafe { c_str_to_string(name) },
        unsafe { c_str_to_string(group) },
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_tags(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    tags: *const kafka_Map_t,
) -> *mut kafka_common_MetricName_t {
    box_metric_name(unsafe { metrics_ref(self_) }.metric_name_with_tags(
        unsafe { c_str_to_string(name) },
        unsafe { c_str_to_string(group) },
        unsafe { map_strings(tags) }.into_iter().collect::<BTreeMap<_, _>>(),
    ))
}

/// `metricName(String name, String group, String description, Map<String, String> tags)`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, the strings NUL-terminated and
/// `tags` null or a valid map of NUL-terminated strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_description_tags(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    tags: *const kafka_Map_t,
) -> *mut kafka_common_MetricName_t {
    box_metric_name(unsafe { metrics_ref(self_) }.metric_name_with_description_tags(
        unsafe { c_str_to_string(name) },
        unsafe { c_str_to_string(group) },
        unsafe { c_str_to_string(description) },
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
/// `key_value` null or a valid list of NUL-terminated strings and
/// `out_metric_name_with_description_key_value` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_name_with_description_key_value(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    key_value: *const kafka_List_t,
    out_metric_name_with_description_key_value: *mut *mut kafka_common_MetricName_t,
) -> *mut kafka_common_Error_t {
    let key_value = unsafe { list_strings(key_value) };
    let key_value: Vec<&str> = key_value.iter().map(String::as_str).collect();
    let result = unsafe { metrics_ref(self_) }
        .metric_name_with_description_key_value(
            unsafe { c_str_to_string(name) },
            unsafe { c_str_to_string(group) },
            unsafe { c_str_to_string(description) },
            &key_value,
        )
        .map(box_metric_name);
    unsafe { deliver(result, out_metric_name_with_description_key_value) }
}

/// `config()`: the registry's default config, shared. Owned handle, freed
/// with `kafka_common_metrics_MetricConfig_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_config(
    self_: *const kafka_common_metrics_Metrics_t,
) -> *mut kafka_common_metrics_MetricConfig_t {
    box_metric_config(Arc::clone(unsafe { metrics_ref(self_) }.config()))
}

/// `getSensor(String name)`: the sensor registered under `name`, `NULL` when
/// there is none. Owned handle, freed with `kafka_common_metrics_Sensor_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `name` NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_get_sensor(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
) -> *mut kafka_common_metrics_Sensor_t {
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
/// `self_` must be a valid registry handle, `name` NUL-terminated and
/// `out_sensor` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    out_sensor: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor(&unsafe { c_str_to_string(name) })
        .map(box_sensor);
    unsafe { deliver(result, out_sensor) }
}

/// `sensor(String name, Sensor.RecordingLevel recordingLevel)`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `name` NUL-terminated,
/// `recording_level` a recording-level singleton and
/// `out_sensor_with_recording_level` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_recording_level(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    out_sensor_with_recording_level: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_recording_level(&unsafe { c_str_to_string(name) }, unsafe {
            recording_level_of(recording_level)
        })
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_recording_level) }
}

/// `sensor(String name, Sensor... parents)`: `parents` holds
/// `const kafka_common_metrics_Sensor_t *` elements, shared with the new
/// sensor.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `name` NUL-terminated, `parents`
/// null or a valid list of sensor handles and `out_sensor_with_parents`
/// writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    parents: *const kafka_List_t,
    out_sensor_with_parents: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_parents(&unsafe { c_str_to_string(name) }, &unsafe { list_sensors(parents) })
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_parents) }
}

/// `sensor(String name, Sensor.RecordingLevel recordingLevel, Sensor... parents)`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_parents`], with
/// `recording_level` a recording-level singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_recording_level_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    parents: *const kafka_List_t,
    out_sensor_with_recording_level_parents: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_recording_level_parents(
            &unsafe { c_str_to_string(name) },
            unsafe { recording_level_of(recording_level) },
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_recording_level_parents) }
}

/// `sensor(String name, MetricConfig config, Sensor... parents)`: a `NULL`
/// config means the registry's own.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_parents`], with `config`
/// null or a valid metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_config_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    config: *const kafka_common_metrics_MetricConfig_t,
    parents: *const kafka_List_t,
    out_sensor_with_config_parents: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_config_parents(
            &unsafe { c_str_to_string(name) },
            unsafe { metric_config_option(config) },
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_config_parents) }
}

/// `sensor(String name, MetricConfig config, Sensor.RecordingLevel recordingLevel, Sensor... parents)`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_config_parents`], with
/// `recording_level` a recording-level singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_config_recording_level_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    config: *const kafka_common_metrics_MetricConfig_t,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
    parents: *const kafka_List_t,
    out_sensor_with_config_recording_level_parents: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_config_recording_level_parents(
            &unsafe { c_str_to_string(name) },
            unsafe { metric_config_option(config) },
            unsafe { recording_level_of(recording_level) },
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_config_recording_level_parents) }
}

/// `sensor(String name, MetricConfig config, long inactiveSensorExpirationTimeSeconds, Sensor... parents)`.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_sensor_with_config_parents`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_config_inactive_sensor_expiration_time_seconds_parents(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
    config: *const kafka_common_metrics_MetricConfig_t,
    inactive_sensor_expiration_time_seconds: i64,
    parents: *const kafka_List_t,
    out_sensor_with_config_inactive_sensor_expiration_time_seconds_parents: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_config_inactive_sensor_expiration_time_seconds_parents(
            &unsafe { c_str_to_string(name) },
            unsafe { metric_config_option(config) },
            inactive_sensor_expiration_time_seconds,
            &unsafe { list_sensors(parents) },
        )
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_config_inactive_sensor_expiration_time_seconds_parents) }
}

/// `sensor_with_options(SensorOptions)`: the full-parameter form behind the
/// overloads above, taking the options built by
/// [`kafka_common_metrics_SensorOptionsBuilder_build`].
///
/// # Safety
///
/// `self_` must be a valid registry handle, `options` a valid options handle
/// and `out_sensor_with_options` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_sensor_with_options(
    self_: *const kafka_common_metrics_Metrics_t,
    options: *const kafka_common_metrics_SensorOptions_t,
    out_sensor_with_options: *mut *mut kafka_common_metrics_Sensor_t,
) -> *mut kafka_common_Error_t {
    let options = unsafe { &*(options as *const SensorOptionsInner) };
    let result = unsafe { metrics_ref(self_) }
        .sensor_with_options(options.options())
        .map(box_sensor);
    unsafe { deliver(result, out_sensor_with_options) }
}

/// `removeSensor(String name)`: removes the sensor, its metrics and its
/// child sensors; a handle on the sensor stays valid.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `name` NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_remove_sensor(
    self_: *const kafka_common_metrics_Metrics_t,
    name: *const c_char,
) {
    unsafe { metrics_ref(self_) }.remove_sensor(&unsafe { c_str_to_string(name) })
}

/// `addMetric(MetricName metricName, Measurable measurable)`: the name is
/// copied; `measurable` is consumed when owned and shared when it is a class
/// view. A metric already registered under the name is Java's
/// `IllegalArgumentException`.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `metric_name` a valid
/// metric-name handle and `measurable` a valid measurable handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_with_measurable(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    measurable: *mut kafka_common_metrics_Measurable_t,
) -> *mut kafka_common_Error_t {
    let measurable = Box::new(ArcMeasurable(unsafe { take_measurable(measurable) }));
    match unsafe { metrics_ref(self_) }
        .add_metric_with_measurable(unsafe { metric_name_ref(metric_name) }.clone(), measurable)
    {
        Ok(()) => std::ptr::null_mut(),
        Err(error) => box_error(error),
    }
}

/// `addMetric(MetricName metricName, MetricValueProvider<?> metricValueProvider)`:
/// the provider is shared with the metric.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `metric_name` a valid
/// metric-name handle and `metric_value_provider` a valid provider handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_with_metric_value_provider(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    metric_value_provider: *const kafka_common_metrics_MetricValueProvider_t,
) -> *mut kafka_common_Error_t {
    match unsafe { metrics_ref(self_) }
        .add_metric_with_metric_value_provider(unsafe { metric_name_ref(metric_name) }.clone(), unsafe {
            metric_value_provider_of(metric_value_provider)
        }) {
        Ok(()) => std::ptr::null_mut(),
        Err(error) => box_error(error),
    }
}

/// `addMetric(MetricName metricName, MetricConfig config, MetricValueProvider<?> metricValueProvider)`:
/// a `NULL` config means the registry's own.
///
/// # Safety
///
/// As [`kafka_common_metrics_Metrics_add_metric_with_metric_value_provider`],
/// with `config` null or a valid metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_with_config_metric_value_provider(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    provider: *const kafka_common_metrics_MetricValueProvider_t,
) -> *mut kafka_common_Error_t {
    match unsafe { metrics_ref(self_) }.add_metric_with_config_metric_value_provider(
        unsafe { metric_name_ref(metric_name) }.clone(),
        unsafe { metric_config_option(config) },
        unsafe { metric_value_provider_of(provider) },
    ) {
        Ok(()) => std::ptr::null_mut(),
        Err(error) => box_error(error),
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_metric_if_absent(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
    config: *const kafka_common_metrics_MetricConfig_t,
    provider: *const kafka_common_metrics_MetricValueProvider_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
    box_kafka_metric(unsafe { metrics_ref(self_) }.add_metric_if_absent(
        unsafe { metric_name_ref(metric_name) }.clone(),
        unsafe { metric_config_option(config) },
        unsafe { metric_value_provider_of(provider) },
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_remove_metric(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
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
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_add_reporter(
    self_: *const kafka_common_metrics_Metrics_t,
    reporter: *mut kafka_common_metrics_MetricsReporter_t,
) {
    unsafe { metrics_ref(self_) }.add_reporter(unsafe { take_metrics_reporter(reporter) })
}

/// `metrics()`: a snapshot of every registered metric as an owned map of
/// owned `kafka_common_MetricName_t *` keys to owned
/// `kafka_common_metrics_KafkaMetric_t *` values, in name order; freed with
/// `kafka_Map_destroy`, which frees both.
///
/// # Safety
///
/// `self_` must be a valid registry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metrics(
    self_: *const kafka_common_metrics_Metrics_t,
) -> *mut kafka_Map_t {
    let mut metrics: Vec<_> = unsafe { metrics_ref(self_) }.metrics().into_iter().collect();
    // A `HashMap` has no order; name order makes the snapshot reproducible.
    metrics.sort_by(|(a, _), (b, _)| (a.group(), a.name(), a.tags()).cmp(&(b.group(), b.name(), b.tags())));
    let entries = metrics
        .into_iter()
        .map(|(name, metric)| (box_metric_name(name) as *mut c_void, box_kafka_metric(metric) as *mut c_void))
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<MetricNameInner>),
        Some(destroy_kafka_metric_element),
        Some(metric_name_key_eq),
    )
}

/// Compares two `kafka_common_MetricName_t *` keys for `kafka_Map_get`.
///
/// # Safety
///
/// Both must be valid metric-name handles.
unsafe fn metric_name_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    let a = unsafe { metric_name_ref(a as *const kafka_common_MetricName_t) };
    let b = unsafe { metric_name_ref(b as *const kafka_common_MetricName_t) };
    a == b
}

/// `metric(MetricName metricName)`: the metric registered under the name,
/// `NULL` when none is. Owned handle, freed with
/// `kafka_common_metrics_KafkaMetric_destroy`.
///
/// # Safety
///
/// `self_` must be a valid registry handle and `metric_name` a valid
/// metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric(
    self_: *const kafka_common_metrics_Metrics_t,
    metric_name: *const kafka_common_MetricName_t,
) -> *mut kafka_common_metrics_KafkaMetric_t {
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
/// `self_` must be a valid registry handle, `template` a valid template
/// handle, `key_value` null or a valid list of NUL-terminated strings and
/// `out_metric_instance_with_key_value` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_instance_with_key_value(
    self_: *const kafka_common_metrics_Metrics_t,
    template: *const kafka_common_MetricNameTemplate_t,
    key_value: *const kafka_List_t,
    out_metric_instance_with_key_value: *mut *mut kafka_common_MetricName_t,
) -> *mut kafka_common_Error_t {
    let key_value = unsafe { list_strings(key_value) };
    let key_value: Vec<&str> = key_value.iter().map(String::as_str).collect();
    let result = unsafe { metrics_ref(self_) }
        .metric_instance_with_key_value(unsafe { metric_name_template_ref(template) }, &key_value)
        .map(box_metric_name);
    unsafe { deliver(result, out_metric_instance_with_key_value) }
}

/// `metricInstance(MetricNameTemplate template, Map<String, String> tags)`:
/// `tags` is a string-to-string map, copied.
///
/// # Safety
///
/// `self_` must be a valid registry handle, `template` a valid template
/// handle, `tags` null or a valid map of NUL-terminated strings and
/// `out_metric_instance_with_tags` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_metric_instance_with_tags(
    self_: *const kafka_common_metrics_Metrics_t,
    template: *const kafka_common_MetricNameTemplate_t,
    tags: *const kafka_Map_t,
    out_metric_instance_with_tags: *mut *mut kafka_common_MetricName_t,
) -> *mut kafka_common_Error_t {
    let result = unsafe { metrics_ref(self_) }
        .metric_instance_with_tags(
            unsafe { metric_name_template_ref(template) },
            unsafe { map_strings(tags) }.into_iter().collect::<BTreeMap<_, _>>(),
        )
        .map(box_metric_name);
    unsafe { deliver(result, out_metric_instance_with_tags) }
}

/// `close()`: closes every reporter. The handle stays valid until
/// [`kafka_common_metrics_Metrics_destroy`].
///
/// # Safety
///
/// `self_` must be a valid registry handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_close(self_: *const kafka_common_metrics_Metrics_t) {
    unsafe { metrics_ref(self_) }.close()
}

/// Frees an owned registry handle; null is a no-op. Sensor and metric
/// handles taken from it stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_Metrics_destroy(self_: *mut kafka_common_metrics_Metrics_t) {
    if !self_.is_null() {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptions_destroy(self_: *mut kafka_common_metrics_SensorOptions_t) {
    if !self_.is_null() {
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

unsafe fn builder_mut<'a>(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
) -> &'a mut SensorOptionsBuilderInner {
    unsafe { &mut *(self_ as *mut SensorOptionsBuilderInner) }
}

/// `SensorOptionsBuilder::new()`: every parameter unset or at its default.
/// Owned, freed with [`kafka_common_metrics_SensorOptionsBuilder_destroy`].
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
/// `self_` must be a valid builder handle and `name` NUL-terminated.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_name(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    name: *const c_char,
) {
    unsafe { builder_mut(self_) }.name = Some(unsafe { c_str_to_string(name) });
}

/// `set_config(config)`: the sensor's config, shared; `NULL` restores the
/// default of using the registry's.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `config` null or a valid
/// metric-config handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_config(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    config: *const kafka_common_metrics_MetricConfig_t,
) {
    unsafe { builder_mut(self_) }.config = unsafe { metric_config_option(config) };
}

/// `set_inactive_sensor_expiration_time_seconds(seconds)`: how long the
/// sensor may go unrecorded before the registry expires it.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_inactive_sensor_expiration_time_seconds(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    inactive_sensor_expiration_time_seconds: i64,
) {
    unsafe { builder_mut(self_) }.inactive_sensor_expiration_time_seconds = inactive_sensor_expiration_time_seconds;
}

/// `set_recording_level(recording_level)`.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `recording_level` a
/// recording-level singleton.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_recording_level(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) {
    unsafe { builder_mut(self_) }.recording_level = unsafe { recording_level_of(recording_level) };
}

/// `set_parents(parents)`: `parents` holds
/// `const kafka_common_metrics_Sensor_t *` elements, shared; the list is
/// read during the call.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `parents` null or a valid
/// list of sensor handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_set_parents(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    parents: *const kafka_List_t,
) {
    unsafe { builder_mut(self_) }.parents = unsafe { list_sensors(parents) };
}

/// `build()`: validates the mandatory parameters through the Rust builder
/// (an unset `name` is Java's `IllegalArgumentException`) and delivers an
/// owned options handle, freed with
/// [`kafka_common_metrics_SensorOptions_destroy`]. The builder stays usable.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `out_build` writable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_build(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
    out_build: *mut *mut kafka_common_metrics_SensorOptions_t,
) -> *mut kafka_common_Error_t {
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
    unsafe { deliver(result, out_build) }
}

/// Frees an owned builder handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_SensorOptionsBuilder_destroy(
    self_: *mut kafka_common_metrics_SensorOptionsBuilder_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut SensorOptionsBuilderInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
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
    use crate::ffi::common::metrics::metric_value_provider::{
        kafka_common_metrics_MetricValueProvider_destroy, kafka_common_metrics_MetricValueProvider_measurable,
    };
    use crate::ffi::common::metrics::sensor::{
        kafka_common_metrics_Sensor_RecordingLevel_debug, kafka_common_metrics_Sensor_destroy,
        kafka_common_metrics_Sensor_name, kafka_common_metrics_Sensor_should_record, sensor_ref,
    };
    use crate::ffi::common::metrics::stats::avg::{
        kafka_common_metrics_stats_Avg__as_Measurable, kafka_common_metrics_stats_Avg_destroy,
        kafka_common_metrics_stats_Avg_new,
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
    /// frees the handle; `error` is the overload's result.
    unsafe fn existing_info_sensor(error: *mut kafka_common_Error_t, plain: *mut kafka_common_metrics_Sensor_t) {
        assert!(error.is_null());
        assert_eq!(
            unsafe { kafka_common_metrics_Sensor_should_record(plain) },
            1,
            "the existing INFO sensor is returned"
        );
        unsafe { kafka_common_metrics_Sensor_destroy(plain) };
    }

    #[test]
    fn names_sensors_and_metrics_round_trip_through_the_registry() {
        let registry = kafka_common_metrics_Metrics_new();
        let (name, group, description) = (c("n"), c("g"), c("d"));
        let (tag_key, tag_value) = (c("k"), c("v"));
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
            let mut from_pairs = ptr::null_mut();
            let error = kafka_common_metrics_Metrics_metric_name_with_description_key_value(
                registry,
                name.as_ptr(),
                group.as_ptr(),
                description.as_ptr(),
                pairs,
                &mut from_pairs,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
            kafka_List_add(pairs, tag_value.as_ptr() as *mut c_void);
            assert!(
                kafka_common_metrics_Metrics_metric_name_with_description_key_value(
                    registry,
                    name.as_ptr(),
                    group.as_ptr(),
                    description.as_ptr(),
                    pairs,
                    &mut from_pairs,
                )
                .is_null()
            );
            assert_eq!(*metric_name_ref(from_pairs), expected);
            kafka_common_MetricName_destroy(from_pairs);
            kafka_List_destroy(pairs);

            // Sensors: get-or-create, lookup, parents, removal.
            let sensor_name = c("s");
            assert!(kafka_common_metrics_Metrics_get_sensor(registry, sensor_name.as_ptr()).is_null());
            let mut sensor = ptr::null_mut();
            assert!(
                kafka_common_metrics_Metrics_sensor_with_recording_level(
                    registry,
                    sensor_name.as_ptr(),
                    kafka_common_metrics_Sensor_RecordingLevel_debug(),
                    &mut sensor
                )
                .is_null()
            );
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
            let mut child = ptr::null_mut();
            assert!(
                kafka_common_metrics_Metrics_sensor_with_parents(registry, child_name.as_ptr(), parents, &mut child)
                    .is_null()
            );
            kafka_List_destroy(parents);
            // Every other overload finds the INFO sensor the first call created.
            let plain_name = c("plain");
            let mut plain = ptr::null_mut();
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor(registry, plain_name.as_ptr(), &mut plain),
                plain,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_recording_level_parents(
                    registry,
                    plain_name.as_ptr(),
                    kafka_common_metrics_Sensor_RecordingLevel_debug(),
                    ptr::null(),
                    &mut plain,
                ),
                plain,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_config_parents(
                    registry,
                    plain_name.as_ptr(),
                    ptr::null(),
                    ptr::null(),
                    &mut plain,
                ),
                plain,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_config_recording_level_parents(
                    registry,
                    plain_name.as_ptr(),
                    ptr::null(),
                    kafka_common_metrics_Sensor_RecordingLevel_debug(),
                    ptr::null(),
                    &mut plain,
                ),
                plain,
            );
            existing_info_sensor(
                kafka_common_metrics_Metrics_sensor_with_config_inactive_sensor_expiration_time_seconds_parents(
                    registry,
                    plain_name.as_ptr(),
                    ptr::null(),
                    60,
                    ptr::null(),
                    &mut plain,
                ),
                plain,
            );

            // Metrics: add through a stat's Measurable view, look up, snapshot, remove.
            let avg = kafka_common_metrics_stats_Avg_new();
            assert!(
                kafka_common_metrics_Metrics_add_metric_with_measurable(
                    registry,
                    tagged,
                    kafka_common_metrics_stats_Avg__as_Measurable(avg) as *mut _
                )
                .is_null()
            );
            let duplicate = kafka_common_metrics_Metrics_add_metric_with_measurable(
                registry,
                tagged,
                kafka_common_metrics_stats_Avg__as_Measurable(avg) as *mut _,
            );
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
        unsafe {
            let config = kafka_common_metrics_MetricConfig_new();
            kafka_common_metrics_MetricConfig_set_samples(config, 7);
            let registry = kafka_common_metrics_Metrics_with_default_config(config);
            let shared = kafka_common_metrics_Metrics_config(registry);
            assert_eq!(kafka_common_metrics_MetricConfig_samples(shared), 7);
            kafka_common_metrics_MetricConfig_destroy(shared);

            let metric_name = kafka_common_metrics_Metrics_metric_name(registry, name.as_ptr(), group.as_ptr());
            let provider = kafka_common_metrics_MetricValueProvider_measurable(
                kafka_common_metrics_stats_Avg__as_Measurable(kafka_common_metrics_stats_Avg_new()) as *mut _,
            );
            assert!(
                kafka_common_metrics_Metrics_add_metric_with_metric_value_provider(registry, metric_name, provider)
                    .is_null()
            );
            let duplicate = kafka_common_metrics_Metrics_add_metric_with_config_metric_value_provider(
                registry,
                metric_name,
                config,
                provider,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(duplicate), 1);
            kafka_common_Error_destroy(duplicate);
            let existing =
                kafka_common_metrics_Metrics_add_metric_if_absent(registry, metric_name, ptr::null(), provider);
            let registered = kafka_common_metrics_Metrics_metric(registry, metric_name);
            assert!(Arc::ptr_eq(kafka_metric_ref(existing), kafka_metric_ref(registered)));
            kafka_common_metrics_KafkaMetric_destroy(existing);
            kafka_common_metrics_KafkaMetric_destroy(registered);
            kafka_common_metrics_MetricValueProvider_destroy(provider);
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
            let mut instance = ptr::null_mut();
            assert!(
                kafka_common_metrics_Metrics_metric_instance_with_key_value(registry, template, pairs, &mut instance)
                    .is_null()
            );
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
            let error =
                kafka_common_metrics_Metrics_metric_instance_with_tags(registry, template, wrong_tags, &mut instance);
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            kafka_common_Error_destroy(error);
            kafka_Map_destroy(wrong_tags);
            kafka_common_MetricNameTemplate_destroy(template);
            kafka_common_metrics_MetricConfig_destroy(config);
            kafka_common_metrics_Metrics_destroy(registry);
        }
    }

    #[test]
    fn the_options_builder_validates_the_name_and_feeds_sensor_with_options() {
        let registry = kafka_common_metrics_Metrics_new();
        unsafe {
            let builder = kafka_common_metrics_SensorOptionsBuilder_new();
            let mut options = ptr::null_mut();
            let error = kafka_common_metrics_SensorOptionsBuilder_build(builder, &mut options);
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "SensorOptionsBuilder::build: mandatory parameter `name` was not set"
            );
            kafka_common_Error_destroy(error);

            let parent_name = c("parent");
            let mut parent = ptr::null_mut();
            assert!(kafka_common_metrics_Metrics_sensor(registry, parent_name.as_ptr(), &mut parent).is_null());
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
            assert!(kafka_common_metrics_SensorOptionsBuilder_build(builder, &mut options).is_null());
            let inner = &*(options as *const SensorOptionsInner);
            assert_eq!(inner.name, "child");
            assert!(inner.config.is_some());
            assert_eq!(inner.inactive_sensor_expiration_time_seconds, 30);
            assert_eq!(inner.recording_level, RecordingLevel::Debug);
            assert_eq!(inner.parents.len(), 1);

            let mut child = ptr::null_mut();
            assert!(kafka_common_metrics_Metrics_sensor_with_options(registry, options, &mut child).is_null());
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
            let mut rebuilt = ptr::null_mut();
            assert!(kafka_common_metrics_SensorOptionsBuilder_build(builder, &mut rebuilt).is_null());
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

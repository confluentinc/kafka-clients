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

//! `kafka_common_metrics_MetricConfig_t`:
//! `org.apache.kafka.common.metrics.MetricConfig` (CLAUDE.md §4).
//!
//! Rust's setters consume and return the config (`set_quota(mut self, ..)
//! -> Self`); in C they update the handle in place. A handle a getter
//! returned (`Metrics_config`, `KafkaMetric_config`) shares its config with
//! the registry or metric it came from until the first setter, which gives
//! the handle its own copy: setting through a handle never changes a config
//! already in use, as in Java where `MetricConfig` is passed by reference
//! but every registered config is read-only in practice.

use std::collections::BTreeMap;
use std::ptr;
use std::sync::Arc;

use crate::common::metrics::MetricConfig;
use crate::ffi::common::metrics::quota::{box_quota, kafka_common_metrics_Quota_t, quota_of};
use crate::ffi::common::metrics::sensor::{
    kafka_common_metrics_Sensor_RecordingLevel_t, recording_level_of, recording_level_singleton,
};
use crate::ffi::common::metrics::time_unit::{kafka_common_metrics_TimeUnit_t, value_of as time_unit_of};
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_string_map, kafka_Map_t, map_strings};

/// Opaque handle to a [`MetricConfig`].
#[repr(C)]
pub struct kafka_common_metrics_MetricConfig_t {
    _private: [u8; 0],
}

/// Where a handle's config lives.
enum MetricConfigImpl {
    /// Shared with the registry, metric or sensor it came from, or the
    /// handle's own after a setter.
    Shared(Arc<MetricConfig>),
    /// Borrowed for the duration of an interface call Rust makes into C.
    Borrowed(*const MetricConfig),
}

/// What a [`kafka_common_metrics_MetricConfig_t`] points at.
pub(crate) struct MetricConfigInner {
    config: MetricConfigImpl,
}

// SAFETY: the borrowed pointer targets a `MetricConfig` (`Send + Sync`) that
// outlives the handle, which only exists for the duration of one call.
unsafe impl Send for MetricConfigInner {}
// SAFETY: `Sync` because a `&MetricConfigInner` gives only shared access to the config: `Shared`
// holds an `Arc<MetricConfig>`, and `Borrowed` a pointer read only as `&MetricConfig`, which is
// `Sync`. Every mutation goes through `inner_mut`, whose callers hold the handle exclusively.
unsafe impl Sync for MetricConfigInner {}

impl MetricConfigInner {
    /// A handle sharing `config`.
    pub(crate) fn shared(config: Arc<MetricConfig>) -> Self {
        Self { config: MetricConfigImpl::Shared(config) }
    }

    /// A handle borrowing `config` for the duration of a call into C.
    pub(crate) fn borrowed(config: &MetricConfig) -> Self {
        Self { config: MetricConfigImpl::Borrowed(config) }
    }

    /// The config.
    pub(crate) fn config(&self) -> &MetricConfig {
        match &self.config {
            MetricConfigImpl::Shared(config) => config,
            // SAFETY: `Borrowed` is built only by `MetricConfigInner::borrowed` from a
            // `&MetricConfig`, and each of its callers (the interface invokers such as
            // `CMeasurable::measure`) keeps the handle a local of the call that holds that borrow,
            // so the config outlives `self`. Nothing writes through a borrowed handle, since every
            // setter requires an owned one, so the shared reference is sound for the lifetime of
            // `&self`.
            MetricConfigImpl::Borrowed(config) => unsafe { &**config },
        }
    }

    /// The config as the `Arc` the Rust API takes, sharing a shared handle's
    /// and copying a borrowed one.
    pub(crate) fn arc(&self) -> Arc<MetricConfig> {
        match &self.config {
            MetricConfigImpl::Shared(config) => Arc::clone(config),
            // SAFETY: `Borrowed` is built only by `MetricConfigInner::borrowed` from a
            // `&MetricConfig`, and each of its callers (the interface invokers such as
            // `CMeasurable::measure`) keeps the handle a local of the call that holds that borrow,
            // so the config outlives `self`. Nothing writes through a borrowed handle, since every
            // setter requires an owned one, so the shared reference is sound while the config is
            // cloned into the new `Arc`.
            MetricConfigImpl::Borrowed(config) => Arc::new(unsafe { &**config }.clone()),
        }
    }

    /// Applies a consuming Rust setter: the handle gets its own copy of the
    /// updated config, leaving whoever shared the previous one untouched.
    fn update(&mut self, set: impl FnOnce(MetricConfig) -> MetricConfig) {
        let updated = set(self.config().clone());
        self.config = MetricConfigImpl::Shared(Arc::new(updated));
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_metrics_MetricConfig_t {
        self as *const Self as *const kafka_common_metrics_MetricConfig_t
    }
}

/// Hands `config` to C as an owned handle sharing it, freed with
/// [`kafka_common_metrics_MetricConfig_destroy`].
pub(crate) fn box_metric_config(config: Arc<MetricConfig>) -> *mut kafka_common_metrics_MetricConfig_t {
    Box::into_raw(Box::new(MetricConfigInner::shared(config))) as *mut kafka_common_metrics_MetricConfig_t
}

/// What a handle points at.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle, alive for `'a`.
unsafe fn inner<'a>(self_: *const kafka_common_metrics_MetricConfig_t) -> &'a MetricConfigInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle alive for
    // `'a`, so it points at a live `MetricConfigInner`: an owned one, the `Box::into_raw` of
    // `box_metric_config`, or one an interface invoker borrows to C for the call
    // (`MetricConfigInner::as_ptr`), which outlives the call. Only `inner_mut` gives exclusive
    // access, and only to an owned handle no other call uses at the same time, so the shared
    // reference is sound for `'a`.
    unsafe { &*(self_ as *const MetricConfigInner) }
}

/// What an owned handle points at, for a setter.
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle, alive for `'a` and
/// used by no other call during it.
unsafe fn inner_mut<'a>(self_: *mut kafka_common_metrics_MetricConfig_t) -> &'a mut MetricConfigInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle, the
    // `Box::into_raw` of `box_metric_config`, alive for `'a` and used by nothing else during it, so
    // the exclusive reference aliases nothing for `'a`.
    unsafe { &mut *(self_ as *mut MetricConfigInner) }
}

/// The config behind a handle.
///
/// # Safety
///
/// `config` must be a valid metric-config handle, alive for `'a`.
pub(crate) unsafe fn metric_config_ref<'a>(config: *const kafka_common_metrics_MetricConfig_t) -> &'a MetricConfig {
    // SAFETY: Per this function's `# Safety`, `config` is a valid metric-config handle, as `inner`
    // requires; the reference returned lives for the same `'a`.
    unsafe { inner(config) }.config()
}

/// The config behind a handle as the `Arc` the Rust API takes.
///
/// # Safety
///
/// `config` must be a valid metric-config handle.
pub(crate) unsafe fn metric_config_arc(config: *const kafka_common_metrics_MetricConfig_t) -> Arc<MetricConfig> {
    // SAFETY: Per this function's `# Safety`, `config` is a valid metric-config handle, as `inner`
    // requires; the reference is used only within this call, since `arc` returns an owned `Arc`.
    unsafe { inner(config) }.arc()
}

/// The config behind a handle, or `None` for null (Java's null config, the
/// default one).
///
/// # Safety
///
/// `config` must be null or a valid metric-config handle.
pub(crate) unsafe fn metric_config_option(
    config: *const kafka_common_metrics_MetricConfig_t,
) -> Option<Arc<MetricConfig>> {
    if config.is_null() {
        None
    } else {
        // SAFETY: `config` is non-null (checked above) and, per this function's `# Safety`, a valid
        // metric-config handle, as `metric_config_arc` requires; the `Arc` returned is owned.
        Some(unsafe { metric_config_arc(config) })
    }
}

/// `new MetricConfig()`: the defaults. Owned, freed with
/// [`kafka_common_metrics_MetricConfig_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_metrics_MetricConfig_new() -> *mut kafka_common_metrics_MetricConfig_t {
    box_metric_config(Arc::new(MetricConfig::new()))
}

/// `quota()`: an owned copy freed with `kafka_common_metrics_Quota_destroy`,
/// or null when none is set.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_quota(
    self_: *const kafka_common_metrics_MetricConfig_t,
) -> *mut kafka_common_metrics_Quota_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle, as
    // `metric_config_ref` requires; the quota is copied into an owned handle before return, so the
    // borrow lasts for this call only.
    unsafe { metric_config_ref(self_) }.quota().map_or(ptr::null_mut(), box_quota)
}

/// `quota(Quota quota)`: `quota` is copied.
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle that no other call
/// uses at the same time, and `quota` a valid quota handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_set_quota(
    self_: *mut kafka_common_metrics_MetricConfig_t,
    quota: *const kafka_common_metrics_Quota_t,
) {
    // SAFETY: Per this function's `# Safety`, `quota` is a valid quota handle, as `quota_of`
    // requires; the `Copy` quota is read out, so the borrow lasts for this call only.
    let quota = unsafe { quota_of(quota) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle that no
    // other call uses at the same time, as `inner_mut` requires; the exclusive reference is used
    // only within this call.
    unsafe { inner_mut(self_) }.update(|config| config.set_quota(quota));
}

/// `eventWindow()`.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_event_window(
    self_: *const kafka_common_metrics_MetricConfig_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle, as
    // `metric_config_ref` requires; the reference is used only within this call.
    unsafe { metric_config_ref(self_) }.event_window()
}

/// `eventWindow(long window)`.
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle that no other call
/// uses at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_set_event_window(
    self_: *mut kafka_common_metrics_MetricConfig_t,
    window: i64,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle that no
    // other call uses at the same time, as `inner_mut` requires; the exclusive reference is used
    // only within this call.
    unsafe { inner_mut(self_) }.update(|config| config.set_event_window(window));
}

/// `timeWindowMs()`.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_time_window_ms(
    self_: *const kafka_common_metrics_MetricConfig_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle, as
    // `metric_config_ref` requires; the reference is used only within this call.
    unsafe { metric_config_ref(self_) }.time_window_ms()
}

/// `timeWindow(long window, TimeUnit unit)`.
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle that no other call
/// uses at the same time, and `unit` a time-unit singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_set_time_window(
    self_: *mut kafka_common_metrics_MetricConfig_t,
    window: i64,
    unit: *const kafka_common_metrics_TimeUnit_t,
) {
    // SAFETY: Per this function's `# Safety`, `unit` is a time-unit singleton, as `time_unit_of`
    // (`time_unit::value_of`) requires; the value is copied out.
    let unit = unsafe { time_unit_of(unit) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle that no
    // other call uses at the same time, as `inner_mut` requires; the exclusive reference is used
    // only within this call.
    unsafe { inner_mut(self_) }.update(|config| config.set_time_window(window, unit));
}

/// `tags()`: an owned map of `char *` to `char *`, in key order, freed with
/// `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_tags(
    self_: *const kafka_common_metrics_MetricConfig_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle, as
    // `metric_config_ref` requires; the tags are copied into an owned map before return, so the
    // borrow lasts for this call only.
    box_string_map(unsafe { metric_config_ref(self_) }.tags())
}

/// `tags(Map<String, String> tags)`: `tags` maps `const char *` to
/// `const char *` and is copied.
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle that no other call
/// uses at the same time, and `tags` null or a valid map of NUL-terminated
/// strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_set_tags(
    self_: *mut kafka_common_metrics_MetricConfig_t,
    tags: *const kafka_Map_t,
) {
    // SAFETY: Per this function's `# Safety`, `tags` is null or a valid map of NUL-terminated
    // strings, as `map_strings` requires; the strings are copied into owned `String`s, so nothing
    // borrows the map after this call.
    let tags: BTreeMap<String, String> = unsafe { map_strings(tags) }.into_iter().collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle that no
    // other call uses at the same time, as `inner_mut` requires; the exclusive reference is used
    // only within this call.
    unsafe { inner_mut(self_) }.update(|config| config.set_tags(tags));
}

/// `samples()`.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_samples(
    self_: *const kafka_common_metrics_MetricConfig_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle, as
    // `metric_config_ref` requires; the reference is used only within this call.
    unsafe { metric_config_ref(self_) }.samples()
}

/// `samples(int samples)`: `samples` must be at least 1, as Java requires
/// (Java throws `IllegalArgumentException` below it). A smaller value breaks
/// that precondition and is not reported as an error (CLAUDE.md §4).
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle that no other call
/// uses at the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_set_samples(
    self_: *mut kafka_common_metrics_MetricConfig_t,
    samples: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle that no
    // other call uses at the same time, as `inner_mut` requires; the exclusive reference is used
    // only within this call.
    unsafe { inner_mut(self_) }.update(|config| config.set_samples(samples));
}

/// `recordLevel()`: the recording-level singleton.
///
/// # Safety
///
/// `self_` must be a valid metric-config handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_record_level(
    self_: *const kafka_common_metrics_MetricConfig_t,
) -> *const kafka_common_metrics_Sensor_RecordingLevel_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-config handle, as
    // `metric_config_ref` requires; the value is copied out and mapped to its static singleton, so
    // the borrow lasts for this call only.
    recording_level_singleton(unsafe { metric_config_ref(self_) }.record_level())
}

/// `recordLevel(Sensor.RecordingLevel recordingLevel)`.
///
/// # Safety
///
/// `self_` must be a valid owned metric-config handle that no other call
/// uses at the same time, and `recording_level` a recording-level
/// singleton.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_set_record_level(
    self_: *mut kafka_common_metrics_MetricConfig_t,
    recording_level: *const kafka_common_metrics_Sensor_RecordingLevel_t,
) {
    // SAFETY: Per this function's `# Safety`, `recording_level` is a recording-level singleton, as
    // `recording_level_of` requires; the value is copied out.
    let recording_level = unsafe { recording_level_of(recording_level) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid owned metric-config handle that no
    // other call uses at the same time, as `inner_mut` requires; the exclusive reference is used
    // only within this call.
    unsafe { inner_mut(self_) }.update(|config| config.set_record_level(recording_level));
}

/// Frees an owned metric-config handle; null is a no-op. The handle Rust
/// passes to an interface method is borrowed for the call and never passed
/// here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_metrics_MetricConfig_destroy(self_: *mut kafka_common_metrics_MetricConfig_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `MetricConfigInner` in
        // `box_metric_config` (the handle an interface invoker borrows to C is never passed here),
        // so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut MetricConfigInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;

    use super::*;
    use crate::common::metrics::{Quota, RecordingLevel, TimeUnit};
    use crate::ffi::common::metrics::quota::kafka_common_metrics_Quota_destroy;
    use crate::ffi::common::metrics::sensor::{
        kafka_common_metrics_Sensor_RecordingLevel_debug, kafka_common_metrics_Sensor_RecordingLevel_info,
    };
    use crate::ffi::common::metrics::time_unit::kafka_common_metrics_TimeUnit_seconds;
    use crate::ffi::util::{kafka_Map_destroy, kafka_Map_new, kafka_Map_put};

    #[test]
    fn setters_update_in_place_without_touching_a_shared_config() {
        let shared = Arc::new(MetricConfig::new());
        let handle = box_metric_config(Arc::clone(&shared));
        let key = CString::new("k").unwrap();
        let value = CString::new("v").unwrap();
        // SAFETY: Test code: `handle` comes from `box_metric_config` and is destroyed once, at the
        // end, and only this test uses it; each quota handle from `box_quota` or the `quota` getter
        // and each map from `kafka_Map_new` or the `tags` getter is destroyed once, after its last
        // use; `key` and `value` outlive the map borrowing them; the singletons are never freed.
        unsafe {
            assert!(kafka_common_metrics_MetricConfig_quota(handle).is_null());
            assert_eq!(
                kafka_common_metrics_MetricConfig_samples(handle),
                MetricConfig::DEFAULT_NUM_SAMPLES
            );
            assert_eq!(
                kafka_common_metrics_MetricConfig_record_level(handle),
                kafka_common_metrics_Sensor_RecordingLevel_info()
            );
            assert!(
                Arc::ptr_eq(&metric_config_arc(handle), &shared),
                "a getter's handle shares the config"
            );

            let quota = box_quota(Quota::upper_bound(5.0));
            kafka_common_metrics_MetricConfig_set_quota(handle, quota);
            kafka_common_metrics_Quota_destroy(quota);
            kafka_common_metrics_MetricConfig_set_event_window(handle, 7);
            kafka_common_metrics_MetricConfig_set_time_window(handle, 3, kafka_common_metrics_TimeUnit_seconds());
            kafka_common_metrics_MetricConfig_set_samples(handle, 4);
            kafka_common_metrics_MetricConfig_set_record_level(
                handle,
                kafka_common_metrics_Sensor_RecordingLevel_debug(),
            );
            let tags = kafka_Map_new();
            kafka_Map_put(tags, key.as_ptr() as *mut _, value.as_ptr() as *mut _);
            kafka_common_metrics_MetricConfig_set_tags(handle, tags);
            kafka_Map_destroy(tags);

            let quota = kafka_common_metrics_MetricConfig_quota(handle);
            assert_eq!(quota_of(quota), Quota::upper_bound(5.0));
            kafka_common_metrics_Quota_destroy(quota);
            assert_eq!(kafka_common_metrics_MetricConfig_event_window(handle), 7);
            assert_eq!(
                kafka_common_metrics_MetricConfig_time_window_ms(handle),
                TimeUnit::Seconds.to_millis(3)
            );
            assert_eq!(kafka_common_metrics_MetricConfig_samples(handle), 4);
            assert_eq!(
                kafka_common_metrics_MetricConfig_record_level(handle),
                kafka_common_metrics_Sensor_RecordingLevel_debug()
            );
            let tags = kafka_common_metrics_MetricConfig_tags(handle);
            assert_eq!(map_strings(tags), [("k".to_string(), "v".to_string())]);
            kafka_Map_destroy(tags);

            // The shared config is untouched; the handle has its own copy.
            assert!(shared.quota().is_none());
            assert_eq!(shared.samples(), MetricConfig::DEFAULT_NUM_SAMPLES);
            assert!(!Arc::ptr_eq(&metric_config_arc(handle), &shared));
            assert_eq!(metric_config_ref(handle).record_level(), RecordingLevel::Debug);
            assert!(metric_config_option(ptr::null()).is_none());
            assert!(metric_config_option(handle).is_some());
            kafka_common_metrics_MetricConfig_destroy(handle);
            kafka_common_metrics_MetricConfig_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn a_borrowed_handle_reads_the_config_and_copies_it_into_an_arc() {
        let config = MetricConfig::new().set_samples(9);
        let borrowed = MetricConfigInner::borrowed(&config);
        // SAFETY: Test code: `borrowed` borrows `config`, which outlives it, and the handle from
        // `as_ptr` is used only while `borrowed` lives and only read.
        unsafe {
            assert_eq!(kafka_common_metrics_MetricConfig_samples(borrowed.as_ptr()), 9);
            assert_eq!(metric_config_arc(borrowed.as_ptr()).samples(), 9);
        }
    }
}

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

//! `kafka_common_MetricName_t`: `org.apache.kafka.common.MetricName`
//! (CLAUDE.md §4).
//!
//! The handle owns a [`MetricName`] plus the NUL-terminated copies of its
//! three strings the borrowed getters hand out.

use std::collections::BTreeMap;
use std::ffi::{CString, c_char, c_void};

use crate::common::MetricName;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_string_map, c_str_to_string, into_c_string, kafka_Map_t, map_strings, owned_c_string};

/// Opaque handle to a [`MetricName`].
#[repr(C)]
pub struct kafka_common_MetricName_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_MetricName_t`] points at: the value plus the
/// NUL-terminated strings its getters borrow out.
pub(crate) struct MetricNameInner {
    name: MetricName,
    name_c: CString,
    group_c: CString,
    description_c: CString,
}

impl MetricNameInner {
    pub(crate) fn new(name: MetricName) -> Self {
        let name_c = owned_c_string(name.name());
        let group_c = owned_c_string(name.group());
        let description_c = owned_c_string(name.description());
        Self { name, name_c, group_c, description_c }
    }

    pub(crate) fn metric_name(&self) -> &MetricName {
        &self.name
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_MetricName_t {
        self as *const Self as *const kafka_common_MetricName_t
    }
}

/// Hands `name` to C as an owned handle, freed with
/// [`kafka_common_MetricName_destroy`].
pub(crate) fn box_metric_name(name: MetricName) -> *mut kafka_common_MetricName_t {
    Box::into_raw(Box::new(MetricNameInner::new(name))) as *mut kafka_common_MetricName_t
}

/// The metric name behind a handle.
///
/// # Safety
///
/// `name` must be a valid metric-name handle, alive for `'a`.
pub(crate) unsafe fn metric_name_ref<'a>(name: *const kafka_common_MetricName_t) -> &'a MetricName {
    // SAFETY: Per this function's `# Safety`, `name` is a valid metric-name handle alive for `'a`,
    // so it points at a live `MetricNameInner`: the `Box::into_raw` of `box_metric_name`, or one a
    // holder (a metric, a named measurable, a `QuotaViolationError` payload) stores and hands out
    // through `MetricNameInner::as_ptr`, which lives as long as that holder. No call mutates a
    // `MetricNameInner`, so the shared reference is sound for `'a`.
    unsafe { &*(name as *const MetricNameInner) }.metric_name()
}

/// What a handle points at.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle, alive for `'a`.
unsafe fn inner<'a>(self_: *const kafka_common_MetricName_t) -> &'a MetricNameInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name handle alive for `'a`,
    // so it points at a live `MetricNameInner`: the `Box::into_raw` of `box_metric_name`, or one a
    // holder (a metric, a named measurable, a `QuotaViolationError` payload) stores and hands out
    // through `MetricNameInner::as_ptr`, which lives as long as that holder. No call mutates a
    // `MetricNameInner`, so the shared reference is sound for `'a`.
    unsafe { &*(self_ as *const MetricNameInner) }
}

/// `new MetricName(String name, String group, String description,
/// Map<String, String> tags)`: `tags` maps `const char *` to `const char *`;
/// everything is copied. Owned, freed with
/// [`kafka_common_MetricName_destroy`].
///
/// # Safety
///
/// The strings must be valid NUL-terminated strings and `tags` null or a
/// valid map of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_new(
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    tags: *const kafka_Map_t,
) -> *mut kafka_common_MetricName_t {
    // SAFETY: Per this function's `# Safety`, `tags` is null or a valid map of NUL-terminated
    // strings, as `map_strings` requires; the strings are copied into owned pairs, so nothing
    // borrows the map after this call.
    let tags: BTreeMap<String, String> = unsafe { map_strings(tags) }.into_iter().collect();
    box_metric_name(MetricName::new(
        // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(name) },
        // SAFETY: Per this function's `# Safety`, `group` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(group) },
        // SAFETY: Per this function's `# Safety`, `description` is a valid NUL-terminated string,
        // as `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(description) },
        tags,
    ))
}

/// `name()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_name(self_: *const kafka_common_MetricName_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name handle, as `inner`
    // requires. The string returned is the inner's own `name_c`, so it lives as long as the handle;
    // that is the rustdoc's "borrowed from the handle, valid as long as it is".
    unsafe { inner(self_) }.name_c.as_ptr()
}

/// `group()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_group(self_: *const kafka_common_MetricName_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name handle, as `inner`
    // requires. The string returned is the inner's own `group_c`, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle, valid as long as it is".
    unsafe { inner(self_) }.group_c.as_ptr()
}

/// `description()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_description(self_: *const kafka_common_MetricName_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name handle, as `inner`
    // requires. The string returned is the inner's own `description_c`, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle, valid as long as it is".
    unsafe { inner(self_) }.description_c.as_ptr()
}

/// `tags()`: an owned map of `char *` to `char *` sorted by key, freed with
/// `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_tags(self_: *const kafka_common_MetricName_t) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name handle, as
    // `metric_name_ref` requires; the reference is used only within this call; the tags are copied
    // into the owned map returned.
    box_string_map(unsafe { metric_name_ref(self_) }.tags())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_to_string(self_: *const kafka_common_MetricName_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name handle, as
    // `metric_name_ref` requires; the reference is used only within this call, and the text is
    // copied into an owned string.
    into_c_string(&unsafe { metric_name_ref(self_) }.to_string())
}

/// Frees an owned metric-name handle; null is a no-op. A handle borrowed
/// from a metric or an error payload is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned metric-name handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_destroy(self_: *mut kafka_common_MetricName_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // metric-name handle not yet destroyed, i.e. the `Box::into_raw` of `box_metric_name`, so
        // `Box::from_raw` reclaims it exactly once. A handle borrowed from a holder is never passed
        // here, as the rustdoc states.
        drop(unsafe { Box::from_raw(self_ as *mut MetricNameInner) });
    }
}

/// Key equality for `kafka_Map_t`s keyed by `kafka_common_MetricName_t`
/// (`metrics()` on the producer and the consumer).
///
/// # Safety
///
/// `a` and `b` must be valid metric-name handles.
pub(crate) unsafe fn metric_name_eq(a: *mut c_void, b: *mut c_void) -> bool {
    // SAFETY: Per this function's `# Safety`, `a` and `b` are valid metric-name handles, as
    // `metric_name_ref` requires; both references are used only for the comparison.
    unsafe { metric_name_ref(a as *const _) == metric_name_ref(b as *const _) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_void};
    use std::ptr;

    use super::*;
    use crate::ffi::util::{
        kafka_Map_destroy, kafka_Map_get, kafka_Map_key, kafka_Map_new, kafka_Map_put, kafka_Map_size,
        kafka_string_destroy,
    };

    #[test]
    fn handle_round_trips_strings_tags_and_display() {
        let name = CString::new("records-sent").unwrap();
        let group = CString::new("producer-metrics").unwrap();
        let description = CString::new("Records sent").unwrap();
        let client_id = CString::new("client-id").unwrap();
        let c1 = CString::new("c1").unwrap();
        let topic = CString::new("topic").unwrap();
        let t = CString::new("t").unwrap();
        // SAFETY: Test code: the `CString`s outlive every call reading them; `tags` comes from
        // `kafka_Map_new`, borrows the strings, and is destroyed once, after the constructor copies
        // it; each name handle comes from `kafka_common_MetricName_new` and is destroyed once,
        // after the strings it lends, the owned tag map and the string from `to_string`, each
        // destroyed once, are read.
        unsafe {
            let tags = kafka_Map_new();
            kafka_Map_put(tags, topic.as_ptr() as *mut c_void, t.as_ptr() as *mut c_void);
            kafka_Map_put(tags, client_id.as_ptr() as *mut c_void, c1.as_ptr() as *mut c_void);
            let handle = kafka_common_MetricName_new(name.as_ptr(), group.as_ptr(), description.as_ptr(), tags);
            kafka_Map_destroy(tags);

            let expected = MetricName::new(
                "records-sent",
                "producer-metrics",
                "Records sent",
                [
                    ("client-id".to_string(), "c1".to_string()),
                    ("topic".to_string(), "t".to_string()),
                ]
                .into_iter()
                .collect(),
            );
            assert_eq!(metric_name_ref(handle), &expected);
            assert_eq!(
                CStr::from_ptr(kafka_common_MetricName_name(handle)).to_str().unwrap(),
                "records-sent"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_MetricName_group(handle)).to_str().unwrap(),
                "producer-metrics"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_MetricName_description(handle)).to_str().unwrap(),
                "Records sent"
            );

            // Tags come back sorted by key and are found by content.
            let out = kafka_common_MetricName_tags(handle);
            assert_eq!(kafka_Map_size(out), 2);
            assert_eq!(
                CStr::from_ptr(kafka_Map_key(out, 0) as *const c_char).to_str().unwrap(),
                "client-id"
            );
            assert_eq!(
                CStr::from_ptr(kafka_Map_get(out, topic.as_ptr() as *mut c_void) as *const c_char)
                    .to_str()
                    .unwrap(),
                "t"
            );
            kafka_Map_destroy(out);

            let s = kafka_common_MetricName_to_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_common_MetricName_destroy(handle);

            let bare = kafka_common_MetricName_new(name.as_ptr(), group.as_ptr(), description.as_ptr(), ptr::null());
            assert!(metric_name_ref(bare).tags().is_empty());
            kafka_common_MetricName_destroy(bare);
            kafka_common_MetricName_destroy(ptr::null_mut());
        }
    }
}

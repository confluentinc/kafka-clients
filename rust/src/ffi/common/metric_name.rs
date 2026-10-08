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
use std::ffi::{CString, c_char};

use crate::common::MetricName;
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
/// `name` must be a valid metric-name handle.
pub(crate) unsafe fn metric_name_ref<'a>(name: *const kafka_common_MetricName_t) -> &'a MetricName {
    unsafe { &*(name as *const MetricNameInner) }.metric_name()
}

unsafe fn inner<'a>(self_: *const kafka_common_MetricName_t) -> &'a MetricNameInner {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_new(
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    tags: *const kafka_Map_t,
) -> *mut kafka_common_MetricName_t {
    let tags: BTreeMap<String, String> = unsafe { map_strings(tags) }.into_iter().collect();
    box_metric_name(MetricName::new(
        unsafe { c_str_to_string(name) },
        unsafe { c_str_to_string(group) },
        unsafe { c_str_to_string(description) },
        tags,
    ))
}

/// `name()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_name(self_: *const kafka_common_MetricName_t) -> *const c_char {
    unsafe { inner(self_) }.name_c.as_ptr()
}

/// `group()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_group(self_: *const kafka_common_MetricName_t) -> *const c_char {
    unsafe { inner(self_) }.group_c.as_ptr()
}

/// `description()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_description(self_: *const kafka_common_MetricName_t) -> *const c_char {
    unsafe { inner(self_) }.description_c.as_ptr()
}

/// `tags()`: an owned map of `char *` to `char *` sorted by key, freed with
/// `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_tags(self_: *const kafka_common_MetricName_t) -> *mut kafka_Map_t {
    box_string_map(unsafe { metric_name_ref(self_) }.tags())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-name handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_to_string(self_: *const kafka_common_MetricName_t) -> *mut c_char {
    into_c_string(&unsafe { metric_name_ref(self_) }.to_string())
}

/// Frees an owned metric-name handle; null is a no-op. A handle borrowed
/// from a metric or an error payload is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned metric-name handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricName_destroy(self_: *mut kafka_common_MetricName_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut MetricNameInner) });
    }
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

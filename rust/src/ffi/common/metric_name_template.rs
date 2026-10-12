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

//! `kafka_common_MetricNameTemplate_t`:
//! `org.apache.kafka.common.MetricNameTemplate` (CLAUDE.md §4).
//!
//! The handle owns a [`MetricNameTemplate`] plus the NUL-terminated copies of
//! its three strings the borrowed getters hand out.

use std::ffi::{CString, c_char};

use indexmap::IndexSet;

use crate::common::MetricNameTemplate;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_string_list, c_str_to_string, into_c_string, kafka_List_t, list_strings, owned_c_string};

/// Opaque handle to a [`MetricNameTemplate`].
#[repr(C)]
pub struct kafka_common_MetricNameTemplate_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_MetricNameTemplate_t`] points at: the value plus
/// the NUL-terminated strings its getters borrow out.
struct MetricNameTemplateInner {
    template: MetricNameTemplate,
    name_c: CString,
    group_c: CString,
    description_c: CString,
}

impl MetricNameTemplateInner {
    fn new(template: MetricNameTemplate) -> Self {
        let name_c = owned_c_string(template.name());
        let group_c = owned_c_string(template.group());
        let description_c = owned_c_string(template.description());
        Self { template, name_c, group_c, description_c }
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `self_` must be a valid metric-name-template handle, alive for `'a`.
unsafe fn inner<'a>(self_: *const kafka_common_MetricNameTemplate_t) -> &'a MetricNameTemplateInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name-template handle alive
    // for `'a`, so it points at a live `MetricNameTemplateInner`, the `Box::into_raw` of
    // `kafka_common_MetricNameTemplate_new`, the only function that makes one. No call mutates a
    // `MetricNameTemplateInner`, so the shared reference is sound for `'a`.
    unsafe { &*(self_ as *const MetricNameTemplateInner) }
}

/// The template behind a handle.
///
/// # Safety
///
/// `template` must be a valid metric-name-template handle, alive for `'a`.
pub(crate) unsafe fn metric_name_template_ref<'a>(
    template: *const kafka_common_MetricNameTemplate_t,
) -> &'a MetricNameTemplate {
    // SAFETY: Per this function's `# Safety`, `template` is a valid metric-name-template handle
    // alive for `'a`, as `inner` requires.
    &unsafe { inner(template) }.template
}

/// `new MetricNameTemplate(String name, String group, String description,
/// Set<String> tagsNames)`: `tag_names` holds `const char *` in the order
/// the template keeps; everything is copied. Owned, freed with
/// [`kafka_common_MetricNameTemplate_destroy`].
///
/// # Safety
///
/// The strings must be valid NUL-terminated strings and `tag_names` null or
/// a valid list of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_new(
    name: *const c_char,
    group: *const c_char,
    description: *const c_char,
    tag_names: *const kafka_List_t,
) -> *mut kafka_common_MetricNameTemplate_t {
    // SAFETY: Per this function's `# Safety`, `tag_names` is null or a valid list of NUL-terminated
    // strings, as `list_strings` requires; the strings are copied into owned `String`s, so nothing
    // borrows the list after this call.
    let tag_names: IndexSet<String> = unsafe { list_strings(tag_names) }.into_iter().collect();
    Box::into_raw(Box::new(MetricNameTemplateInner::new(MetricNameTemplate::new(
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
        tag_names,
    )))) as *mut kafka_common_MetricNameTemplate_t
}

/// `name()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name-template handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_name(
    self_: *const kafka_common_MetricNameTemplate_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name-template handle, as
    // `inner` requires. The string returned is the inner's own `name_c`, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle, valid as long as it is".
    unsafe { inner(self_) }.name_c.as_ptr()
}

/// `group()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name-template handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_group(
    self_: *const kafka_common_MetricNameTemplate_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name-template handle, as
    // `inner` requires. The string returned is the inner's own `group_c`, so it lives as long as
    // the handle; that is the rustdoc's "borrowed from the handle, valid as long as it is".
    unsafe { inner(self_) }.group_c.as_ptr()
}

/// `description()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid metric-name-template handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_description(
    self_: *const kafka_common_MetricNameTemplate_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name-template handle, as
    // `inner` requires. The string returned is the inner's own `description_c`, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle, valid as long as it is".
    unsafe { inner(self_) }.description_c.as_ptr()
}

/// `tags()`: the tag names in the template's order, as an owned list of
/// `char *` freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-name-template handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_tags(
    self_: *const kafka_common_MetricNameTemplate_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name-template handle, as
    // `metric_name_template_ref` requires; the reference is used only within this call; the tag
    // names are copied into the owned list returned.
    box_string_list(unsafe { metric_name_template_ref(self_) }.tags())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid metric-name-template handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_to_string(
    self_: *const kafka_common_MetricNameTemplate_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid metric-name-template handle, as
    // `metric_name_template_ref` requires; the reference is used only within this call, and the
    // text is copied into an owned string.
    into_c_string(&unsafe { metric_name_template_ref(self_) }.to_string())
}

/// Frees an owned metric-name-template handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_MetricNameTemplate_destroy(self_: *mut kafka_common_MetricNameTemplate_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // metric-name-template handle not yet destroyed, i.e. the `Box::into_raw` of
        // `kafka_common_MetricNameTemplate_new`, so `Box::from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut MetricNameTemplateInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, c_void};
    use std::ptr;

    use super::*;
    use crate::ffi::util::{
        kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size, kafka_string_destroy,
    };

    #[test]
    fn handle_round_trips_strings_tag_order_and_display() {
        let name = CString::new("records-sent").unwrap();
        let group = CString::new("producer-metrics").unwrap();
        let description = CString::new("Records sent").unwrap();
        let topic = CString::new("topic").unwrap();
        let client_id = CString::new("client-id").unwrap();
        // SAFETY: Test code: the `CString`s outlive every call reading them; `tag_names` comes from
        // `kafka_List_new`, borrows the strings, and is destroyed once, after the constructor
        // copies it; each template handle comes from `kafka_common_MetricNameTemplate_new` and is
        // destroyed once, after the strings it lends, the owned tag list and the string from
        // `to_string`, each destroyed once, are read.
        unsafe {
            let tag_names = kafka_List_new();
            kafka_List_add(tag_names, topic.as_ptr() as *mut c_void);
            kafka_List_add(tag_names, client_id.as_ptr() as *mut c_void);
            let handle =
                kafka_common_MetricNameTemplate_new(name.as_ptr(), group.as_ptr(), description.as_ptr(), tag_names);
            kafka_List_destroy(tag_names);

            let expected = MetricNameTemplate::new(
                "records-sent",
                "producer-metrics",
                "Records sent",
                ["topic".to_string(), "client-id".to_string()].into_iter().collect(),
            );
            assert_eq!(metric_name_template_ref(handle), &expected);
            assert_eq!(
                CStr::from_ptr(kafka_common_MetricNameTemplate_name(handle)).to_str().unwrap(),
                "records-sent"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_MetricNameTemplate_group(handle)).to_str().unwrap(),
                "producer-metrics"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_MetricNameTemplate_description(handle))
                    .to_str()
                    .unwrap(),
                "Records sent"
            );

            // Insertion order is preserved, as in Java's `LinkedHashSet`.
            let tags = kafka_common_MetricNameTemplate_tags(handle);
            assert_eq!(kafka_List_size(tags), 2);
            assert_eq!(
                CStr::from_ptr(kafka_List_get(tags, 0) as *const c_char).to_str().unwrap(),
                "topic"
            );
            assert_eq!(
                CStr::from_ptr(kafka_List_get(tags, 1) as *const c_char).to_str().unwrap(),
                "client-id"
            );
            kafka_List_destroy(tags);

            let s = kafka_common_MetricNameTemplate_to_string(handle);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_common_MetricNameTemplate_destroy(handle);

            let bare =
                kafka_common_MetricNameTemplate_new(name.as_ptr(), group.as_ptr(), description.as_ptr(), ptr::null());
            assert!(metric_name_template_ref(bare).tags().is_empty());
            kafka_common_MetricNameTemplate_destroy(bare);
            kafka_common_MetricNameTemplate_destroy(ptr::null_mut());
        }
    }
}

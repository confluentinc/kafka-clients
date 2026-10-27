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

//! `kafka_common_resource_ResourcePattern_t`:
//! `org.apache.kafka.common.resource.ResourcePattern` (CLAUDE.md §4).

use std::ffi::{CString, c_char};

use crate::common::resource::ResourcePattern;
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::resource::pattern_type::{
    kafka_common_resource_PatternType_t, singleton as pattern_type_singleton, value_of as pattern_type_of,
};
use crate::ffi::common::resource::resource_pattern_filter::{
    box_resource_pattern_filter, kafka_common_resource_ResourcePatternFilter_t,
};
use crate::ffi::common::resource::resource_type::{
    kafka_common_resource_ResourceType_t, singleton as resource_type_singleton, value_of as resource_type_of,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`ResourcePattern`].
#[repr(C)]
pub struct kafka_common_resource_ResourcePattern_t {
    _private: [u8; 0],
}

/// What a `kafka_common_resource_ResourcePattern_t` points at: the pattern
/// plus the NUL-terminated name its getter borrows out.
pub(crate) struct ResourcePatternInner {
    pattern: ResourcePattern,
    name_c: CString,
}

impl ResourcePatternInner {
    pub(crate) fn new(pattern: ResourcePattern) -> Self {
        let name_c = owned_c_string(pattern.name());
        Self { pattern, name_c }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_resource_ResourcePattern_t {
        self as *const Self as *const kafka_common_resource_ResourcePattern_t
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `pattern` must be a valid resource-pattern handle, alive for `'a`.
unsafe fn inner_ref<'a>(pattern: *const kafka_common_resource_ResourcePattern_t) -> &'a ResourcePatternInner {
    // SAFETY: Per this function's `# Safety`, `pattern` is a valid resource-pattern handle alive
    // for `'a`, so it points at a live `ResourcePatternInner`: the `Box::into_raw` of
    // `box_resource_pattern`, or one a parent handle embeds and hands out a view on, which lives as
    // long as that parent. Nothing mutates a `ResourcePatternInner` while a handle to it exists, so
    // the shared reference is sound for `'a`.
    unsafe { &*(pattern as *const ResourcePatternInner) }
}

/// The pattern behind a handle.
///
/// # Safety
///
/// `pattern` must be a valid resource-pattern handle, alive for `'a`.
pub(crate) unsafe fn resource_pattern_ref<'a>(
    pattern: *const kafka_common_resource_ResourcePattern_t,
) -> &'a ResourcePattern {
    // SAFETY: Per this function's `# Safety`, `pattern` is a valid resource-pattern handle, as
    // `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(pattern) }.pattern
}

/// Hands `pattern` to C as an owned handle, freed with
/// `kafka_common_resource_ResourcePattern_destroy`.
pub(crate) fn box_resource_pattern(pattern: ResourcePattern) -> *mut kafka_common_resource_ResourcePattern_t {
    Box::into_raw(Box::new(ResourcePatternInner::new(pattern))) as *mut kafka_common_resource_ResourcePattern_t
}

/// Create a pattern using the supplied parameters.
///
/// # Parameters
///
/// - `resource_type` - non-`NULL`, specific, resource type
/// - `name` - non-`NULL` resource name, which can be the `WILDCARD_RESOURCE` (`*`).
/// - `pattern_type` - non-`NULL`, specific, resource pattern type, which controls how the pattern
///   will match resource names.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if `resource_type` is `ANY`, or
///   `pattern_type` is `ANY` or `MATCH`.
/// - `kafka_common_Error_is_local_illegal_state_error` for a Rust panic caught at the FFI boundary.
///
/// # Safety
///
/// `resource_type` and `pattern_type` must be singletons of their enums, `name`
/// a valid NUL-terminated string and `out_err` a valid slot pointing at `NULL`.
/// The returned pattern is owned, freed with `kafka_common_resource_ResourcePattern_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_new(
    resource_type: *const kafka_common_resource_ResourceType_t,
    name: *const c_char,
    pattern_type: *const kafka_common_resource_PatternType_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_common_resource_ResourcePattern_t {
    let pattern = ResourcePattern::new(
        // SAFETY: Per this function's `# Safety`, `resource_type` is a singleton of its enum, i.e.
        // one returned by the `resource_type` module, as `resource_type_of`
        // (`resource_type::value_of`) requires; the value is copied out.
        unsafe { resource_type_of(resource_type) },
        // SAFETY: Per this function's `# Safety`, `name` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(name) },
        // SAFETY: Per this function's `# Safety`, `pattern_type` is a singleton of its enum, i.e.
        // one returned by the `pattern_type` module, as `pattern_type_of`
        // (`pattern_type::value_of`) requires; the value is copied out.
        unsafe { pattern_type_of(pattern_type) },
    );
    match pattern {
        Ok(pattern) => box_resource_pattern(pattern),
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// `resourceType()`.
///
/// # Returns
///
/// the specific resource type this pattern matches
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
/// The returned singleton is never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_resource_type(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *const kafka_common_resource_ResourceType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern handle, as
    // `resource_pattern_ref` requires; the value is copied out and mapped to its static singleton,
    // so the borrow lasts for this call only.
    resource_type_singleton(unsafe { resource_pattern_ref(self_) }.resource_type())
}

/// `name()`.
///
/// # Returns
///
/// the resource name.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
/// The returned string is borrowed, valid as long as the handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_name(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern handle, as
    // `inner_ref` requires; the string returned is the handle's own C string, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `patternType()`.
///
/// # Returns
///
/// the resource pattern type.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
/// The returned singleton is never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_pattern_type(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *const kafka_common_resource_PatternType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern handle, as
    // `resource_pattern_ref` requires; the value is copied out and mapped to its static singleton,
    // so the borrow lasts for this call only.
    pattern_type_singleton(unsafe { resource_pattern_ref(self_) }.pattern_type())
}

/// `toFilter()`.
///
/// # Returns
///
/// a filter which matches only this pattern.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
/// The returned filter is owned, freed with `kafka_common_resource_ResourcePatternFilter_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_to_filter(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *mut kafka_common_resource_ResourcePatternFilter_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern handle, as
    // `resource_pattern_ref` requires; the result is copied into an owned value before return, so
    // the borrow lasts for this call only.
    box_resource_pattern_filter(unsafe { resource_pattern_ref(self_) }.to_filter())
}

/// `isUnknown()`.
///
/// # Returns
///
/// 1 if this Resource has any UNKNOWN components, else 0.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_is_unknown(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern handle, as
    // `resource_pattern_ref` requires; the reference is used only within this call.
    i8::from(unsafe { resource_pattern_ref(self_) }.is_unknown())
}

/// Returns the text form of the pattern.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_to_string(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern handle, as
    // `resource_pattern_ref` requires; the result is copied into an owned value before return, so
    // the borrow lasts for this call only.
    into_c_string(&unsafe { resource_pattern_ref(self_) }.to_string())
}

/// Frees an owned pattern handle. Null is a no-op; the pattern borrowed
/// from a `kafka_common_acl_AclBinding_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned pattern handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_destroy(
    self_: *mut kafka_common_resource_ResourcePattern_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `ResourcePatternInner` in
        // `box_resource_pattern` (a view, which points into its parent, is never passed here), so
        // `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut ResourcePatternInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::resource::{PatternType, ResourceType};
    use crate::ffi::common::resource::pattern_type::{
        kafka_common_resource_PatternType_any, kafka_common_resource_PatternType_literal,
        kafka_common_resource_PatternType_prefixed,
    };
    use crate::ffi::common::resource::resource_pattern_filter::{
        kafka_common_resource_ResourcePatternFilter_destroy, kafka_common_resource_ResourcePatternFilter_matches,
        resource_pattern_filter_ref,
    };
    use crate::ffi::common::resource::resource_type::{
        kafka_common_resource_ResourceType_any, kafka_common_resource_ResourceType_topic,
    };
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_argument_error;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructor_getters_and_filter_follow_java() {
        let name = CString::new("orders-").unwrap();
        // SAFETY: Test code: the enum arguments are singletons from their getters, `name` is a
        // `CString` that outlives the constructor copying it, `err` is a valid slot pointing at
        // null, the name view is read only while `pattern` is alive, and every owned handle (the
        // pattern the constructor returns, the filter, the string) is destroyed once.
        unsafe {
            let mut err = ptr::null_mut();
            let pattern = kafka_common_resource_ResourcePattern_new(
                kafka_common_resource_ResourceType_topic(),
                name.as_ptr(),
                kafka_common_resource_PatternType_prefixed(),
                &mut err,
            );
            assert!(err.is_null());
            let expected = ResourcePattern::new(ResourceType::Topic, "orders-", PatternType::Prefixed).unwrap();
            assert_eq!(*resource_pattern_ref(pattern), expected);
            assert_eq!(
                kafka_common_resource_ResourcePattern_resource_type(pattern),
                kafka_common_resource_ResourceType_topic()
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_resource_ResourcePattern_name(pattern))
                    .to_str()
                    .unwrap(),
                "orders-"
            );
            assert_eq!(
                kafka_common_resource_ResourcePattern_pattern_type(pattern),
                kafka_common_resource_PatternType_prefixed()
            );
            assert_eq!(kafka_common_resource_ResourcePattern_is_unknown(pattern), 0);
            let s = kafka_common_resource_ResourcePattern_to_string(pattern);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);

            let filter = kafka_common_resource_ResourcePattern_to_filter(pattern);
            assert_eq!(*resource_pattern_filter_ref(filter), expected.to_filter());
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches(filter, pattern), 1);
            kafka_common_resource_ResourcePatternFilter_destroy(filter);
            kafka_common_resource_ResourcePattern_destroy(pattern);
            kafka_common_resource_ResourcePattern_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn any_resource_type_and_filter_pattern_types_are_rejected() {
        let name = CString::new("orders").unwrap();
        // SAFETY: Test code: the enum arguments are singletons from their getters, `name` is a
        // `CString` that outlives the calls reading it, `error` is a valid slot pointing at null
        // before each call, and each error the failing constructor stores is destroyed once,
        // after its message is read, and the slot reset.
        unsafe {
            let mut error = ptr::null_mut();
            let pattern = kafka_common_resource_ResourcePattern_new(
                kafka_common_resource_ResourceType_any(),
                name.as_ptr(),
                kafka_common_resource_PatternType_literal(),
                &mut error,
            );
            assert!(pattern.is_null(), "nothing is returned on failure");
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "resourceType must not be ANY"
            );
            kafka_common_Error_destroy(error);
            error = ptr::null_mut();
            let pattern = kafka_common_resource_ResourcePattern_new(
                kafka_common_resource_ResourceType_topic(),
                name.as_ptr(),
                kafka_common_resource_PatternType_any(),
                &mut error,
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "patternType must not be ANY"
            );
            kafka_common_Error_destroy(error);
            assert!(pattern.is_null(), "nothing is returned on failure");
        }
    }
}

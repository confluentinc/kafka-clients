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
use std::ptr;

use crate::common::resource::ResourcePattern;
use crate::ffi::common::resource::pattern_type::{
    kafka_common_resource_PatternType_t, singleton as pattern_type_singleton, value_of as pattern_type_of,
};
use crate::ffi::common::resource::resource_pattern_filter::{
    box_resource_pattern_filter, kafka_common_resource_ResourcePatternFilter_t,
};
use crate::ffi::common::resource::resource_type::{
    kafka_common_resource_ResourceType_t, singleton as resource_type_singleton, value_of as resource_type_of,
};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`ResourcePattern`].
#[repr(C)]
pub struct kafka_common_resource_ResourcePattern_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_resource_ResourcePattern_t`] points at: the pattern
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

unsafe fn inner_ref<'a>(pattern: *const kafka_common_resource_ResourcePattern_t) -> &'a ResourcePatternInner {
    unsafe { &*(pattern as *const ResourcePatternInner) }
}

/// The pattern behind a handle.
///
/// # Safety
///
/// `pattern` must be a valid resource-pattern handle.
pub(crate) unsafe fn resource_pattern_ref<'a>(
    pattern: *const kafka_common_resource_ResourcePattern_t,
) -> &'a ResourcePattern {
    &unsafe { inner_ref(pattern) }.pattern
}

/// Hands `pattern` to C as an owned handle, freed with
/// [`kafka_common_resource_ResourcePattern_destroy`].
pub(crate) fn box_resource_pattern(pattern: ResourcePattern) -> *mut kafka_common_resource_ResourcePattern_t {
    Box::into_raw(Box::new(ResourcePatternInner::new(pattern))) as *mut kafka_common_resource_ResourcePattern_t
}

/// `new ResourcePattern(ResourceType resourceType, String name, PatternType patternType)`:
/// delivers the owned pattern through `out_pattern`, or returns the owned
/// `IllegalArgumentException` translation when `resource_type` is `ANY` or
/// `pattern_type` is `ANY` or `MATCH`.
///
/// # Safety
///
/// `resource_type` and `pattern_type` must be singletons of their enums,
/// `name` a valid NUL-terminated string and `out_pattern` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_new(
    resource_type: *const kafka_common_resource_ResourceType_t,
    name: *const c_char,
    pattern_type: *const kafka_common_resource_PatternType_t,
    out_pattern: *mut *mut kafka_common_resource_ResourcePattern_t,
) -> *mut kafka_common_Error_t {
    let pattern = ResourcePattern::new(
        unsafe { resource_type_of(resource_type) },
        unsafe { c_str_to_string(name) },
        unsafe { pattern_type_of(pattern_type) },
    );
    match pattern {
        Ok(pattern) => {
            unsafe { *out_pattern = box_resource_pattern(pattern) };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// `resourceType()`: the borrowed `ResourceType` singleton.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_resource_type(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *const kafka_common_resource_ResourceType_t {
    resource_type_singleton(unsafe { resource_pattern_ref(self_) }.resource_type())
}

/// `name()`: borrowed from the handle; `*` is the wildcard resource.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_name(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `patternType()`: the borrowed `PatternType` singleton.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_pattern_type(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *const kafka_common_resource_PatternType_t {
    pattern_type_singleton(unsafe { resource_pattern_ref(self_) }.pattern_type())
}

/// `toFilter()`: an owned filter matching exactly this pattern, freed with
/// `kafka_common_resource_ResourcePatternFilter_destroy`.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_to_filter(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *mut kafka_common_resource_ResourcePatternFilter_t {
    box_resource_pattern_filter(unsafe { resource_pattern_ref(self_) }.to_filter())
}

/// `isUnknown()`: whether the resource type or the pattern type is
/// `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_is_unknown(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> i8 {
    i8::from(unsafe { resource_pattern_ref(self_) }.is_unknown())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_to_string(
    self_: *const kafka_common_resource_ResourcePattern_t,
) -> *mut c_char {
    into_c_string(&unsafe { resource_pattern_ref(self_) }.to_string())
}

/// Frees an owned pattern handle. Null is a no-op; the pattern borrowed
/// from a `kafka_common_acl_AclBinding_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned pattern handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePattern_destroy(
    self_: *mut kafka_common_resource_ResourcePattern_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ResourcePatternInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

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
        unsafe {
            let mut pattern = ptr::null_mut();
            assert!(
                kafka_common_resource_ResourcePattern_new(
                    kafka_common_resource_ResourceType_topic(),
                    name.as_ptr(),
                    kafka_common_resource_PatternType_prefixed(),
                    &mut pattern
                )
                .is_null()
            );
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
        unsafe {
            let mut pattern = ptr::null_mut();
            let error = kafka_common_resource_ResourcePattern_new(
                kafka_common_resource_ResourceType_any(),
                name.as_ptr(),
                kafka_common_resource_PatternType_literal(),
                &mut pattern,
            );
            assert_eq!(kafka_common_Error_is_local_illegal_argument_error(error), 1);
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "resourceType must not be ANY"
            );
            kafka_common_Error_destroy(error);
            let error = kafka_common_resource_ResourcePattern_new(
                kafka_common_resource_ResourceType_topic(),
                name.as_ptr(),
                kafka_common_resource_PatternType_any(),
                &mut pattern,
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "patternType must not be ANY"
            );
            kafka_common_Error_destroy(error);
            assert!(pattern.is_null(), "nothing is delivered on failure");
        }
    }
}

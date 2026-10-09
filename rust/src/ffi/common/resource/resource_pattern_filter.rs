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

//! `kafka_common_resource_ResourcePatternFilter_t`:
//! `org.apache.kafka.common.resource.ResourcePatternFilter` (CLAUDE.md §4). A
//! null name is Java's null: "matches any name".

use std::ffi::{CString, c_char};
use std::ptr;

use crate::common::resource::ResourcePatternFilter;
use crate::ffi::common::resource::pattern_type::{
    kafka_common_resource_PatternType_t, singleton as pattern_type_singleton, value_of as pattern_type_of,
};
use crate::ffi::common::resource::resource_pattern::{kafka_common_resource_ResourcePattern_t, resource_pattern_ref};
use crate::ffi::common::resource::resource_type::{
    kafka_common_resource_ResourceType_t, singleton as resource_type_singleton, value_of as resource_type_of,
};
use crate::ffi::util::{c_str_to_option, into_c_string, owned_c_string};

/// Opaque handle to a [`ResourcePatternFilter`].
#[repr(C)]
pub struct kafka_common_resource_ResourcePatternFilter_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_resource_ResourcePatternFilter_t`] points at: the
/// filter plus the NUL-terminated name its getter borrows out (`None` for
/// Java's null).
pub(crate) struct ResourcePatternFilterInner {
    filter: ResourcePatternFilter,
    name_c: Option<CString>,
}

impl ResourcePatternFilterInner {
    pub(crate) fn new(filter: ResourcePatternFilter) -> Self {
        let name_c = filter.name().map(owned_c_string);
        Self { filter, name_c }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_resource_ResourcePatternFilter_t {
        self as *const Self as *const kafka_common_resource_ResourcePatternFilter_t
    }
}

unsafe fn inner_ref<'a>(
    filter: *const kafka_common_resource_ResourcePatternFilter_t,
) -> &'a ResourcePatternFilterInner {
    unsafe { &*(filter as *const ResourcePatternFilterInner) }
}

/// The filter behind a handle.
///
/// # Safety
///
/// `filter` must be a valid resource-pattern-filter handle.
pub(crate) unsafe fn resource_pattern_filter_ref<'a>(
    filter: *const kafka_common_resource_ResourcePatternFilter_t,
) -> &'a ResourcePatternFilter {
    &unsafe { inner_ref(filter) }.filter
}

/// Hands `filter` to C as an owned handle, freed with
/// [`kafka_common_resource_ResourcePatternFilter_destroy`].
pub(crate) fn box_resource_pattern_filter(
    filter: ResourcePatternFilter,
) -> *mut kafka_common_resource_ResourcePatternFilter_t {
    Box::into_raw(Box::new(ResourcePatternFilterInner::new(filter)))
        as *mut kafka_common_resource_ResourcePatternFilter_t
}

/// `new ResourcePatternFilter(ResourceType resourceType, String name, PatternType patternType)`:
/// a null `name` matches any name. Owned, freed with
/// [`kafka_common_resource_ResourcePatternFilter_destroy`].
///
/// # Safety
///
/// `resource_type` and `pattern_type` must be singletons of their enums and
/// `name` null or a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_new(
    resource_type: *const kafka_common_resource_ResourceType_t,
    name: *const c_char,
    pattern_type: *const kafka_common_resource_PatternType_t,
) -> *mut kafka_common_resource_ResourcePatternFilter_t {
    box_resource_pattern_filter(ResourcePatternFilter::new(
        unsafe { resource_type_of(resource_type) },
        unsafe { c_str_to_option(name) },
        unsafe { pattern_type_of(pattern_type) },
    ))
}

/// `ResourcePatternFilter.ANY`: a filter matching every pattern. Owned, as
/// every handle of this type.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourcePatternFilter_any() -> *mut kafka_common_resource_ResourcePatternFilter_t
{
    box_resource_pattern_filter(ResourcePatternFilter::any())
}

/// `isUnknown()`: whether the resource type or the pattern type is
/// `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_is_unknown(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> i8 {
    i8::from(unsafe { resource_pattern_filter_ref(self_) }.is_unknown())
}

/// `resourceType()`: the borrowed `ResourceType` singleton.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_resource_type(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *const kafka_common_resource_ResourceType_t {
    resource_type_singleton(unsafe { resource_pattern_filter_ref(self_) }.resource_type())
}

/// `name()`: borrowed from the handle, or null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_name(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ref().map_or(ptr::null(), |s| s.as_ptr())
}

/// `patternType()`: the borrowed `PatternType` singleton.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_pattern_type(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *const kafka_common_resource_PatternType_t {
    pattern_type_singleton(unsafe { resource_pattern_filter_ref(self_) }.pattern_type())
}

/// `matches(ResourcePattern pattern)`: whether the filter matches
/// `pattern`, with Java's `MATCH` semantics for prefixed and wildcard
/// patterns.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle and `pattern` a
/// valid resource-pattern handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_matches(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
    pattern: *const kafka_common_resource_ResourcePattern_t,
) -> i8 {
    i8::from(unsafe { resource_pattern_filter_ref(self_) }.matches(unsafe { resource_pattern_ref(pattern) }))
}

/// `matchesAtMostOne()`: whether the filter can match at most one pattern.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_matches_at_most_one(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> i8 {
    i8::from(unsafe { resource_pattern_filter_ref(self_) }.matches_at_most_one())
}

/// `findIndefiniteField()`: a description of the first field that keeps
/// the filter from matching at most one pattern, as an owned string freed
/// with `kafka_string_destroy`, or null when there is none.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_find_indefinite_field(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *mut c_char {
    unsafe { resource_pattern_filter_ref(self_) }
        .find_indefinite_field()
        .map_or(ptr::null_mut(), |field| into_c_string(&field))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_to_string(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *mut c_char {
    into_c_string(&unsafe { resource_pattern_filter_ref(self_) }.to_string())
}

/// Frees an owned filter handle. Null is a no-op; the filter borrowed from
/// a `kafka_common_acl_AclBindingFilter_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned filter handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_destroy(
    self_: *mut kafka_common_resource_ResourcePatternFilter_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ResourcePatternFilterInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::resource::{PatternType, ResourcePattern, ResourceType};
    use crate::ffi::common::resource::pattern_type::{
        kafka_common_resource_PatternType_any, kafka_common_resource_PatternType_literal,
        kafka_common_resource_PatternType_match,
    };
    use crate::ffi::common::resource::resource_pattern::{
        box_resource_pattern, kafka_common_resource_ResourcePattern_destroy,
    };
    use crate::ffi::common::resource::resource_type::{
        kafka_common_resource_ResourceType_any, kafka_common_resource_ResourceType_topic,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn null_name_matches_any_and_match_follows_prefixes() {
        let literal =
            box_resource_pattern(ResourcePattern::new(ResourceType::Topic, "orders", PatternType::Literal).unwrap());
        let prefixed =
            box_resource_pattern(ResourcePattern::new(ResourceType::Topic, "ord", PatternType::Prefixed).unwrap());
        let name = CString::new("orders").unwrap();
        unsafe {
            let by_type = kafka_common_resource_ResourcePatternFilter_new(
                kafka_common_resource_ResourceType_topic(),
                ptr::null(),
                kafka_common_resource_PatternType_literal(),
            );
            assert_eq!(
                *resource_pattern_filter_ref(by_type),
                ResourcePatternFilter::new(ResourceType::Topic, None, PatternType::Literal)
            );
            assert!(kafka_common_resource_ResourcePatternFilter_name(by_type).is_null());
            assert_eq!(
                kafka_common_resource_ResourcePatternFilter_resource_type(by_type),
                kafka_common_resource_ResourceType_topic()
            );
            assert_eq!(
                kafka_common_resource_ResourcePatternFilter_pattern_type(by_type),
                kafka_common_resource_PatternType_literal()
            );
            assert_eq!(kafka_common_resource_ResourcePatternFilter_is_unknown(by_type), 0);
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches(by_type, literal), 1);
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches(by_type, prefixed), 0);
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches_at_most_one(by_type), 0);
            let field = kafka_common_resource_ResourcePatternFilter_find_indefinite_field(by_type);
            assert_eq!(
                CStr::from_ptr(field).to_str().unwrap(),
                resource_pattern_filter_ref(by_type).find_indefinite_field().unwrap()
            );
            kafka_string_destroy(field);
            let s = kafka_common_resource_ResourcePatternFilter_to_string(by_type);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                resource_pattern_filter_ref(by_type).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_resource_ResourcePatternFilter_destroy(by_type);

            // MATCH: the literal name and the prefix of it both match.
            let matching = kafka_common_resource_ResourcePatternFilter_new(
                kafka_common_resource_ResourceType_topic(),
                name.as_ptr(),
                kafka_common_resource_PatternType_match(),
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_resource_ResourcePatternFilter_name(matching))
                    .to_str()
                    .unwrap(),
                "orders"
            );
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches(matching, literal), 1);
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches(matching, prefixed), 1);
            kafka_common_resource_ResourcePatternFilter_destroy(matching);

            // Exact: definite.
            let exact = kafka_common_resource_ResourcePatternFilter_new(
                kafka_common_resource_ResourceType_topic(),
                name.as_ptr(),
                kafka_common_resource_PatternType_literal(),
            );
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches_at_most_one(exact), 1);
            assert!(kafka_common_resource_ResourcePatternFilter_find_indefinite_field(exact).is_null());
            kafka_common_resource_ResourcePatternFilter_destroy(exact);

            let any = kafka_common_resource_ResourcePatternFilter_any();
            assert_eq!(*resource_pattern_filter_ref(any), ResourcePatternFilter::any());
            assert_eq!(
                kafka_common_resource_ResourcePatternFilter_resource_type(any),
                kafka_common_resource_ResourceType_any()
            );
            assert_eq!(
                kafka_common_resource_ResourcePatternFilter_pattern_type(any),
                kafka_common_resource_PatternType_any()
            );
            assert_eq!(kafka_common_resource_ResourcePatternFilter_matches(any, prefixed), 1);
            kafka_common_resource_ResourcePatternFilter_destroy(any);
            kafka_common_resource_ResourcePatternFilter_destroy(ptr::null_mut());
            kafka_common_resource_ResourcePattern_destroy(literal);
            kafka_common_resource_ResourcePattern_destroy(prefixed);
        }
    }
}

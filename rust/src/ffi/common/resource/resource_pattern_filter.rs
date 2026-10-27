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
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_option, into_c_string, owned_c_string};

/// Opaque handle to a [`ResourcePatternFilter`].
#[repr(C)]
pub struct kafka_common_resource_ResourcePatternFilter_t {
    _private: [u8; 0],
}

/// What a `kafka_common_resource_ResourcePatternFilter_t` points at: the
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

/// What a handle points at.
///
/// # Safety
///
/// `filter` must be a valid resource-pattern-filter handle, alive for `'a`.
unsafe fn inner_ref<'a>(
    filter: *const kafka_common_resource_ResourcePatternFilter_t,
) -> &'a ResourcePatternFilterInner {
    // SAFETY: Per this function's `# Safety`, `filter` is a valid resource-pattern-filter handle
    // alive for `'a`, so it points at a live `ResourcePatternFilterInner`: the `Box::into_raw` of
    // `box_resource_pattern_filter`, or one a parent handle embeds and hands out a view on, which
    // lives as long as that parent. Nothing mutates a `ResourcePatternFilterInner` while a handle
    // to it exists, so the shared reference is sound for `'a`.
    unsafe { &*(filter as *const ResourcePatternFilterInner) }
}

/// The filter behind a handle.
///
/// # Safety
///
/// `filter` must be a valid resource-pattern-filter handle, alive for `'a`.
pub(crate) unsafe fn resource_pattern_filter_ref<'a>(
    filter: *const kafka_common_resource_ResourcePatternFilter_t,
) -> &'a ResourcePatternFilter {
    // SAFETY: Per this function's `# Safety`, `filter` is a valid resource-pattern-filter handle,
    // as `inner_ref` requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(filter) }.filter
}

/// Hands `filter` to C as an owned handle, freed with
/// `kafka_common_resource_ResourcePatternFilter_destroy`.
pub(crate) fn box_resource_pattern_filter(
    filter: ResourcePatternFilter,
) -> *mut kafka_common_resource_ResourcePatternFilter_t {
    Box::into_raw(Box::new(ResourcePatternFilterInner::new(filter)))
        as *mut kafka_common_resource_ResourcePatternFilter_t
}

/// Create a filter using the supplied parameters.
///
/// # Parameters
///
/// - `resource_type` - non-`NULL` resource type. If `ANY`, the filter will ignore the resource type
///   of the pattern. If any other resource type, the filter will match only patterns with the same
///   type.
/// - `name` - resource name or `NULL`. If `NULL`, the filter will ignore the name of resources. If
///   `WILDCARD_RESOURCE` (`*`), will match only wildcard patterns.
/// - `pattern_type` - non-`NULL` resource pattern type. If `ANY`, the filter will match patterns
///   regardless of pattern type. If `MATCH`, the filter will match patterns that would match the
///   supplied `name`, including a matching prefixed and wildcards patterns. If any other resource
///   pattern type, the filter will match only patterns with the same type.
///
/// # Safety
///
/// `resource_type` and `pattern_type` must be singletons of their enums and
/// `name` `NULL` or a valid NUL-terminated string, copied. The returned filter is
/// owned, freed with `kafka_common_resource_ResourcePatternFilter_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_new(
    resource_type: *const kafka_common_resource_ResourceType_t,
    name: *const c_char,
    pattern_type: *const kafka_common_resource_PatternType_t,
) -> *mut kafka_common_resource_ResourcePatternFilter_t {
    box_resource_pattern_filter(ResourcePatternFilter::new(
        // SAFETY: Per this function's `# Safety`, `resource_type` is a singleton of its enum, i.e.
        // one returned by the `resource_type` module, as `resource_type_of`
        // (`resource_type::value_of`) requires; the value is copied out.
        unsafe { resource_type_of(resource_type) },
        // SAFETY: Per this function's `# Safety`, `name` is null or a valid NUL-terminated string,
        // as `c_str_to_option` requires; it is copied into an owned value, so nothing borrows it
        // after this call.
        unsafe { c_str_to_option(name) },
        // SAFETY: Per this function's `# Safety`, `pattern_type` is a singleton of its enum, i.e.
        // one returned by the `pattern_type` module, as `pattern_type_of`
        // (`pattern_type::value_of`) requires; the value is copied out.
        unsafe { pattern_type_of(pattern_type) },
    ))
}

/// Matches any resource pattern.
///
/// # Returns
///
/// The filter.
///
/// # Safety
///
/// The returned filter is owned, freed with
/// `kafka_common_resource_ResourcePatternFilter_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_ResourcePatternFilter_any() -> *mut kafka_common_resource_ResourcePatternFilter_t
{
    box_resource_pattern_filter(ResourcePatternFilter::any())
}

/// `isUnknown()`.
///
/// # Returns
///
/// 1 if this filter has any UNKNOWN components, else 0.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_is_unknown(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `resource_pattern_filter_ref` requires; the reference is used only within this call.
    i8::from(unsafe { resource_pattern_filter_ref(self_) }.is_unknown())
}

/// `resourceType()`.
///
/// # Returns
///
/// the specific resource type this pattern matches
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
/// The returned singleton is never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_resource_type(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *const kafka_common_resource_ResourceType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `resource_pattern_filter_ref` requires; the value is copied out and mapped to its static
    // singleton, so the borrow lasts for this call only.
    resource_type_singleton(unsafe { resource_pattern_filter_ref(self_) }.resource_type())
}

/// `name()`.
///
/// # Returns
///
/// the resource name.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
/// The returned string is `NULL` (Java's `null`) or borrowed, valid as long as the handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_name(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `inner_ref` requires; the string returned, when there is one, is the handle's own C string,
    // so it lives as long as the handle; that is the rustdoc's "borrowed from the handle, or null".
    unsafe { inner_ref(self_) }.name_c.as_ref().map_or(ptr::null(), |s| s.as_ptr())
}

/// `patternType()`.
///
/// # Returns
///
/// the resource pattern type.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
/// The returned singleton is never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_pattern_type(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *const kafka_common_resource_PatternType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `resource_pattern_filter_ref` requires; the value is copied out and mapped to its static
    // singleton, so the borrow lasts for this call only.
    pattern_type_singleton(unsafe { resource_pattern_filter_ref(self_) }.pattern_type())
}

/// `matches(ResourcePattern)`.
///
/// # Parameters
///
/// - `pattern` - The pattern to match.
///
/// # Returns
///
/// 1 if this filter matches the given pattern, else 0.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle. `pattern` must be a valid
/// resource-pattern handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_matches(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
    pattern: *const kafka_common_resource_ResourcePattern_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle and
    // `pattern` a valid resource-pattern handle, as `resource_pattern_filter_ref` and
    // `resource_pattern_ref` require. Both borrows last for this call only.
    i8::from(unsafe { resource_pattern_filter_ref(self_) }.matches(unsafe { resource_pattern_ref(pattern) }))
}

/// `matchesAtMostOne()`.
///
/// # Returns
///
/// 1 if this filter could only match one pattern, else 0. In other words, if there are no ANY or
/// UNKNOWN fields.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_matches_at_most_one(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `resource_pattern_filter_ref` requires; the reference is used only within this call.
    i8::from(unsafe { resource_pattern_filter_ref(self_) }.matches_at_most_one())
}

/// `findIndefiniteField()`.
///
/// # Returns
///
/// a string describing any ANY or UNKNOWN field, or `NULL` if there is no such field.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_find_indefinite_field(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `resource_pattern_filter_ref` requires; the result is copied into an owned value before
    // return, so the borrow lasts for this call only.
    unsafe { resource_pattern_filter_ref(self_) }
        .find_indefinite_field()
        .map_or(ptr::null_mut(), |field| into_c_string(&field))
}

/// Returns the text form of the filter.
///
/// # Safety
///
/// `self_` must be a valid resource-pattern-filter handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_to_string(
    self_: *const kafka_common_resource_ResourcePatternFilter_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource-pattern-filter handle, as
    // `resource_pattern_filter_ref` requires; the result is copied into an owned value before
    // return, so the borrow lasts for this call only.
    into_c_string(&unsafe { resource_pattern_filter_ref(self_) }.to_string())
}

/// Frees an owned filter handle. Null is a no-op; the filter borrowed from
/// a `kafka_common_acl_AclBindingFilter_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned filter handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_ResourcePatternFilter_destroy(
    self_: *mut kafka_common_resource_ResourcePatternFilter_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `ResourcePatternFilterInner` in
        // `box_resource_pattern_filter` (a view, which points into its parent, is never passed
        // here), so `from_raw` reclaims that allocation, once.
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
        // SAFETY: Test code: `literal` and `prefixed` come from `box_resource_pattern`, the filters
        // from this module's constructors, the enum arguments are singletons from their getters,
        // `name` is a `CString` that outlives the constructors copying it, the name views are read
        // only while their filter is alive, and every owned handle (the filters, the strings, the
        // patterns) is destroyed once.
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

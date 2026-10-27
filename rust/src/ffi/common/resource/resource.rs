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

//! `kafka_common_resource_Resource_t`: `org.apache.kafka.common.resource.Resource`
//! (CLAUDE.md §4).

use std::ffi::{CString, c_char};

use crate::common::resource::Resource;
use crate::ffi::common::resource::resource_type::{
    kafka_common_resource_ResourceType_t, singleton as resource_type_singleton, value_of as resource_type_of,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`Resource`].
#[repr(C)]
pub struct kafka_common_resource_Resource_t {
    _private: [u8; 0],
}

/// What a `kafka_common_resource_Resource_t` points at: the resource plus
/// the NUL-terminated name its getter borrows out.
pub(crate) struct ResourceInner {
    resource: Resource,
    name_c: CString,
}

impl ResourceInner {
    pub(crate) fn new(resource: Resource) -> Self {
        let name_c = owned_c_string(resource.name());
        Self { resource, name_c }
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `resource` must be a valid resource handle, alive for `'a`.
unsafe fn inner_ref<'a>(resource: *const kafka_common_resource_Resource_t) -> &'a ResourceInner {
    // SAFETY: Per this function's `# Safety`, `resource` is a valid resource handle alive for `'a`,
    // so it points at a live `ResourceInner`, the `Box::into_raw` of `box_resource` (no view of
    // this type is handed out). Nothing mutates a `ResourceInner` while a handle to it exists, so
    // the shared reference is sound for `'a`.
    unsafe { &*(resource as *const ResourceInner) }
}

/// The resource behind a handle.
///
/// # Safety
///
/// `resource` must be a valid resource handle, alive for `'a`.
pub(crate) unsafe fn resource_ref<'a>(resource: *const kafka_common_resource_Resource_t) -> &'a Resource {
    // SAFETY: Per this function's `# Safety`, `resource` is a valid resource handle, as `inner_ref`
    // requires; the reference returned lives for the same `'a`.
    &unsafe { inner_ref(resource) }.resource
}

/// Hands `resource` to C as an owned handle, freed with
/// `kafka_common_resource_Resource_destroy`.
pub(crate) fn box_resource(resource: Resource) -> *mut kafka_common_resource_Resource_t {
    Box::into_raw(Box::new(ResourceInner::new(resource))) as *mut kafka_common_resource_Resource_t
}

/// Create an instance of this class with the provided parameters.
///
/// # Parameters
///
/// - `resource_type` - non-`NULL` resource type
/// - `name` - non-`NULL` resource name
///
/// # Safety
///
/// `resource_type` must be a resource-type singleton and `name` a valid
/// NUL-terminated string, copied. The returned resource is owned, freed with
/// `kafka_common_resource_Resource_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_new(
    resource_type: *const kafka_common_resource_ResourceType_t,
    name: *const c_char,
) -> *mut kafka_common_resource_Resource_t {
    // SAFETY: Per this function's `# Safety`, `resource_type` is a resource-type singleton, as
    // `resource_type_of` requires, and its value is copied out; `name` is a valid NUL-terminated
    // string, as `c_str_to_string` requires, and is copied into an owned `String`. Nothing borrows
    // either argument after this call.
    box_resource(Resource::new(unsafe { resource_type_of(resource_type) }, unsafe {
        c_str_to_string(name)
    }))
}

/// A resource representing the whole cluster.
///
/// # Returns
///
/// The resource.
///
/// # Safety
///
/// The returned resource is owned, freed with `kafka_common_resource_Resource_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_Resource_cluster() -> *mut kafka_common_resource_Resource_t {
    box_resource(Resource::cluster())
}

/// Returns the resource type.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
/// The returned singleton is never freed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_resource_type(
    self_: *const kafka_common_resource_Resource_t,
) -> *const kafka_common_resource_ResourceType_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource handle, as `resource_ref`
    // requires; the value is copied out and mapped to its static singleton, so the borrow lasts for
    // this call only.
    resource_type_singleton(unsafe { resource_ref(self_) }.resource_type())
}

/// Returns the resource name.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
/// The returned string is borrowed, valid as long as the handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_name(
    self_: *const kafka_common_resource_Resource_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource handle, as `inner_ref`
    // requires; the string returned is the handle's own C string, so it lives as long as the
    // handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// Returns 1 if this Resource has any UNKNOWN components, else 0.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_is_unknown(
    self_: *const kafka_common_resource_Resource_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource handle, as `resource_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { resource_ref(self_) }.is_unknown())
}

/// Returns the text form of the resource.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_to_string(
    self_: *const kafka_common_resource_Resource_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid resource handle, as `resource_ref`
    // requires; the result is copied into an owned value before return, so the borrow lasts for
    // this call only.
    into_c_string(&unsafe { resource_ref(self_) }.to_string())
}

/// Frees an owned resource handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned resource handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_destroy(self_: *mut kafka_common_resource_Resource_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `ResourceInner` in
        // `box_resource`, so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut ResourceInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::resource::ResourceType;
    use crate::ffi::common::resource::resource_type::{
        kafka_common_resource_ResourceType_cluster, kafka_common_resource_ResourceType_topic,
        kafka_common_resource_ResourceType_unknown,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn constructor_cluster_and_getters_follow_java() {
        let name = CString::new("orders").unwrap();
        // SAFETY: Test code: the resource-type arguments are singletons from their getters, `name`
        // is a `CString` that outlives the constructors copying it, the name views are read only
        // while their resource is alive, and every owned handle (the resources, the string) is
        // destroyed once.
        unsafe {
            let topic = kafka_common_resource_Resource_new(kafka_common_resource_ResourceType_topic(), name.as_ptr());
            assert_eq!(*resource_ref(topic), Resource::new(ResourceType::Topic, "orders"));
            assert_eq!(
                kafka_common_resource_Resource_resource_type(topic),
                kafka_common_resource_ResourceType_topic()
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_resource_Resource_name(topic)).to_str().unwrap(),
                "orders"
            );
            assert_eq!(kafka_common_resource_Resource_is_unknown(topic), 0);
            let s = kafka_common_resource_Resource_to_string(topic);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                Resource::new(ResourceType::Topic, "orders").to_string()
            );
            kafka_string_destroy(s);
            kafka_common_resource_Resource_destroy(topic);

            let cluster = kafka_common_resource_Resource_cluster();
            assert_eq!(*resource_ref(cluster), Resource::cluster());
            assert_eq!(
                kafka_common_resource_Resource_resource_type(cluster),
                kafka_common_resource_ResourceType_cluster()
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_resource_Resource_name(cluster)).to_str().unwrap(),
                Resource::CLUSTER_NAME
            );
            kafka_common_resource_Resource_destroy(cluster);

            let unknown =
                kafka_common_resource_Resource_new(kafka_common_resource_ResourceType_unknown(), name.as_ptr());
            assert_eq!(kafka_common_resource_Resource_is_unknown(unknown), 1);
            kafka_common_resource_Resource_destroy(unknown);
            kafka_common_resource_Resource_destroy(ptr::null_mut());
        }
    }
}

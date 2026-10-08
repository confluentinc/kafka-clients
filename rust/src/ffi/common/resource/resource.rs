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
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`Resource`].
#[repr(C)]
pub struct kafka_common_resource_Resource_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_resource_Resource_t`] points at: the resource plus
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

unsafe fn inner_ref<'a>(resource: *const kafka_common_resource_Resource_t) -> &'a ResourceInner {
    unsafe { &*(resource as *const ResourceInner) }
}

/// The resource behind a handle.
///
/// # Safety
///
/// `resource` must be a valid resource handle.
pub(crate) unsafe fn resource_ref<'a>(resource: *const kafka_common_resource_Resource_t) -> &'a Resource {
    &unsafe { inner_ref(resource) }.resource
}

/// Hands `resource` to C as an owned handle, freed with
/// [`kafka_common_resource_Resource_destroy`].
pub(crate) fn box_resource(resource: Resource) -> *mut kafka_common_resource_Resource_t {
    Box::into_raw(Box::new(ResourceInner::new(resource))) as *mut kafka_common_resource_Resource_t
}

/// `new Resource(ResourceType resourceType, String name)`: the name is
/// copied. Owned, freed with [`kafka_common_resource_Resource_destroy`].
///
/// # Safety
///
/// `resource_type` must be a resource-type singleton and `name` a valid
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_new(
    resource_type: *const kafka_common_resource_ResourceType_t,
    name: *const c_char,
) -> *mut kafka_common_resource_Resource_t {
    box_resource(Resource::new(unsafe { resource_type_of(resource_type) }, unsafe {
        c_str_to_string(name)
    }))
}

/// `Resource.CLUSTER`: the single cluster resource. Owned, as every handle
/// of this type.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_resource_Resource_cluster() -> *mut kafka_common_resource_Resource_t {
    box_resource(Resource::cluster())
}

/// `resourceType()`: the borrowed `ResourceType` singleton.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_resource_type(
    self_: *const kafka_common_resource_Resource_t,
) -> *const kafka_common_resource_ResourceType_t {
    resource_type_singleton(unsafe { resource_ref(self_) }.resource_type())
}

/// `name()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_name(
    self_: *const kafka_common_resource_Resource_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `isUnknown()`: whether the resource type is `UNKNOWN`.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_is_unknown(
    self_: *const kafka_common_resource_Resource_t,
) -> i8 {
    i8::from(unsafe { resource_ref(self_) }.is_unknown())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_to_string(
    self_: *const kafka_common_resource_Resource_t,
) -> *mut c_char {
    into_c_string(&unsafe { resource_ref(self_) }.to_string())
}

/// Frees an owned resource handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned resource handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_resource_Resource_destroy(self_: *mut kafka_common_resource_Resource_t) {
    if !self_.is_null() {
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

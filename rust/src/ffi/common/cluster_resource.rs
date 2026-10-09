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

//! `kafka_common_ClusterResource_t`: `org.apache.kafka.common.ClusterResource`
//! (CLAUDE.md §4).

use std::ffi::{CString, c_char};
use std::ptr;

use crate::common::ClusterResource;
use crate::ffi::util::{c_str_to_option, into_c_string, owned_c_string};

/// Opaque handle to a [`ClusterResource`].
#[repr(C)]
pub struct kafka_common_ClusterResource_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_ClusterResource_t`] points at: the value plus the
/// NUL-terminated cluster id the getter borrows out (`None` for Java's null).
pub(crate) struct ClusterResourceInner {
    resource: ClusterResource,
    cluster_id_c: Option<CString>,
}

impl ClusterResourceInner {
    pub(crate) fn new(resource: ClusterResource) -> Self {
        let cluster_id_c = resource.cluster_id().map(owned_c_string);
        Self { resource, cluster_id_c }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_ClusterResource_t {
        self as *const Self as *const kafka_common_ClusterResource_t
    }
}

unsafe fn inner_ref<'a>(resource: *const kafka_common_ClusterResource_t) -> &'a ClusterResourceInner {
    unsafe { &*(resource as *const ClusterResourceInner) }
}

/// `new ClusterResource(String clusterId)`; a null `cluster_id` is Java's
/// `null` (a broker too old to report one). Owned, freed with
/// [`kafka_common_ClusterResource_destroy`].
///
/// # Safety
///
/// `cluster_id` must be null or a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_new(
    cluster_id: *const c_char,
) -> *mut kafka_common_ClusterResource_t {
    let inner = ClusterResourceInner::new(ClusterResource::new(unsafe { c_str_to_option(cluster_id) }));
    Box::into_raw(Box::new(inner)) as *mut kafka_common_ClusterResource_t
}

/// `clusterId()`: borrowed from the handle, or null when the cluster has no
/// id.
///
/// # Safety
///
/// `self_` must be a valid cluster-resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_cluster_id(
    self_: *const kafka_common_ClusterResource_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }
        .cluster_id_c
        .as_ref()
        .map_or(ptr::null(), |id| id.as_ptr())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid cluster-resource handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_to_string(
    self_: *const kafka_common_ClusterResource_t,
) -> *mut c_char {
    into_c_string(&unsafe { inner_ref(self_) }.resource.to_string())
}

/// Frees an owned cluster-resource handle. Null is a no-op; the resource
/// borrowed from a `kafka_common_Cluster_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned cluster-resource handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_destroy(self_: *mut kafka_common_ClusterResource_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ClusterResourceInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn cluster_id_is_nullable() {
        let id = CString::new("abc").unwrap();
        unsafe {
            let with_id = kafka_common_ClusterResource_new(id.as_ptr());
            assert_eq!(
                CStr::from_ptr(kafka_common_ClusterResource_cluster_id(with_id))
                    .to_str()
                    .unwrap(),
                "abc"
            );
            let s = kafka_common_ClusterResource_to_string(with_id);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ClusterResource::new(Some("abc".into())).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_ClusterResource_destroy(with_id);

            let without = kafka_common_ClusterResource_new(ptr::null());
            assert!(kafka_common_ClusterResource_cluster_id(without).is_null());
            kafka_common_ClusterResource_destroy(without);
            kafka_common_ClusterResource_destroy(ptr::null_mut());
        }
    }
}

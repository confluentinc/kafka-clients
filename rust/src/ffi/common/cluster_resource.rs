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
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_option, into_c_string, owned_c_string};

/// Opaque handle to a [`ClusterResource`].
#[repr(C)]
pub struct kafka_common_ClusterResource_t {
    _private: [u8; 0],
}

/// What a `kafka_common_ClusterResource_t` points at: the value plus the
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

/// What a handle points at.
///
/// # Safety
///
/// `resource` must be a valid cluster-resource handle, alive for `'a`.
unsafe fn inner_ref<'a>(resource: *const kafka_common_ClusterResource_t) -> &'a ClusterResourceInner {
    // SAFETY: Per this function's `# Safety`, `resource` is a valid cluster-resource handle alive
    // for `'a`, so it points at a live `ClusterResourceInner`: the `Box::into_raw` of
    // `kafka_common_ClusterResource_new`, or the one a `kafka_common_Cluster_t` stores and hands
    // out through `ClusterResourceInner::as_ptr`, which lives as long as that cluster. No call
    // mutates a `ClusterResourceInner`, so the shared reference is sound for `'a`.
    unsafe { &*(resource as *const ClusterResourceInner) }
}

/// Create `ClusterResource` with a cluster id. Note that cluster id may be `NULL` if the
/// metadata request was sent to a broker without support for cluster ids.
///
/// # Parameters
///
/// - `cluster_id` - The cluster id
///
/// # Safety
///
/// `cluster_id` must be `NULL` or a valid NUL-terminated string. The returned handle is owned,
/// freed with `kafka_common_ClusterResource_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_new(
    cluster_id: *const c_char,
) -> *mut kafka_common_ClusterResource_t {
    // SAFETY: Per this function's `# Safety`, `cluster_id` is null or a valid NUL-terminated
    // string, as `c_str_to_option` requires; it is copied into an owned value, so nothing borrows
    // it after this call.
    let inner = ClusterResourceInner::new(ClusterResource::new(unsafe { c_str_to_option(cluster_id) }));
    Box::into_raw(Box::new(inner)) as *mut kafka_common_ClusterResource_t
}

/// Return the cluster id. Note that it may be `NULL` if the metadata request was sent to a broker without
/// support for cluster ids.
///
/// # Returns
///
/// The cluster id, or `NULL`.
///
/// # Safety
///
/// `self_` must be a valid cluster-resource handle. The string is borrowed from the handle,
/// valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_cluster_id(
    self_: *const kafka_common_ClusterResource_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster-resource handle, as
    // `inner_ref` requires. The string returned is the inner's own `cluster_id_c`, so it lives as
    // long as the handle; that is the rustdoc's "borrowed from the handle".
    unsafe { inner_ref(self_) }
        .cluster_id_c
        .as_ref()
        .map_or(ptr::null(), |id| id.as_ptr())
}

/// Returns `ClusterResource(clusterId=<id>)`.
///
/// # Safety
///
/// `self_` must be a valid cluster-resource handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_to_string(
    self_: *const kafka_common_ClusterResource_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid cluster-resource handle, as
    // `inner_ref` requires; the reference is used only within this call, and the text is copied
    // into an owned string.
    into_c_string(&unsafe { inner_ref(self_) }.resource.to_string())
}

/// Frees an owned cluster-resource handle. Null is a no-op; the resource
/// borrowed from a `kafka_common_Cluster_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned cluster-resource handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ClusterResource_destroy(self_: *mut kafka_common_ClusterResource_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // cluster-resource handle not yet destroyed, i.e. the `Box::into_raw` of
        // `kafka_common_ClusterResource_new`, so `Box::from_raw` reclaims it exactly once. The
        // resource borrowed from a cluster is never passed here, as the rustdoc states.
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
        // SAFETY: Test code: `id` outlives the constructor, which copies it; each resource comes
        // from `kafka_common_ClusterResource_new` and is destroyed once, after the id it lends and
        // the string from `to_string`, itself destroyed once, are read.
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

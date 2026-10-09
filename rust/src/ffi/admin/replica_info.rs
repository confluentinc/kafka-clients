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

//! `kafka_admin_ReplicaInfo_t`: `org.apache.kafka.clients.admin.ReplicaInfo`
//! (CLAUDE.md §4). A plain value class: the handle points at the Rust value.

use std::ffi::{c_char, c_void};

use crate::admin::ReplicaInfo;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`ReplicaInfo`].
#[repr(C)]
pub struct kafka_admin_ReplicaInfo_t {
    _private: [u8; 0],
}

/// Hands `info` to C as an owned handle, freed with
/// [`kafka_admin_ReplicaInfo_destroy`].
pub(crate) fn box_replica_info(info: ReplicaInfo) -> *mut kafka_admin_ReplicaInfo_t {
    Box::into_raw(Box::new(info)) as *mut kafka_admin_ReplicaInfo_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `info` must be a live replica-info handle.
pub(crate) unsafe fn replica_info_ref<'a>(info: *const kafka_admin_ReplicaInfo_t) -> &'a ReplicaInfo {
    unsafe { &*(info as *const ReplicaInfo) }
}

/// Frees a `kafka_admin_ReplicaInfo_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned replica-info handle.
pub(crate) unsafe fn destroy_replica_info_element(element: *mut c_void) {
    unsafe { kafka_admin_ReplicaInfo_destroy(element as *mut kafka_admin_ReplicaInfo_t) }
}

/// `new ReplicaInfo(long size, long offsetLag, boolean isFuture)`. Owned,
/// freed with [`kafka_admin_ReplicaInfo_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ReplicaInfo_new(
    size: i64,
    offset_lag: i64,
    is_future: i8,
) -> *mut kafka_admin_ReplicaInfo_t {
    box_replica_info(ReplicaInfo::new(size, offset_lag, is_future != 0))
}

/// `size()`: the total size of the log segments in this replica, in bytes.
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_size(self_: *const kafka_admin_ReplicaInfo_t) -> i64 {
    unsafe { replica_info_ref(self_) }.size()
}

/// `offsetLag()`: the lag of the log's LEO with respect to the partition's
/// high watermark (or the current replica's LEO for a future log).
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_offset_lag(self_: *const kafka_admin_ReplicaInfo_t) -> i64 {
    unsafe { replica_info_ref(self_) }.offset_lag()
}

/// `isFuture()`: whether this replica has been created by an
/// `alterReplicaLogDirs` and will replace the current one.
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_is_future(self_: *const kafka_admin_ReplicaInfo_t) -> i8 {
    i8::from(unsafe { replica_info_ref(self_) }.is_future())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_to_string(self_: *const kafka_admin_ReplicaInfo_t) -> *mut c_char {
    into_c_string(&unsafe { replica_info_ref(self_) }.to_string())
}

/// Frees an owned replica-info handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_destroy(self_: *mut kafka_admin_ReplicaInfo_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut ReplicaInfo) });
    }
}

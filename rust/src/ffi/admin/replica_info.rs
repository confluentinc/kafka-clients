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
use crate::ffi::ffi_guard;
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
/// `info` must be a valid replica-info handle, kept alive for `'a`.
pub(crate) unsafe fn replica_info_ref<'a>(info: *const kafka_admin_ReplicaInfo_t) -> &'a ReplicaInfo {
    // SAFETY: Per this function's `# Safety`, `info` is a valid replica-info handle kept alive for
    // `'a`; every `kafka_admin_ReplicaInfo_t` is the `Box::into_raw` of a `Box<ReplicaInfo>`
    // (`box_replica_info`, the only producer), so the shared reference is to a live `ReplicaInfo`,
    // which no entry point mutates.
    unsafe { &*(info as *const ReplicaInfo) }
}

/// Frees a `kafka_admin_ReplicaInfo_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned replica-info handle not yet destroyed, that no
/// other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_replica_info_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned replica-info handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_ReplicaInfo_destroy` requires.
    unsafe { kafka_admin_ReplicaInfo_destroy(element as *mut kafka_admin_ReplicaInfo_t) }
}

/// Creates the description of a replica's log directory placement.
///
/// # Parameters
///
/// - `size` - the total size of the log segments in this replica, in bytes.
/// - `offset_lag` - the lag of the log's LEO with respect to the partition's high watermark, or
///   to the current replica's LEO for a future log.
/// - `is_future` - 1 if this is the future log of the partition, else 0.
///
/// # Returns
///
/// The replica info.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with `kafka_admin_ReplicaInfo_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_ReplicaInfo_new(
    size: i64,
    offset_lag: i64,
    is_future: i8,
) -> *mut kafka_admin_ReplicaInfo_t {
    box_replica_info(ReplicaInfo::new(size, offset_lag, is_future != 0))
}

/// Returns the total size of the log segments in this replica in bytes. This value does not
/// include the size of data stored in remote storage.
///
/// A caught Rust panic returns -1, which no replica size is.
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[ffi_guard(fallback = -1)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_size(self_: *const kafka_admin_ReplicaInfo_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid replica-info handle, kept alive
    // for this call, as `replica_info_ref` requires.
    unsafe { replica_info_ref(self_) }.size()
}

/// Returns the lag of the log's LEO with respect to the partition's high watermark (if it is the
/// current log for the partition) or the current replica's LEO (if it is the future log for the
/// partition).
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_offset_lag(self_: *const kafka_admin_ReplicaInfo_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid replica-info handle, kept alive
    // for this call, as `replica_info_ref` requires.
    unsafe { replica_info_ref(self_) }.offset_lag()
}

/// `isFuture()`: whether this replica has been created by an
/// `alterReplicaLogDirs` and will replace the current one.
///
/// # Returns
///
/// true if this log is created by AlterReplicaLogDirsRequest and will replace the current log of
/// the replica at some time in the future.
///
/// # Safety
///
/// `self_` must be a valid replica-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_is_future(self_: *const kafka_admin_ReplicaInfo_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid replica-info handle, kept alive
    // for this call, as `replica_info_ref` requires.
    i8::from(unsafe { replica_info_ref(self_) }.is_future())
}

/// Returns the text form of the replica info.
///
/// # Safety
///
/// `self_` must be a valid replica-info handle. The returned string is owned, freed with
/// `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_to_string(self_: *const kafka_admin_ReplicaInfo_t) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid replica-info handle, kept alive
    // for this call, as `replica_info_ref` requires.
    into_c_string(&unsafe { replica_info_ref(self_) }.to_string())
}

/// Frees an owned replica-info handle; `NULL` is a no-op.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_ReplicaInfo_destroy(self_: *mut kafka_admin_ReplicaInfo_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards; every handle is the `Box::into_raw` of a `Box<ReplicaInfo>`
        // (`box_replica_info`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut ReplicaInfo) });
    }
}

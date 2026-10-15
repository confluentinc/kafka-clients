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

//! `kafka_admin_DeletedRecords_t`: `org.apache.kafka.clients.admin.DeletedRecords`
//! (CLAUDE.md §4). A plain value class: the handle points at the Rust value.

use std::ffi::c_void;

use crate::admin::DeletedRecords;
use crate::ffi::ffi_guard;

/// Opaque handle to a [`DeletedRecords`].
#[repr(C)]
pub struct kafka_admin_DeletedRecords_t {
    _private: [u8; 0],
}

/// Hands `records` to C as an owned handle, freed with
/// [`kafka_admin_DeletedRecords_destroy`].
pub(crate) fn box_deleted_records(records: DeletedRecords) -> *mut kafka_admin_DeletedRecords_t {
    Box::into_raw(Box::new(records)) as *mut kafka_admin_DeletedRecords_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `records` must be a valid deleted-records handle, kept alive for `'a`.
pub(crate) unsafe fn deleted_records_ref<'a>(records: *const kafka_admin_DeletedRecords_t) -> &'a DeletedRecords {
    // SAFETY: Per this function's `# Safety`, `records` is a valid deleted-records handle kept
    // alive for `'a`; every `kafka_admin_DeletedRecords_t` is the `Box::into_raw` of a
    // `Box<DeletedRecords>` (`box_deleted_records`, the only producer), so the shared reference is
    // to a live `DeletedRecords`, which no entry point mutates.
    unsafe { &*(records as *const DeletedRecords) }
}

/// Frees a `kafka_admin_DeletedRecords_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned deleted-records handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
pub(crate) unsafe fn destroy_deleted_records_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned deleted-records handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_DeletedRecords_destroy` requires.
    unsafe { kafka_admin_DeletedRecords_destroy(element as *mut kafka_admin_DeletedRecords_t) }
}

/// `new DeletedRecords(long lowWatermark)`. Owned, freed with
/// [`kafka_admin_DeletedRecords_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_DeletedRecords_new(low_watermark: i64) -> *mut kafka_admin_DeletedRecords_t {
    box_deleted_records(DeletedRecords::new(low_watermark))
}

/// `lowWatermark()`: the "low watermark" for the topic partition on which the
/// deletion was executed.
///
/// # Safety
///
/// `self_` must be a valid deleted-records handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeletedRecords_low_watermark(self_: *const kafka_admin_DeletedRecords_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deleted-records handle, kept alive
    // for this call, as `deleted_records_ref` requires.
    unsafe { deleted_records_ref(self_) }.low_watermark()
}

/// Frees an owned deleted-records handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeletedRecords_destroy(self_: *mut kafka_admin_DeletedRecords_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards; every handle is the `Box::into_raw` of a `Box<DeletedRecords>`
        // (`box_deleted_records`), so `from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut DeletedRecords) });
    }
}

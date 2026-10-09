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
/// `records` must be a live deleted-records handle.
pub(crate) unsafe fn deleted_records_ref<'a>(records: *const kafka_admin_DeletedRecords_t) -> &'a DeletedRecords {
    unsafe { &*(records as *const DeletedRecords) }
}

/// Frees a `kafka_admin_DeletedRecords_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned deleted-records handle.
pub(crate) unsafe fn destroy_deleted_records_element(element: *mut c_void) {
    unsafe { kafka_admin_DeletedRecords_destroy(element as *mut kafka_admin_DeletedRecords_t) }
}

/// `new DeletedRecords(long lowWatermark)`. Owned, freed with
/// [`kafka_admin_DeletedRecords_destroy`].
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeletedRecords_low_watermark(self_: *const kafka_admin_DeletedRecords_t) -> i64 {
    unsafe { deleted_records_ref(self_) }.low_watermark()
}

/// Frees an owned deleted-records handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DeletedRecords_destroy(self_: *mut kafka_admin_DeletedRecords_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut DeletedRecords) });
    }
}

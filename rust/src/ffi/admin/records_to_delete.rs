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

//! `kafka_admin_RecordsToDelete_t`:
//! `org.apache.kafka.clients.admin.RecordsToDelete` (CLAUDE.md §4).

use std::ffi::c_char;

use crate::admin::RecordsToDelete;
use crate::ffi::util::into_c_string;

/// Opaque handle to a [`RecordsToDelete`].
#[repr(C)]
pub struct kafka_admin_RecordsToDelete_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_RecordsToDelete_t`] points at.
pub(crate) struct RecordsToDeleteInner {
    records: RecordsToDelete,
}

/// The value behind a handle.
///
/// # Safety
///
/// `records` must be a valid records-to-delete handle.
pub(crate) unsafe fn records_to_delete_ref<'a>(records: *const kafka_admin_RecordsToDelete_t) -> &'a RecordsToDelete {
    &unsafe { &*(records as *const RecordsToDeleteInner) }.records
}

/// Hands `records` to C as an owned handle, freed with
/// [`kafka_admin_RecordsToDelete_destroy`].
pub(crate) fn box_records_to_delete(records: RecordsToDelete) -> *mut kafka_admin_RecordsToDelete_t {
    Box::into_raw(Box::new(RecordsToDeleteInner { records })) as *mut kafka_admin_RecordsToDelete_t
}

/// `RecordsToDelete.beforeOffset(long offset)`, the static factory: delete
/// every record before `offset`. Owned, freed with
/// [`kafka_admin_RecordsToDelete_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_admin_RecordsToDelete_with_offset(offset: i64) -> *mut kafka_admin_RecordsToDelete_t {
    box_records_to_delete(RecordsToDelete::with_offset(offset))
}

/// `beforeOffset()`, the getter.
///
/// # Safety
///
/// `self_` must be a valid records-to-delete handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RecordsToDelete_before_offset(self_: *const kafka_admin_RecordsToDelete_t) -> i64 {
    unsafe { records_to_delete_ref(self_) }.before_offset()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid records-to-delete handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RecordsToDelete_to_string(
    self_: *const kafka_admin_RecordsToDelete_t,
) -> *mut c_char {
    into_c_string(&unsafe { records_to_delete_ref(self_) }.to_string())
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_RecordsToDelete_destroy(self_: *mut kafka_admin_RecordsToDelete_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut RecordsToDeleteInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn factory_getter_and_to_string() {
        let records = kafka_admin_RecordsToDelete_with_offset(10);
        unsafe {
            assert_eq!(*records_to_delete_ref(records), RecordsToDelete::with_offset(10));
            assert_eq!(kafka_admin_RecordsToDelete_before_offset(records), 10);
            let s = kafka_admin_RecordsToDelete_to_string(records);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), "(beforeOffset = 10)");
            kafka_string_destroy(s);
            kafka_admin_RecordsToDelete_destroy(records);
            kafka_admin_RecordsToDelete_destroy(ptr::null_mut());
        }
    }
}

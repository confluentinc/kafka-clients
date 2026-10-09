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

//! `kafka_common_header_internals_RecordHeader_t`:
//! `org.apache.kafka.common.header.internals.RecordHeader` (CLAUDE.md §4).
//!
//! The handle owns a [`RecordHeader`] plus, once asked for, the `Header`
//! interface view `__as_Header` hands out. That view points back into the
//! handle, so every owner keeps a [`RecordHeaderInner`] boxed and never moves
//! it after the view exists.

use std::ffi::c_char;
use std::sync::OnceLock;

use crate::common::header::{Header, RecordHeader};
use crate::ffi::common::header::{HeaderInner, kafka_common_header_Header_t};
use crate::ffi::util::{c_str_to_string, into_c_string, kafka_Bytes_t, kafka_List_t, list_elements};

/// Opaque handle to a [`RecordHeader`]. Public although Java keeps the
/// class in `internals`: it is the type the `Headers` getters return, and the
/// Rust type carries the same `public-in-rust` tag.
#[doc(alias = "public-in-rust")]
#[repr(C)]
pub struct kafka_common_header_internals_RecordHeader_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_header_internals_RecordHeader_t`] points at: the
/// header plus the lazily built `Header` view.
pub(crate) struct RecordHeaderInner {
    header: RecordHeader,
    view: OnceLock<HeaderInner>,
}

impl RecordHeaderInner {
    pub(crate) fn new(header: RecordHeader) -> Self {
        Self { header, view: OnceLock::new() }
    }

    /// A handle at a stable address, as the `Header` view requires.
    pub(crate) fn boxed(header: RecordHeader) -> Box<Self> {
        Box::new(Self::new(header))
    }

    pub(crate) fn header(&self) -> &RecordHeader {
        &self.header
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_header_internals_RecordHeader_t {
        self as *const Self as *const kafka_common_header_internals_RecordHeader_t
    }

    /// The `Header` interface view, built on first request. It points back
    /// into `self`, which therefore must not move afterwards.
    pub(crate) fn as_header_ptr(&self) -> *const kafka_common_header_Header_t {
        self.view
            .get_or_init(|| unsafe { HeaderInner::new(&self.header as *const RecordHeader as *const dyn Header) })
            .as_ptr()
    }
}

/// Hands `header` to C as an owned handle, freed with
/// [`kafka_common_header_internals_RecordHeader_destroy`].
pub(crate) fn box_record_header(header: RecordHeader) -> *mut kafka_common_header_internals_RecordHeader_t {
    Box::into_raw(RecordHeaderInner::boxed(header)) as *mut kafka_common_header_internals_RecordHeader_t
}

/// The header behind a handle.
///
/// # Safety
///
/// `header` must be a valid record-header handle.
pub(crate) unsafe fn record_header_ref<'a>(
    header: *const kafka_common_header_internals_RecordHeader_t,
) -> &'a RecordHeader {
    unsafe { &*(header as *const RecordHeaderInner) }.header()
}

/// Reads a list of `const kafka_common_header_internals_RecordHeader_t *`
/// into owned headers, in order; null reads as empty.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are record-header
/// handles.
pub(crate) unsafe fn list_record_headers(list: *const kafka_List_t) -> Vec<RecordHeader> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            unsafe { record_header_ref(element as *const kafka_common_header_internals_RecordHeader_t) }.clone()
        })
        .collect()
}

/// `new RecordHeader(String key, byte[] value)`: `value.data == NULL` is
/// Java's null value; both are copied. Owned, freed with
/// [`kafka_common_header_internals_RecordHeader_destroy`].
///
/// # Safety
///
/// `key` must be a valid NUL-terminated string and `value` null or a valid
/// buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader_new(
    key: *const c_char,
    value: kafka_Bytes_t,
) -> *mut kafka_common_header_internals_RecordHeader_t {
    box_record_header(RecordHeader::new(
        unsafe { c_str_to_string(key) },
        unsafe { value.as_slice() }.map(<[u8]>::to_vec),
    ))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-header handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader_to_string(
    self_: *const kafka_common_header_internals_RecordHeader_t,
) -> *mut c_char {
    into_c_string(&unsafe { record_header_ref(self_) }.to_string())
}

/// The handle as the `Header` interface it implements: a borrowed view valid
/// until the handle is destroyed, never passed to a `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-header handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader__as_Header(
    self_: *const kafka_common_header_internals_RecordHeader_t,
) -> *const kafka_common_header_Header_t {
    unsafe { &*(self_ as *const RecordHeaderInner) }.as_header_ptr()
}

/// Frees an owned record-header handle; null is a no-op. The borrowed
/// elements of a `kafka_common_header_Headers_t` getter's list are never
/// passed here.
///
/// # Safety
///
/// `self_` must be null or an owned record-header handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader_destroy(
    self_: *mut kafka_common_header_internals_RecordHeader_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut RecordHeaderInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString, c_void};
    use std::ptr;

    use super::*;
    use crate::ffi::common::header::{kafka_common_header_Header_key, kafka_common_header_Header_value};
    use crate::ffi::util::{kafka_List_add, kafka_List_destroy, kafka_List_new, kafka_string_destroy};

    #[test]
    fn handle_round_trips_key_value_and_display() {
        let key = CString::new("h1").unwrap();
        let bytes = [9_u8, 9];
        unsafe {
            let header =
                kafka_common_header_internals_RecordHeader_new(key.as_ptr(), kafka_Bytes_t::from_slice(&bytes));
            assert_eq!(
                record_header_ref(header),
                &RecordHeader::new("h1".to_string(), Some(vec![9, 9]))
            );
            let s = kafka_common_header_internals_RecordHeader_to_string(header);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                RecordHeader::new("h1".to_string(), Some(vec![9, 9])).to_string()
            );
            kafka_string_destroy(s);

            // The interface view is borrowed and cached: the same pointer twice.
            let view = kafka_common_header_internals_RecordHeader__as_Header(header);
            assert_eq!(view, kafka_common_header_internals_RecordHeader__as_Header(header));
            assert_eq!(CStr::from_ptr(kafka_common_header_Header_key(view)).to_str().unwrap(), "h1");
            assert_eq!(kafka_common_header_Header_value(view).as_slice(), Some(&bytes[..]));
            kafka_common_header_internals_RecordHeader_destroy(header);

            // A null value stays null through the view.
            let null = kafka_common_header_internals_RecordHeader_new(key.as_ptr(), kafka_Bytes_t::NULL);
            let view = kafka_common_header_internals_RecordHeader__as_Header(null);
            assert!(kafka_common_header_Header_value(view).data.is_null());
            kafka_common_header_internals_RecordHeader_destroy(null);
            kafka_common_header_internals_RecordHeader_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn list_reads_handles_in_order() {
        let a = CString::new("a").unwrap();
        let b = CString::new("b").unwrap();
        unsafe {
            let list = kafka_List_new();
            let first = kafka_common_header_internals_RecordHeader_new(a.as_ptr(), kafka_Bytes_t::NULL);
            let second = kafka_common_header_internals_RecordHeader_new(b.as_ptr(), kafka_Bytes_t::NULL);
            kafka_List_add(list, first as *mut c_void);
            kafka_List_add(list, second as *mut c_void);
            let headers = list_record_headers(list);
            assert_eq!(headers.iter().map(|h| h.key()).collect::<Vec<_>>(), ["a", "b"]);
            assert!(list_record_headers(ptr::null()).is_empty());
            kafka_List_destroy(list);
            kafka_common_header_internals_RecordHeader_destroy(first);
            kafka_common_header_internals_RecordHeader_destroy(second);
        }
    }
}

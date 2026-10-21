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
use crate::ffi::ffi_guard;
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
            // SAFETY: `&self.header` is a live `RecordHeader` that outlives the view and is not
            // mutated while it exists, as `HeaderInner::new` requires: the view is cached in
            // `self.view`, a field of the same `RecordHeaderInner`, so it is dropped with the
            // header; no code mutates a `RecordHeaderInner`'s `header`; and, per this method's
            // rustdoc, `self` does not move once the view exists.
            .get_or_init(|| unsafe {
                HeaderInner::new(&self.header as *const RecordHeader as *const (dyn Header + Send + Sync))
            })
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
/// `header` must be a valid record-header handle, alive for `'a`.
pub(crate) unsafe fn record_header_ref<'a>(
    header: *const kafka_common_header_internals_RecordHeader_t,
) -> &'a RecordHeader {
    // SAFETY: Per this function's `# Safety`, `header` is a valid record-header handle alive for
    // `'a`, so it points at a live `RecordHeaderInner`: the `Box::into_raw` of `box_record_header`,
    // or an element of a `HeadersInner`'s `views` (alive until that headers handle's next mutation,
    // as the module docs of `ffi::common::header` state). No code mutates a `RecordHeaderInner`'s
    // `header`, so the shared reference is sound for `'a`.
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
    // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle, as
    // `list_elements` requires; the slice is used only within this call, which does not mutate the
    // list.
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            // SAFETY: Per this function's `# Safety`, every element of `list` is a record-header
            // handle, as `record_header_ref` requires; the header is cloned, so the borrow lasts
            // for this call only.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader_new(
    key: *const c_char,
    value: kafka_Bytes_t,
) -> *mut kafka_common_header_internals_RecordHeader_t {
    box_record_header(RecordHeader::new(
        // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(key) },
        // SAFETY: Per this function's `# Safety`, `value` is null or a valid buffer, so
        // `value.data` is null or valid for `value.len` bytes, as `kafka_Bytes_t::as_slice`
        // requires; the bytes are copied into an owned `Vec`, so nothing borrows them after this
        // call.
        unsafe { value.as_slice() }.map(<[u8]>::to_vec),
    ))
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-header handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader_to_string(
    self_: *const kafka_common_header_internals_RecordHeader_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-header handle, as
    // `record_header_ref` requires; the result is copied into an owned value before return, so the
    // borrow lasts for this call only.
    into_c_string(&unsafe { record_header_ref(self_) }.to_string())
}

/// The handle as the `Header` interface it implements: a borrowed view valid
/// until the handle is destroyed, never passed to a `_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-header handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader__as_Header(
    self_: *const kafka_common_header_internals_RecordHeader_t,
) -> *const kafka_common_header_Header_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-header handle, so it points
    // at a live `RecordHeaderInner` that does not move while it lives, as `as_header_ptr` requires:
    // a boxed one, or an element of a `HeadersInner`'s `views`, a vector never pushed to after it
    // is built. The view returned
    // points into the handle, so it lives as long as the handle, as the rustdoc states.
    unsafe { &*(self_ as *const RecordHeaderInner) }.as_header_ptr()
}

/// Frees an owned record-header handle; null is a no-op. The borrowed
/// elements of a `kafka_common_header_Headers_t` getter's list are never
/// passed here.
///
/// # Safety
///
/// `self_` must be null or an owned record-header handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeader_destroy(
    self_: *mut kafka_common_header_internals_RecordHeader_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // record-header handle not yet destroyed, i.e. the `Box::into_raw` of a `RecordHeaderInner`
        // in `box_record_header` (a borrowed element of a `Headers` getter's list is never passed
        // here), so `from_raw` reclaims that allocation, once.
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
        // SAFETY: Test code: `key` and `bytes` outlive the calls reading them; each handle comes
        // from `kafka_common_header_internals_RecordHeader_new` and is destroyed once, after its
        // `Header` view, whose key and value are read before then; the string from `to_string` is
        // destroyed once, after it is read; destroying null is a no-op.
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
        // SAFETY: Test code: `a` and `b` outlive the constructors reading them; `first` and
        // `second` come from `kafka_common_header_internals_RecordHeader_new` and are destroyed
        // once, after `list_record_headers` clones them and after the list that borrows them is
        // destroyed; `list_record_headers` reads null as empty.
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

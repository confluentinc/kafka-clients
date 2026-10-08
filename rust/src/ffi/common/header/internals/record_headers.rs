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

//! `kafka_common_header_internals_RecordHeaders_t`:
//! `org.apache.kafka.common.header.internals.RecordHeaders` (CLAUDE.md §4).
//!
//! The handle owns a [`RecordHeaders`] plus, once asked for, the `Headers`
//! interface view `__as_Headers` hands out. The view points back into the
//! handle, so every owner keeps a [`RecordHeadersInner`] boxed and never
//! moves it after the view exists. The `Headers` methods themselves
//! (`add_*`, `remove`, `last_header`, ...) are reached through that view.

use std::ffi::c_char;
use std::ptr;
use std::sync::OnceLock;

use crate::common::header::{Headers, RecordHeaders};
use crate::ffi::common::header::internals::record_header::list_record_headers;
use crate::ffi::common::header::{HeadersInner, kafka_common_header_Headers_t};
use crate::ffi::util::{into_c_string, kafka_List_t};

/// Opaque handle to a [`RecordHeaders`]. Public although Java keeps the
/// class in `internals`: it is the `Headers` implementation the public API
/// hands out, and the Rust type carries the same `public-in-rust` tag.
#[doc(alias = "public-in-rust")]
#[repr(C)]
pub struct kafka_common_header_internals_RecordHeaders_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_header_internals_RecordHeaders_t`] points at: the
/// headers plus the lazily built `Headers` view.
pub(crate) struct RecordHeadersInner {
    headers: RecordHeaders,
    view: OnceLock<HeadersInner>,
}

impl RecordHeadersInner {
    pub(crate) fn new(headers: RecordHeaders) -> Self {
        Self { headers, view: OnceLock::new() }
    }

    /// A handle at a stable address, as the `Headers` view requires.
    pub(crate) fn boxed(headers: RecordHeaders) -> Box<Self> {
        Box::new(Self::new(headers))
    }

    pub(crate) fn headers(&self) -> &RecordHeaders {
        &self.headers
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_header_internals_RecordHeaders_t {
        self as *const Self as *const kafka_common_header_internals_RecordHeaders_t
    }

    /// The `Headers` interface view on the handle at `this`, built on first
    /// request; `*mut` because the trait mutates. Works on the raw pointer so
    /// the view may later mutate the headers it points at.
    ///
    /// # Safety
    ///
    /// `this` must point at a live handle that does not move afterwards.
    pub(crate) unsafe fn headers_view(this: *mut Self) -> *mut kafka_common_header_Headers_t {
        let headers = unsafe { &raw mut (*this).headers } as *mut dyn Headers;
        let view = unsafe { &raw mut (*this).view };
        unsafe { (*view).get_or_init(|| HeadersInner::borrowed(headers)) };
        unsafe { (*view).get_mut() }.map_or(ptr::null_mut(), HeadersInner::as_mut_ptr)
    }
}

/// Hands `headers` to C as an owned handle, freed with
/// [`kafka_common_header_internals_RecordHeaders_destroy`].
pub(crate) fn box_record_headers(headers: RecordHeaders) -> *mut kafka_common_header_internals_RecordHeaders_t {
    Box::into_raw(RecordHeadersInner::boxed(headers)) as *mut kafka_common_header_internals_RecordHeaders_t
}

/// The headers behind a handle.
///
/// # Safety
///
/// `headers` must be a valid record-headers handle.
pub(crate) unsafe fn record_headers_ref<'a>(
    headers: *const kafka_common_header_internals_RecordHeaders_t,
) -> &'a RecordHeaders {
    unsafe { &*(headers as *const RecordHeadersInner) }.headers()
}

/// `new RecordHeaders()`: empty, mutable. Owned, freed with
/// [`kafka_common_header_internals_RecordHeaders_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_header_internals_RecordHeaders_new() -> *mut kafka_common_header_internals_RecordHeaders_t
{
    box_record_headers(RecordHeaders::new())
}

/// `new RecordHeaders(Header[] headers)`: `headers` holds
/// `const kafka_common_header_internals_RecordHeader_t *`, copied.
///
/// # Safety
///
/// `headers` must be null or a valid list of record-header handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_with_header_slice(
    headers: *const kafka_List_t,
) -> *mut kafka_common_header_internals_RecordHeaders_t {
    box_record_headers(RecordHeaders::with_header_slice(&unsafe { list_record_headers(headers) }))
}

/// `new RecordHeaders(Iterable<Header> headers)`: `headers` holds
/// `const kafka_common_header_internals_RecordHeader_t *`, copied.
///
/// # Safety
///
/// `headers` must be null or a valid list of record-header handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_with_header_iter(
    headers: *const kafka_List_t,
) -> *mut kafka_common_header_internals_RecordHeaders_t {
    box_record_headers(RecordHeaders::with_header_iter(unsafe { list_record_headers(headers) }))
}

/// `new RecordHeaders(RecordHeaders other)`: a mutable copy of `other`.
///
/// # Safety
///
/// `other` must be a valid record-headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_with_record_headers(
    other: *const kafka_common_header_internals_RecordHeaders_t,
) -> *mut kafka_common_header_internals_RecordHeaders_t {
    box_record_headers(RecordHeaders::with_record_headers(unsafe { record_headers_ref(other) }))
}

/// `setReadOnly()`: every later mutation through the `Headers` view fails
/// with the translation of `IllegalStateException`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_set_read_only(
    self_: *mut kafka_common_header_internals_RecordHeaders_t,
) {
    unsafe { &mut *(self_ as *mut RecordHeadersInner) }.headers.set_read_only();
}

/// `isReadOnly()`, as `0` / `1`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_is_read_only(
    self_: *const kafka_common_header_internals_RecordHeaders_t,
) -> i8 {
    i8::from(unsafe { record_headers_ref(self_) }.is_read_only())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_to_string(
    self_: *const kafka_common_header_internals_RecordHeaders_t,
) -> *mut c_char {
    into_c_string(&unsafe { record_headers_ref(self_) }.to_string())
}

/// The handle as the `Headers` interface it implements: a borrowed view
/// valid until the handle is destroyed, never passed to
/// `kafka_common_header_Headers_destroy`. Mutable, as `Headers` mutates.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders__as_Headers(
    self_: *mut kafka_common_header_internals_RecordHeaders_t,
) -> *mut kafka_common_header_Headers_t {
    unsafe { RecordHeadersInner::headers_view(self_ as *mut RecordHeadersInner) }
}

/// Frees an owned record-headers handle; null is a no-op. The view a
/// `kafka_common_RecordDeserializationError_headers` getter borrows out is
/// never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned record-headers handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_destroy(
    self_: *mut kafka_common_header_internals_RecordHeaders_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut RecordHeadersInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString, c_void};

    use super::*;
    use crate::common::header::{Header, RecordHeader};
    use crate::ffi::common::header::internals::record_header::kafka_common_header_internals_RecordHeader_t;
    use crate::ffi::common::header::internals::record_header::{
        kafka_common_header_internals_RecordHeader_destroy, kafka_common_header_internals_RecordHeader_new,
        record_header_ref,
    };
    use crate::ffi::common::header::{
        kafka_common_header_Headers_add_with_key_value, kafka_common_header_Headers_to_array,
    };
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_state_error;
    use crate::ffi::util::{
        kafka_Bytes_t, kafka_List_add, kafka_List_destroy, kafka_List_get, kafka_List_new, kafka_List_size,
        kafka_string_destroy,
    };

    #[test]
    fn constructors_copy_their_input_and_display_follows_java() {
        let a = CString::new("a").unwrap();
        unsafe {
            let header = kafka_common_header_internals_RecordHeader_new(a.as_ptr(), kafka_Bytes_t::from_slice(&[1]));
            let list = kafka_List_new();
            kafka_List_add(list, header as *mut c_void);

            let from_slice = kafka_common_header_internals_RecordHeaders_with_header_slice(list);
            let from_iter = kafka_common_header_internals_RecordHeaders_with_header_iter(list);
            kafka_List_destroy(list);
            kafka_common_header_internals_RecordHeader_destroy(header);
            let expected = RecordHeaders::with_header_iter([RecordHeader::new("a".to_string(), Some(vec![1]))]);
            assert_eq!(record_headers_ref(from_slice), &expected);
            assert_eq!(record_headers_ref(from_iter), &expected);

            let copy = kafka_common_header_internals_RecordHeaders_with_record_headers(from_slice);
            assert_eq!(record_headers_ref(copy), &expected);
            let s = kafka_common_header_internals_RecordHeaders_to_string(copy);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);

            let empty = kafka_common_header_internals_RecordHeaders_new();
            assert_eq!(record_headers_ref(empty), &RecordHeaders::new());
            assert_eq!(
                record_headers_ref(kafka_common_header_internals_RecordHeaders_with_header_slice(ptr::null()))
                    .to_array()
                    .len(),
                0
            );

            for handle in [from_slice, from_iter, copy, empty] {
                kafka_common_header_internals_RecordHeaders_destroy(handle);
            }
            kafka_common_header_internals_RecordHeaders_destroy(ptr::null_mut());
        }
    }

    #[test]
    fn interface_view_mutates_the_class_and_honours_read_only() {
        let k = CString::new("k").unwrap();
        unsafe {
            let headers = kafka_common_header_internals_RecordHeaders_new();
            let view = kafka_common_header_internals_RecordHeaders__as_Headers(headers);
            // The view is borrowed and cached.
            assert_eq!(view, kafka_common_header_internals_RecordHeaders__as_Headers(headers));
            assert!(
                kafka_common_header_Headers_add_with_key_value(view, k.as_ptr(), kafka_Bytes_t::from_slice(&[7]))
                    .is_null()
            );
            // The class handle sees the mutation made through the view ...
            assert_eq!(record_headers_ref(headers).to_array().len(), 1);
            // ... and the view hands the header back as a borrowed handle.
            let array = kafka_common_header_Headers_to_array(view);
            assert_eq!(kafka_List_size(array), 1);
            let first =
                record_header_ref(kafka_List_get(array, 0) as *const kafka_common_header_internals_RecordHeader_t);
            assert_eq!(first.key(), "k");
            assert_eq!(first.value(), Some(&[7_u8][..]));
            kafka_List_destroy(array);

            assert_eq!(kafka_common_header_internals_RecordHeaders_is_read_only(headers), 0);
            kafka_common_header_internals_RecordHeaders_set_read_only(headers);
            assert_eq!(kafka_common_header_internals_RecordHeaders_is_read_only(headers), 1);
            let error = kafka_common_header_Headers_add_with_key_value(view, k.as_ptr(), kafka_Bytes_t::NULL);
            assert!(!error.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_state_error(error), 1);
            kafka_common_Error_destroy(error);
            kafka_common_header_internals_RecordHeaders_destroy(headers);
        }
    }
}

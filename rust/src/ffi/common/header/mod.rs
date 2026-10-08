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

//! `kafka_common_header_Header_t` and `kafka_common_header_Headers_t`: the
//! `org.apache.kafka.common.header.Header` / `Headers` interfaces (CLAUDE.md
//! §4, "Traits").
//!
//! A `Header_t` is a borrowed view on a Rust [`Header`] implementation,
//! reached through `kafka_common_header_internals_RecordHeader__as_Header`.
//! It has no `_new`: nothing in the public API accepts a caller-supplied
//! `Header`, so C only reads one.
//!
//! A `Headers_t` is either the borrowed view
//! `kafka_common_header_internals_RecordHeaders__as_Headers` returns, valid
//! until that class handle is destroyed, or a C implementation registered
//! with [`kafka_common_header_Headers_new`] (what a C caller passes to
//! `Deserializer_deserialize_with_headers`) and freed with
//! [`kafka_common_header_Headers_destroy`].
//!
//! The getters that return headers (`last_header`, `headers`, `to_array`,
//! `iter`) hand out borrowed `kafka_common_header_internals_RecordHeader_t *`
//! copies the `Headers_t` owns. They stay valid until the next mutating call
//! on the same handle (`add_*`, `remove`), which rebuilds them, or until the
//! handle is destroyed; the lists holding them are owned by the caller and
//! freed with `kafka_List_destroy`, which leaves the elements alone.

#![expect(non_camel_case_types)]

pub(crate) mod internals;

use std::ffi::{CString, c_char, c_void};
use std::ptr;
use std::sync::OnceLock;

use crate::common::header::{Header, Headers, RecordHeader};
use crate::common::{Error, LocalIllegalStateError};
use crate::ffi::common::header::internals::record_header::{
    RecordHeaderInner, kafka_common_header_internals_RecordHeader_t, record_header_ref,
};
use crate::ffi::common::{box_error, kafka_common_Error_t, take_error};
use crate::ffi::util::{
    box_list, c_str_to_string, kafka_Bytes_t, kafka_List_destroy, kafka_List_t, list_elements, owned_c_string,
};

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Header`] implementation.
#[repr(C)]
pub struct kafka_common_header_Header_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_header_Header_t`] points at: the implementation,
/// owned by the class handle that produced this view, plus the
/// NUL-terminated copy of its key the string getter borrows out.
pub(crate) struct HeaderInner {
    header: *const dyn Header,
    key_c: CString,
}

impl HeaderInner {
    /// A view on `header`, whose owner must outlive the view.
    ///
    /// # Safety
    ///
    /// `header` must point at a live implementation.
    pub(crate) unsafe fn new(header: *const dyn Header) -> Self {
        let key_c = owned_c_string(unsafe { &*header }.key());
        Self { header, key_c }
    }

    fn header(&self) -> &dyn Header {
        unsafe { &*self.header }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_header_Header_t {
        self as *const Self as *const kafka_common_header_Header_t
    }
}

// SAFETY: the pointer targets a `RecordHeader` (`Send + Sync`) owned by the
// same handle tree as this view, and a handle tree crosses threads only as a
// whole, under the C caller's synchronisation, like every other handle.
unsafe impl Send for HeaderInner {}
unsafe impl Sync for HeaderInner {}

/// `key()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid header handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Header_key(self_: *const kafka_common_header_Header_t) -> *const c_char {
    unsafe { &*(self_ as *const HeaderInner) }.key_c.as_ptr()
}

/// `value()`: a view over the header's bytes, valid as long as the handle;
/// `data` is null for Java's null value.
///
/// # Safety
///
/// `self_` must be a valid header handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Header_value(self_: *const kafka_common_header_Header_t) -> kafka_Bytes_t {
    kafka_Bytes_t::from_option(unsafe { &*(self_ as *const HeaderInner) }.header().value())
}

// ---------------------------------------------------------------------------
// Headers
// ---------------------------------------------------------------------------

/// Opaque handle to a [`Headers`] implementation.
#[repr(C)]
pub struct kafka_common_header_Headers_t {
    _private: [u8; 0],
}

/// Where a `Headers_t`'s implementation lives.
enum HeadersImpl {
    /// A Rust implementation owned by its class handle
    /// (`RecordHeaders__as_Headers`).
    Borrowed(*mut dyn Headers),
    /// A C implementation registered through [`kafka_common_header_Headers_new`].
    Owned(Box<CHeaders>),
}

/// What a [`kafka_common_header_Headers_t`] points at: the implementation
/// plus the header handles its getters hand out.
pub(crate) struct HeadersInner {
    headers: HeadersImpl,
    /// The `RecordHeader_t` copies the getters return, over `to_array()` in
    /// order, built on first request and dropped by each mutating call. The
    /// vector is never pushed to after it is built, so each element (and the
    /// `Header` view it caches, which points back into it) keeps its address.
    views: OnceLock<Vec<RecordHeaderInner>>,
}

impl HeadersInner {
    /// A view on `headers`, whose owner must outlive it.
    pub(crate) fn borrowed(headers: *mut dyn Headers) -> Self {
        Self { headers: HeadersImpl::Borrowed(headers), views: OnceLock::new() }
    }

    fn owned(headers: CHeaders) -> Self {
        Self { headers: HeadersImpl::Owned(Box::new(headers)), views: OnceLock::new() }
    }

    fn headers(&self) -> &dyn Headers {
        match &self.headers {
            HeadersImpl::Borrowed(headers) => unsafe { &**headers },
            HeadersImpl::Owned(headers) => &**headers,
        }
    }

    /// Mutable access, dropping the views the mutation would invalidate.
    fn headers_mut(&mut self) -> &mut dyn Headers {
        self.views.take();
        match &mut self.headers {
            HeadersImpl::Borrowed(headers) => unsafe { &mut **headers },
            HeadersImpl::Owned(headers) => &mut **headers,
        }
    }

    fn views(&self) -> &[RecordHeaderInner] {
        self.views
            .get_or_init(|| self.headers().to_array().iter().cloned().map(RecordHeaderInner::new).collect())
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_mut_ptr(&mut self) -> *mut kafka_common_header_Headers_t {
        self as *mut Self as *mut kafka_common_header_Headers_t
    }
}

// SAFETY: the borrowed pointer targets a `RecordHeaders` (`Send + Sync`)
// owned by the same handle tree as this view, and the owned variant holds
// what the C caller registered, whose thread-safety is the caller's
// responsibility as for every interface implementation (CLAUDE.md §4).
unsafe impl Send for HeadersInner {}
unsafe impl Sync for HeadersInner {}

/// Reads a C implementation's error slot back into the trait's error type:
/// the translation of `IllegalStateException` passes through, any other
/// class keeps only its message.
unsafe fn illegal_state(error: *mut kafka_common_Error_t) -> Result<(), LocalIllegalStateError> {
    match unsafe { take_error(error) } {
        None => Ok(()),
        Some(Error::LocalIllegalState(e)) => Err(e),
        Some(other) => Err(LocalIllegalStateError::new(other.message())),
    }
}

/// The error slot of a mutating method: null on success.
fn result(result: Result<(), LocalIllegalStateError>) -> *mut kafka_common_Error_t {
    match result {
        Ok(()) => ptr::null_mut(),
        Err(e) => box_error(Error::LocalIllegalState(e)),
    }
}

/// An owned list of borrowed `kafka_common_header_internals_RecordHeader_t *`,
/// freed with `kafka_List_destroy` without touching the elements.
fn view_list<'a>(views: impl Iterator<Item = &'a RecordHeaderInner>) -> *mut kafka_List_t {
    box_list(views.map(|view| view.as_ptr() as *mut c_void).collect(), None)
}

unsafe fn headers_ref<'a>(self_: *const kafka_common_header_Headers_t) -> &'a HeadersInner {
    unsafe { &*(self_ as *const HeadersInner) }
}

unsafe fn headers_mut<'a>(self_: *mut kafka_common_header_Headers_t) -> &'a mut dyn Headers {
    unsafe { &mut *(self_ as *mut HeadersInner) }.headers_mut()
}

/// `add(Header header)`: appends a copy of `header`. Returns the translation
/// of `IllegalStateException` when the headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle and `header` a valid
/// record-header handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_add_with_header(
    self_: *mut kafka_common_header_Headers_t,
    header: *const kafka_common_header_internals_RecordHeader_t,
) -> *mut kafka_common_Error_t {
    let header = unsafe { record_header_ref(header) }.clone();
    result(unsafe { headers_mut(self_) }.add_with_header(header))
}

/// `add(String key, byte[] value)`: appends a header built from copies of
/// `key` and `value` (`value.data == NULL` is Java's null). Returns the
/// translation of `IllegalStateException` when the headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle, `key` a valid NUL-terminated
/// string and `value` null or a valid buffer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_add_with_key_value(
    self_: *mut kafka_common_header_Headers_t,
    key: *const c_char,
    value: kafka_Bytes_t,
) -> *mut kafka_common_Error_t {
    let key = unsafe { c_str_to_string(key) };
    result(unsafe { headers_mut(self_) }.add_with_key_value(&key, unsafe { value.as_slice() }))
}

/// `remove(String key)`: drops every header with `key`, keeping the order of
/// the rest. Returns the translation of `IllegalStateException` when the
/// headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle and `key` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_remove(
    self_: *mut kafka_common_header_Headers_t,
    key: *const c_char,
) -> *mut kafka_common_Error_t {
    let key = unsafe { c_str_to_string(key) };
    result(unsafe { headers_mut(self_) }.remove(&key))
}

/// `lastHeader(String key)`: the last header with `key`, borrowed from the
/// handle until its next mutation, or null.
///
/// # Safety
///
/// `self_` must be a valid headers handle and `key` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_last_header(
    self_: *const kafka_common_header_Headers_t,
    key: *const c_char,
) -> *const kafka_common_header_internals_RecordHeader_t {
    let key = unsafe { c_str_to_string(key) };
    unsafe { headers_ref(self_) }
        .views()
        .iter()
        .rev()
        .find(|view| view.header().key() == key)
        .map_or(ptr::null(), |view| view.as_ptr())
}

/// `headers(String key)`: every header with `key`, in insertion order, as an
/// owned list of borrowed `kafka_common_header_internals_RecordHeader_t *`
/// freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid headers handle and `key` a valid NUL-terminated
/// string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_headers(
    self_: *const kafka_common_header_Headers_t,
    key: *const c_char,
) -> *mut kafka_List_t {
    let key = unsafe { c_str_to_string(key) };
    view_list(
        unsafe { headers_ref(self_) }
            .views()
            .iter()
            .filter(|view| view.header().key() == key),
    )
}

/// `toArray()`: every header, in insertion order, as an owned list of
/// borrowed `kafka_common_header_internals_RecordHeader_t *` freed with
/// `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_to_array(
    self_: *const kafka_common_header_Headers_t,
) -> *mut kafka_List_t {
    view_list(unsafe { headers_ref(self_) }.views().iter())
}

/// `iterator()`: the same list as [`kafka_common_header_Headers_to_array`],
/// standing for the Rust-only `iter()`.
///
/// # Safety
///
/// `self_` must be a valid headers handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_iter(
    self_: *const kafka_common_header_Headers_t,
) -> *mut kafka_List_t {
    view_list(unsafe { headers_ref(self_) }.views().iter())
}

// ---------------------------------------------------------------------------
// C implementations of `Headers`
// ---------------------------------------------------------------------------

/// `add(Header header)` of a C implementation: `header` is borrowed for the
/// call and copied by the implementation. Returns null, or an owned error
/// the Rust side takes over.
pub type kafka_common_header_Headers_add_with_header_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    header: *const kafka_common_header_internals_RecordHeader_t,
) -> *mut kafka_common_Error_t;

/// `add(String key, byte[] value)` of a C implementation: both are borrowed
/// for the call. Returns null, or an owned error the Rust side takes over.
pub type kafka_common_header_Headers_add_with_key_value_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, key: *const c_char, value: kafka_Bytes_t) -> *mut kafka_common_Error_t;

/// `remove(String key)` of a C implementation. Returns null, or an owned
/// error the Rust side takes over.
pub type kafka_common_header_Headers_remove_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, key: *const c_char) -> *mut kafka_common_Error_t;

/// `lastHeader(String key)` of a C implementation: a handle the C side owns
/// and keeps valid until its next mutation, or null.
pub type kafka_common_header_Headers_last_header_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, key: *const c_char) -> *const kafka_common_header_internals_RecordHeader_t;

/// `headers(String key)` of a C implementation: a list built with
/// `kafka_List_new` of handles the C side owns and keeps valid until its
/// next mutation; the Rust side frees the list.
pub type kafka_common_header_Headers_headers_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, key: *const c_char) -> *mut kafka_List_t;

/// `toArray()` of a C implementation: a list as for
/// [`kafka_common_header_Headers_headers_fn_t`]; the Rust side copies its
/// elements on the first call after each mutation.
pub type kafka_common_header_Headers_to_array_fn_t = unsafe extern "C" fn(self_: *mut c_void) -> *mut kafka_List_t;

/// `iterator()` of a C implementation, standing for the Rust-only `iter()`:
/// a list as for [`kafka_common_header_Headers_to_array_fn_t`].
pub type kafka_common_header_Headers_iter_fn_t = unsafe extern "C" fn(self_: *mut c_void) -> *mut kafka_List_t;

/// A C implementation of [`Headers`] registered through
/// [`kafka_common_header_Headers_new`].
struct CHeaders {
    self_: *mut c_void,
    add_with_header: kafka_common_header_Headers_add_with_header_fn_t,
    add_with_key_value: kafka_common_header_Headers_add_with_key_value_fn_t,
    remove: kafka_common_header_Headers_remove_fn_t,
    last_header: kafka_common_header_Headers_last_header_fn_t,
    headers: kafka_common_header_Headers_headers_fn_t,
    to_array: kafka_common_header_Headers_to_array_fn_t,
    iter: kafka_common_header_Headers_iter_fn_t,
    /// The copy of the C side's headers that `to_array` / `iter` borrow
    /// out, taken on the first call after the last mutation through this
    /// handle.
    array: OnceLock<Vec<RecordHeader>>,
}

impl CHeaders {
    /// Reads a list of borrowed handles the C side returned and frees the
    /// list.
    unsafe fn borrowed_headers<'a>(list: *mut kafka_List_t) -> Vec<&'a RecordHeader> {
        let headers = unsafe { list_elements(list) }
            .iter()
            .map(|&element| unsafe {
                record_header_ref(element as *const kafka_common_header_internals_RecordHeader_t)
            })
            .collect();
        unsafe { kafka_List_destroy(list) };
        headers
    }

    fn array(&self, fetch: kafka_common_header_Headers_to_array_fn_t) -> &[RecordHeader] {
        self.array.get_or_init(|| {
            unsafe { Self::borrowed_headers(fetch(self.self_)) }
                .into_iter()
                .cloned()
                .collect()
        })
    }
}

impl Headers for CHeaders {
    fn add_with_header(&mut self, header: RecordHeader) -> Result<(), LocalIllegalStateError> {
        self.array.take();
        let header = RecordHeaderInner::new(header);
        unsafe { illegal_state((self.add_with_header)(self.self_, header.as_ptr())) }
    }

    fn add_with_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), LocalIllegalStateError> {
        self.array.take();
        let key = owned_c_string(key);
        unsafe {
            illegal_state((self.add_with_key_value)(
                self.self_,
                key.as_ptr(),
                kafka_Bytes_t::from_option(value),
            ))
        }
    }

    fn remove(&mut self, key: &str) -> Result<(), LocalIllegalStateError> {
        self.array.take();
        let key = owned_c_string(key);
        unsafe { illegal_state((self.remove)(self.self_, key.as_ptr())) }
    }

    fn last_header(&self, key: &str) -> Option<&RecordHeader> {
        let key = owned_c_string(key);
        let header = unsafe { (self.last_header)(self.self_, key.as_ptr()) };
        if header.is_null() {
            None
        } else {
            Some(unsafe { record_header_ref(header) })
        }
    }

    fn headers(&self, key: &str) -> Vec<&RecordHeader> {
        let key = owned_c_string(key);
        unsafe { Self::borrowed_headers((self.headers)(self.self_, key.as_ptr())) }
    }

    fn to_array(&self) -> &[RecordHeader] {
        self.array(self.to_array)
    }

    fn iter(&self) -> std::slice::Iter<'_, RecordHeader> {
        self.array(self.iter).iter()
    }
}

/// Registers a C implementation of `Headers`. The caller owns `self_` and
/// keeps it alive until the returned handle is destroyed with
/// [`kafka_common_header_Headers_destroy`]. The handles the C functions
/// return are borrowed from the C side and must stay valid until its next
/// mutating call; the lists are built with `kafka_List_new` and freed by the
/// Rust side.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_header_Headers_new(
    self_: *mut c_void,
    add_with_header: kafka_common_header_Headers_add_with_header_fn_t,
    add_with_key_value: kafka_common_header_Headers_add_with_key_value_fn_t,
    remove: kafka_common_header_Headers_remove_fn_t,
    last_header: kafka_common_header_Headers_last_header_fn_t,
    headers: kafka_common_header_Headers_headers_fn_t,
    to_array: kafka_common_header_Headers_to_array_fn_t,
    iter: kafka_common_header_Headers_iter_fn_t,
) -> *mut kafka_common_header_Headers_t {
    Box::into_raw(Box::new(HeadersInner::owned(CHeaders {
        self_,
        add_with_header,
        add_with_key_value,
        remove,
        last_header,
        headers,
        to_array,
        iter,
        array: OnceLock::new(),
    }))) as *mut kafka_common_header_Headers_t
}

/// Frees a handle built by [`kafka_common_header_Headers_new`]; null is a
/// no-op. The view a class handle hands out (`RecordHeaders__as_Headers`) is
/// never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned headers handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_destroy(self_: *mut kafka_common_header_Headers_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut HeadersInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::header::RecordHeaders;
    use crate::ffi::common::header::internals::record_header::{
        kafka_common_header_internals_RecordHeader_destroy, kafka_common_header_internals_RecordHeader_new,
    };
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders__as_Headers,
        kafka_common_header_internals_RecordHeaders_destroy, kafka_common_header_internals_RecordHeaders_set_read_only,
        record_headers_ref,
    };
    use crate::ffi::common::kafka_common_Error_destroy;
    use crate::ffi::error_predicates::kafka_common_Error_is_local_illegal_state_error;
    use crate::ffi::util::{kafka_List_get, kafka_List_size};

    unsafe fn keys(list: *mut kafka_List_t) -> Vec<String> {
        let keys = (0..unsafe { kafka_List_size(list) })
            .map(|i| {
                unsafe {
                    record_header_ref(kafka_List_get(list, i) as *const kafka_common_header_internals_RecordHeader_t)
                }
                .key()
                .to_string()
            })
            .collect();
        unsafe { kafka_List_destroy(list) };
        keys
    }

    #[test]
    fn view_getters_follow_java_and_survive_mutation() {
        let a = CString::new("a").unwrap();
        let b = CString::new("b").unwrap();
        let missing = CString::new("missing").unwrap();
        let class = box_record_headers(RecordHeaders::with_header_iter([
            RecordHeader::new("a".to_string(), Some(vec![1])),
            RecordHeader::new("b".to_string(), None),
            RecordHeader::new("a".to_string(), Some(vec![2])),
        ]));
        unsafe {
            let view = kafka_common_header_internals_RecordHeaders__as_Headers(class);
            assert_eq!(keys(kafka_common_header_Headers_to_array(view)), ["a", "b", "a"]);
            assert_eq!(keys(kafka_common_header_Headers_iter(view)), ["a", "b", "a"]);
            assert_eq!(keys(kafka_common_header_Headers_headers(view, a.as_ptr())), ["a", "a"]);
            assert_eq!(
                keys(kafka_common_header_Headers_headers(view, missing.as_ptr())),
                Vec::<String>::new()
            );

            // `lastHeader` is the last one added, and the borrowed handle is
            // the same one `toArray` lists.
            let last = kafka_common_header_Headers_last_header(view, a.as_ptr());
            assert_eq!(record_header_ref(last).value(), Some(&[2_u8][..]));
            let array = kafka_common_header_Headers_to_array(view);
            assert_eq!(
                kafka_List_get(array, 2) as *const kafka_common_header_internals_RecordHeader_t,
                last
            );
            kafka_List_destroy(array);
            assert!(kafka_common_header_Headers_last_header(view, missing.as_ptr()).is_null());

            // Mutations rebuild the borrowed handles.
            assert!(kafka_common_header_Headers_remove(view, a.as_ptr()).is_null());
            assert_eq!(keys(kafka_common_header_Headers_to_array(view)), ["b"]);
            let header = kafka_common_header_internals_RecordHeader_new(b.as_ptr(), kafka_Bytes_t::from_slice(&[3]));
            assert!(kafka_common_header_Headers_add_with_header(view, header).is_null());
            kafka_common_header_internals_RecordHeader_destroy(header);
            assert_eq!(keys(kafka_common_header_Headers_to_array(view)), ["b", "b"]);
            assert_eq!(
                record_header_ref(kafka_common_header_Headers_last_header(view, b.as_ptr())).value(),
                Some(&[3_u8][..])
            );
            assert_eq!(record_headers_ref(class).to_array().len(), 2);
            kafka_common_header_internals_RecordHeaders_destroy(class);
        }
    }

    // A C implementation of `Headers` for the tests: `self_` is a borrowed
    // `kafka_common_header_Headers_t` view on a `RecordHeaders`, and every
    // method forwards to the view's invoker.
    unsafe extern "C" fn c_add_with_header(
        self_: *mut c_void,
        header: *const kafka_common_header_internals_RecordHeader_t,
    ) -> *mut kafka_common_Error_t {
        unsafe { kafka_common_header_Headers_add_with_header(self_ as *mut kafka_common_header_Headers_t, header) }
    }
    unsafe extern "C" fn c_add_with_key_value(
        self_: *mut c_void,
        key: *const c_char,
        value: kafka_Bytes_t,
    ) -> *mut kafka_common_Error_t {
        unsafe {
            kafka_common_header_Headers_add_with_key_value(self_ as *mut kafka_common_header_Headers_t, key, value)
        }
    }
    unsafe extern "C" fn c_remove(self_: *mut c_void, key: *const c_char) -> *mut kafka_common_Error_t {
        unsafe { kafka_common_header_Headers_remove(self_ as *mut kafka_common_header_Headers_t, key) }
    }
    unsafe extern "C" fn c_last_header(
        self_: *mut c_void,
        key: *const c_char,
    ) -> *const kafka_common_header_internals_RecordHeader_t {
        unsafe { kafka_common_header_Headers_last_header(self_ as *const kafka_common_header_Headers_t, key) }
    }
    unsafe extern "C" fn c_headers(self_: *mut c_void, key: *const c_char) -> *mut kafka_List_t {
        unsafe { kafka_common_header_Headers_headers(self_ as *const kafka_common_header_Headers_t, key) }
    }
    unsafe extern "C" fn c_to_array(self_: *mut c_void) -> *mut kafka_List_t {
        unsafe { kafka_common_header_Headers_to_array(self_ as *const kafka_common_header_Headers_t) }
    }
    unsafe extern "C" fn c_iter(self_: *mut c_void) -> *mut kafka_List_t {
        unsafe { kafka_common_header_Headers_iter(self_ as *const kafka_common_header_Headers_t) }
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait() {
        let a = CString::new("a").unwrap();
        let b = CString::new("b").unwrap();
        let class = box_record_headers(RecordHeaders::new());
        unsafe {
            let backing = kafka_common_header_internals_RecordHeaders__as_Headers(class);
            let c = kafka_common_header_Headers_new(
                backing as *mut c_void,
                c_add_with_header,
                c_add_with_key_value,
                c_remove,
                c_last_header,
                c_headers,
                c_to_array,
                c_iter,
            );
            // Rust drives the C implementation through `Headers`.
            let headers: &mut dyn Headers = headers_mut(c);
            headers.add_with_key_value("a", Some(&[1])).unwrap();
            headers.add_with_header(RecordHeader::new("b".to_string(), None)).unwrap();
            headers.add_with_key_value("a", Some(&[2])).unwrap();
            assert_eq!(headers.to_array().len(), 3);
            assert_eq!(headers.iter().map(Header::key).collect::<Vec<_>>(), ["a", "b", "a"]);
            assert_eq!(headers.last_header("a").and_then(Header::value), Some(&[2_u8][..]));
            assert!(headers.last_header("missing").is_none());
            assert_eq!(headers.headers("a").len(), 2);
            headers.remove("a").unwrap();
            // The snapshot is refreshed after a mutation.
            assert_eq!(headers.to_array().len(), 1);
            assert_eq!(record_headers_ref(class).to_array().len(), 1);

            // The C-side invokers on the owned handle see the same state.
            assert_eq!(keys(kafka_common_header_Headers_to_array(c)), ["b"]);
            assert!(kafka_common_header_Headers_add_with_key_value(c, a.as_ptr(), kafka_Bytes_t::NULL).is_null());
            assert_eq!(keys(kafka_common_header_Headers_headers(c, b.as_ptr())), ["b"]);

            // A read-only backing surfaces as the trait's error, through C
            // and back.
            kafka_common_header_internals_RecordHeaders_set_read_only(class);
            let error = headers_mut(c).remove("b").unwrap_err();
            assert_eq!(
                error.to_string(),
                LocalIllegalStateError::new("RecordHeaders has been closed.").to_string()
            );
            let error = kafka_common_header_Headers_remove(c, b.as_ptr());
            assert_eq!(kafka_common_Error_is_local_illegal_state_error(error), 1);
            assert_eq!(
                CStr::from_ptr(crate::ffi::common::kafka_common_Error_message(error))
                    .to_str()
                    .unwrap(),
                "RecordHeaders has been closed."
            );
            kafka_common_Error_destroy(error);

            kafka_common_header_Headers_destroy(c);
            kafka_common_header_Headers_destroy(ptr::null_mut());
            kafka_common_header_internals_RecordHeaders_destroy(class);
        }
    }

    #[test]
    fn illegal_state_keeps_the_class_or_the_message() {
        unsafe {
            assert!(illegal_state(ptr::null_mut()).is_ok());
            let error =
                illegal_state(box_error(Error::LocalIllegalState(LocalIllegalStateError::new("closed")))).unwrap_err();
            assert_eq!(error.message(), "closed");
            let error = illegal_state(box_error(Error::kafka_message("other"))).unwrap_err();
            assert_eq!(error.message(), "other");
        }
    }
}

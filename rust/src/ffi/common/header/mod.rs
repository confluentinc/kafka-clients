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
use crate::ffi::ffi_guard;
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
    header: *const (dyn Header + Send + Sync),
    key_c: CString,
}

impl HeaderInner {
    /// A view on `header`, whose owner must outlive the view.
    ///
    /// # Safety
    ///
    /// `header` must point at a live implementation that outlives the view
    /// and is not mutated while the view exists.
    pub(crate) unsafe fn new(header: *const (dyn Header + Send + Sync)) -> Self {
        // SAFETY: Per this function's `# Safety`, `header` points at a live implementation; the
        // reference is used only to copy its key.
        let key_c = owned_c_string(unsafe { &*header }.key());
        Self { header, key_c }
    }

    fn header(&self) -> &dyn Header {
        // SAFETY: `self.header` pointed at a live implementation when `HeaderInner::new` built this
        // view, and per that function's `# Safety` the implementation outlives the view and is not
        // mutated while it exists, so the shared reference, bounded by `&self`, is sound.
        unsafe { &*self.header }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_header_Header_t {
        self as *const Self as *const kafka_common_header_Header_t
    }
}

// SAFETY: `header` points at a `dyn Header + Send + Sync` that, per `HeaderInner::new`'s
// `# Safety`, outlives the view and is not mutated while it exists, so sending the view to
// another thread is as sound as sending a shared reference to it; `key_c` is a `CString`, which
// is `Send`.
unsafe impl Send for HeaderInner {}
// SAFETY: As for `Send`: the pointee is `Sync` and outlives the view, so sharing the view shares
// only a shared reference to it, and `key_c` is a `CString`, which is `Sync`; the view has no
// interior mutability.
unsafe impl Sync for HeaderInner {}

/// `key()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid header handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Header_key(self_: *const kafka_common_header_Header_t) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid header handle, so it points at a
    // live `HeaderInner`: the view `RecordHeaderInner::as_header_ptr` caches in a record-header
    // handle, the only place one is built, which lives as long as that handle. No code mutates a
    // `HeaderInner` once it is built, so the shared reference is sound; the key returned is the
    // view's own C string, so it lives as long as the handle, as the rustdoc states.
    unsafe { &*(self_ as *const HeaderInner) }.key_c.as_ptr()
}

/// `value()`: a view over the header's bytes, valid as long as the handle;
/// `data` is null for Java's null value.
///
/// # Safety
///
/// `self_` must be a valid header handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Header_value(self_: *const kafka_common_header_Header_t) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid header handle, so it points at a
    // live `HeaderInner`: the view `RecordHeaderInner::as_header_ptr` caches in a record-header
    // handle, the only place one is built, which lives as long as that handle. No code mutates a
    // `HeaderInner` once it is built, so the shared reference is sound; the bytes returned are the
    // value of the `RecordHeader` the view points at, so they live as long as the handle, as the
    // rustdoc states.
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

    /// The implementation.
    pub(crate) fn headers(&self) -> &dyn Headers {
        match &self.headers {
            // SAFETY: A borrowed `HeadersImpl` points at an implementation that outlives this view,
            // as `HeadersInner::borrowed`'s rustdoc requires of its caller: the `RecordHeaders` of
            // the handle that caches this view (`RecordHeadersInner::headers_view`), or the
            // `&dyn Headers` a deserializer call holds for the whole life of its call-local view.
            // The implementation is mutated only through `headers_mut`, which takes `&mut self`, or
            // by `kafka_common_header_internals_RecordHeaders_set_read_only`, which requires that
            // no other call use the handle at the same time, so no mutation overlaps this shared
            // reference, bounded by `&self`.
            HeadersImpl::Borrowed(headers) => unsafe { &**headers },
            HeadersImpl::Owned(headers) => &**headers,
        }
    }

    /// Mutable access, dropping the views the mutation would invalidate.
    fn headers_mut(&mut self) -> &mut dyn Headers {
        self.views.take();
        match &mut self.headers {
            // SAFETY: As in `headers`, a borrowed `HeadersImpl` points at an implementation that
            // outlives this view. The mutating callers reach it only through a handle whose headers
            // may be mutated and that no other call uses at the same time (the `# Safety` of the
            // free `headers_mut`), so the exclusive reference, bounded by `&mut self`, aliases
            // nothing. A deserializer's call-local view is not such a handle: its C callee receives
            // it `const`.
            HeadersImpl::Borrowed(headers) => unsafe { &mut **headers },
            HeadersImpl::Owned(headers) => &mut **headers,
        }
    }

    fn views(&self) -> &[RecordHeaderInner] {
        self.views
            .get_or_init(|| self.headers().to_array().iter().cloned().map(RecordHeaderInner::new).collect())
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_header_Headers_t {
        self as *const Self as *const kafka_common_header_Headers_t
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_mut_ptr(&mut self) -> *mut kafka_common_header_Headers_t {
        self as *mut Self as *mut kafka_common_header_Headers_t
    }
}

// SAFETY: a `HeadersInner` holds one of three implementations. A view from
// `RecordHeadersInner::headers_view` points at the `RecordHeaders` (`Send + Sync`) of the handle
// that caches it, so it crosses threads with that handle. A view from the deserializer's
// `headers_view` points at the `&dyn Headers` a `Deserializer` call was given; it is a local of
// that call that the Rust side never sends to another task, and a C callee that passes it to
// another thread during the call does so under its own synchronisation. The owned `CHeaders`
// holds the `self_` and function pointers the C caller registered: the C user is responsible for
// the thread-safety of `self_`, and the function pointers are trivially shareable. `views` holds
// `RecordHeaderInner`s, which are `Send + Sync`. The deserializer case rests on that call-local
// use, not on a type bound.
unsafe impl Send for HeadersInner {}
// SAFETY: As for `Send`: through a shared `HeadersInner` the Rust side only reads the
// implementation and fills `views` through `OnceLock`, which is `Sync`; the `RecordHeaders` and
// `RecordHeaderInner` it reaches are `Sync`, the deserializer's view is never shared across tasks
// by the Rust side, and the C user is responsible for the thread-safety of a C implementation's
// `self_`.
unsafe impl Sync for HeadersInner {}

/// Reads a C implementation's error slot back into the trait's error type:
/// the translation of `IllegalStateException` passes through, any other
/// class keeps only its message.
///
/// # Safety
///
/// `error` must be null or an owned error handle not yet destroyed, as a C
/// implementation returns; it is consumed.
unsafe fn illegal_state(error: *mut kafka_common_Error_t) -> Result<(), LocalIllegalStateError> {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error handle not yet
    // destroyed, as `take_error` requires; it is consumed here, and the caller does not use it
    // again.
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

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid headers handle, alive and not mutated for `'a`.
pub(crate) unsafe fn headers_ref<'a>(self_: *const kafka_common_header_Headers_t) -> &'a HeadersInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle, alive and not
    // mutated for `'a`, so it points at a live `HeadersInner`: the `Box::into_raw` of
    // `kafka_common_header_Headers_new`, the view a record-headers handle caches
    // (`RecordHeadersInner::headers_view`), which lives as long as that handle, or a deserializer's
    // call-local view. The shared reference is therefore sound for `'a`.
    unsafe { &*(self_ as *const HeadersInner) }
}

/// Mutable access to the implementation behind a pointer, dropping the views
/// the mutation would invalidate.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated, alive
/// for `'a` and reached by no other reference during it.
unsafe fn headers_mut<'a>(self_: *mut kafka_common_header_Headers_t) -> &'a mut dyn Headers {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated, alive for `'a` and reached by no other reference during it, so it points at a
    // live `HeadersInner` (one of the kinds `headers_ref` lists) and the exclusive reference
    // aliases nothing for `'a`.
    unsafe { &mut *(self_ as *mut HeadersInner) }.headers_mut()
}

/// `add(Header header)`: appends a copy of `header`. Returns the translation
/// of `IllegalStateException` when the headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated (not a
/// view on a handle the caller received `const`, nor the `const` view a
/// deserializer receives), that no other call uses at the same time, and
/// `header` a valid record-header handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_add_with_header(
    self_: *mut kafka_common_header_Headers_t,
    header: *const kafka_common_header_internals_RecordHeader_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `header` is a valid record-header handle, as
    // `record_header_ref` requires. It is cloned before `headers_mut` below drops this handle's
    // cached views, so the borrow ends before a `header` taken from those views is freed.
    let header = unsafe { record_header_ref(header) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated and that no other call uses at the same time, as `headers_mut` requires; the
    // reference is used only within this call.
    result(unsafe { headers_mut(self_) }.add_with_header(header))
}

/// `add(String key, byte[] value)`: appends a header built from copies of
/// `key` and `value` (`value.data == NULL` is Java's null). Returns the
/// translation of `IllegalStateException` when the headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated (not a
/// view on a handle the caller received `const`, nor the `const` view a
/// deserializer receives), that no other call uses at the same time, `key` a
/// valid NUL-terminated string and `value` null or a valid buffer.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_add_with_key_value(
    self_: *mut kafka_common_header_Headers_t,
    key: *const c_char,
    value: kafka_Bytes_t,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let key = unsafe { c_str_to_string(key) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated and that no other call uses at the same time, as `headers_mut` requires, and
    // `value` is null or a valid buffer, so `value.data` is null or valid for `value.len` bytes, as
    // `kafka_Bytes_t::as_slice` requires; both borrows are used only within this call, and the
    // implementation copies the bytes.
    result(unsafe { headers_mut(self_) }.add_with_key_value(&key, unsafe { value.as_slice() }))
}

/// `remove(String key)`: drops every header with `key`, keeping the order of
/// the rest. Returns the translation of `IllegalStateException` when the
/// headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated (not a
/// view on a handle the caller received `const`, nor the `const` view a
/// deserializer receives), that no other call uses at the same time, and
/// `key` a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_remove(
    self_: *mut kafka_common_header_Headers_t,
    key: *const c_char,
) -> *mut kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let key = unsafe { c_str_to_string(key) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated and that no other call uses at the same time, as `headers_mut` requires; the
    // reference is used only within this call.
    result(unsafe { headers_mut(self_) }.remove(&key))
}

/// `lastHeader(String key)`: the last header with `key`, borrowed from the
/// handle until its next mutation, or null.
///
/// # Safety
///
/// `self_` must be a valid headers handle and `key` a valid NUL-terminated
/// string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_last_header(
    self_: *const kafka_common_header_Headers_t,
    key: *const c_char,
) -> *const kafka_common_header_internals_RecordHeader_t {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let key = unsafe { c_str_to_string(key) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle, as `headers_ref`
    // requires; it is not mutated during this call, because the mutating `Headers_*` functions
    // require that no other call use the handle at the same time. The handle returned is an element
    // of the handle's cached `views`, so it lives until the handle's next mutation, as the rustdoc
    // states.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_headers(
    self_: *const kafka_common_header_Headers_t,
    key: *const c_char,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let key = unsafe { c_str_to_string(key) };
    view_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle, as
        // `headers_ref` requires; it is not mutated during this call, because the mutating
        // `Headers_*` functions require that no other call use the handle at the same time. The
        // list returned is owned by the caller and its elements are the handle's cached `views`,
        // which live until the handle's next mutation, as the module docs state.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_to_array(
    self_: *const kafka_common_header_Headers_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle, as `headers_ref`
    // requires; it is not mutated during this call, because the mutating `Headers_*` functions
    // require that no other call use the handle at the same time. The list returned is owned by the
    // caller and its elements are the handle's cached `views`, which live until the handle's next
    // mutation, as the module docs state.
    view_list(unsafe { headers_ref(self_) }.views().iter())
}

/// `iterator()`: the same list as [`kafka_common_header_Headers_to_array`],
/// standing for the Rust-only `iter()`.
///
/// # Safety
///
/// `self_` must be a valid headers handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_iter(
    self_: *const kafka_common_header_Headers_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle, as `headers_ref`
    // requires; it is not mutated during this call, because the mutating `Headers_*` functions
    // require that no other call use the handle at the same time. The list returned is owned by the
    // caller and its elements are the handle's cached `views`, which live until the handle's next
    // mutation, as the module docs state.
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
    ///
    /// # Safety
    ///
    /// `list` must be null or a valid list handle not yet destroyed, which
    /// this call takes over, whose elements are record-header handles alive
    /// and not mutated for `'a`.
    unsafe fn borrowed_headers<'a>(list: *mut kafka_List_t) -> Vec<&'a RecordHeader> {
        // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle that this
        // call takes over, as `list_elements` requires; the slice is used only until the list is
        // destroyed below, and nothing else mutates the list.
        let headers = unsafe { list_elements(list) }
            .iter()
            // SAFETY: Per this function's `# Safety`, every element of `list` is a record-header
            // handle alive and not mutated for `'a`, as `record_header_ref` requires.
            .map(|&element| unsafe {
                record_header_ref(element as *const kafka_common_header_internals_RecordHeader_t)
            })
            .collect();
        // SAFETY: Per this function's `# Safety`, `list` is null or a valid list handle not yet
        // destroyed that this call takes over, as `kafka_List_destroy` requires, and the slice read
        // from it is no longer used. A list from `kafka_List_new` does not own its elements, so
        // destroying it leaves the record headers alone.
        unsafe { kafka_List_destroy(list) };
        headers
    }

    fn array(&self, fetch: kafka_common_header_Headers_to_array_fn_t) -> &[RecordHeader] {
        self.array.get_or_init(|| {
            // SAFETY: `fetch` and `self_` were registered together through
            // `kafka_common_header_Headers_new`, whose rustdoc requires the C side to keep `self_`
            // alive until this handle is destroyed (`fetch` is its `to_array` or `iter`). The list
            // returned is built with `kafka_List_new` and handed over, and its elements stay valid
            // until the C side's next mutating call, as `borrowed_headers` requires; they are
            // cloned before this returns, so the snapshot borrows nothing.
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
        // SAFETY: `add_with_header` and `self_` were registered together through
        // `kafka_common_header_Headers_new`, whose rustdoc requires the C side to keep `self_`
        // alive until this handle is destroyed. `header` is a local record-header handle that
        // outlives the call, which borrows and copies it, as the `_fn_t` rustdoc states; the result
        // is null or an owned error, as `illegal_state` requires.
        unsafe { illegal_state((self.add_with_header)(self.self_, header.as_ptr())) }
    }

    fn add_with_key_value(&mut self, key: &str, value: Option<&[u8]>) -> Result<(), LocalIllegalStateError> {
        self.array.take();
        let key = owned_c_string(key);
        // SAFETY: `add_with_key_value` and `self_` were registered together through
        // `kafka_common_header_Headers_new`, whose rustdoc requires the C side to keep `self_`
        // alive until this handle is destroyed. `key` and the bytes `value` views outlive the call,
        // which borrows them; the result is null or an owned error, as `illegal_state` requires.
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
        // SAFETY: `remove` and `self_` were registered together through
        // `kafka_common_header_Headers_new`, whose rustdoc requires the C side to keep `self_`
        // alive until this handle is destroyed. `key` outlives the call, which borrows it; the
        // result is null or an owned error, as `illegal_state` requires.
        unsafe { illegal_state((self.remove)(self.self_, key.as_ptr())) }
    }

    fn last_header(&self, key: &str) -> Option<&RecordHeader> {
        let key = owned_c_string(key);
        // SAFETY: `last_header` and `self_` were registered together through
        // `kafka_common_header_Headers_new`, whose rustdoc requires the C side to keep `self_`
        // alive until this handle is destroyed. `key` outlives the call, which borrows it; the
        // handle returned is null or one the C side owns and keeps valid until its next mutating
        // call, as the `_fn_t` rustdoc states.
        let header = unsafe { (self.last_header)(self.self_, key.as_ptr()) };
        if header.is_null() {
            None
        } else {
            // SAFETY: `header` is non-null (checked above) and a record-header handle the C side
            // keeps valid until its next mutating call through this handle, as
            // `kafka_common_header_Headers_new`'s rustdoc requires; those calls take `&mut self`,
            // so none happens while the reference, bounded by `&self`, lives.
            Some(unsafe { record_header_ref(header) })
        }
    }

    fn headers(&self, key: &str) -> Vec<&RecordHeader> {
        let key = owned_c_string(key);
        // SAFETY: `headers` and `self_` were registered together through
        // `kafka_common_header_Headers_new`, whose rustdoc requires the C side to keep `self_`
        // alive until this handle is destroyed. `key` outlives the call, which borrows it. The list
        // returned is built with `kafka_List_new` and handed over, and its elements stay valid
        // until the C side's next mutating call, which takes `&mut self`, so they outlive the
        // references `borrowed_headers` returns, bounded by `&self`, as it requires.
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
#[ffi_guard]
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_destroy(self_: *mut kafka_common_header_Headers_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of a `HeadersInner` in
        // `kafka_common_header_Headers_new` (a view, which points into its parent, is never passed
        // here), so `from_raw` reclaims that allocation, once.
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
        // SAFETY: Test code: callers pass an owned list of record-header handles, not yet
        // destroyed, that a `Headers` getter returned; it is read here before it is destroyed
        // below.
        let keys = (0..unsafe { kafka_List_size(list) })
            .map(|i| {
                // SAFETY: Test code: each element of the list is a record-header handle from the
                // cached views of a headers handle, alive until that handle is next mutated, which
                // happens after this helper returns.
                unsafe {
                    record_header_ref(kafka_List_get(list, i) as *const kafka_common_header_internals_RecordHeader_t)
                }
                .key()
                .to_string()
            })
            .collect();
        // SAFETY: Test code: the list is owned and destroyed once, after its elements are read; it
        // does not own the elements, so they are left alone.
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
        // SAFETY: Test code: `class` comes from `box_record_headers` and is destroyed once, at the
        // end; `view` is its cached `Headers` view, used only while it lives; each list is owned
        // and destroyed once, after it is read, and every record-header reference read from a
        // getter is used before the next mutation; `a`, `b` and `missing` outlive the calls reading
        // them; `header` comes from `kafka_common_header_internals_RecordHeader_new` and is
        // destroyed once, after `add_with_header` copies it.
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
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe { kafka_common_header_Headers_add_with_header(self_ as *mut kafka_common_header_Headers_t, header) }
    }
    unsafe extern "C" fn c_add_with_key_value(
        self_: *mut c_void,
        key: *const c_char,
        value: kafka_Bytes_t,
    ) -> *mut kafka_common_Error_t {
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe {
            kafka_common_header_Headers_add_with_key_value(self_ as *mut kafka_common_header_Headers_t, key, value)
        }
    }
    unsafe extern "C" fn c_remove(self_: *mut c_void, key: *const c_char) -> *mut kafka_common_Error_t {
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe { kafka_common_header_Headers_remove(self_ as *mut kafka_common_header_Headers_t, key) }
    }
    unsafe extern "C" fn c_last_header(
        self_: *mut c_void,
        key: *const c_char,
    ) -> *const kafka_common_header_internals_RecordHeader_t {
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe { kafka_common_header_Headers_last_header(self_ as *const kafka_common_header_Headers_t, key) }
    }
    unsafe extern "C" fn c_headers(self_: *mut c_void, key: *const c_char) -> *mut kafka_List_t {
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe { kafka_common_header_Headers_headers(self_ as *const kafka_common_header_Headers_t, key) }
    }
    unsafe extern "C" fn c_to_array(self_: *mut c_void) -> *mut kafka_List_t {
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe { kafka_common_header_Headers_to_array(self_ as *const kafka_common_header_Headers_t) }
    }
    unsafe extern "C" fn c_iter(self_: *mut c_void) -> *mut kafka_List_t {
        // SAFETY: Test code: `self_` is the `Headers_t` view this test registered as the C
        // implementation's state, alive for the whole test, and the other arguments are what the
        // caller passes; they are forwarded unchanged to the invoker, whose `# Safety` they meet.
        unsafe { kafka_common_header_Headers_iter(self_ as *const kafka_common_header_Headers_t) }
    }

    #[test]
    fn c_implementation_is_driven_through_the_trait() {
        let a = CString::new("a").unwrap();
        let b = CString::new("b").unwrap();
        let class = box_record_headers(RecordHeaders::new());
        // SAFETY: Test code: `class` comes from `box_record_headers` and is destroyed once, last;
        // `backing` is its cached `Headers` view, registered as `c`'s state and alive until `class`
        // is destroyed, after `c`; `c` comes from `kafka_common_header_Headers_new` and is
        // destroyed once; the exclusive reference `headers` is last used before the C-side invokers
        // touch `c`; each list is owned and destroyed once, after it is read; `a` and `b` outlive
        // the calls reading them; the error is destroyed once, after it is read.
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
        // SAFETY: Test code: null is accepted, and each error handle comes from `box_error` and is
        // consumed by `illegal_state`.
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

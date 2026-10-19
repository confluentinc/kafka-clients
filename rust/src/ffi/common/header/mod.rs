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
//! A `Headers_t` is the borrowed view
//! `kafka_common_header_internals_RecordHeaders__as_Headers` returns, valid
//! until that class handle is destroyed. Like `Header_t` it has no `_new`:
//! the serde interfaces take the class handle,
//! `kafka_common_header_internals_RecordHeaders_t`, so nothing in the public
//! API accepts a caller-supplied `Headers`, and C only drives one.
//!
//! The getters that return headers (`last_header`, `headers`, `to_array`,
//! `iter`) hand out borrowed `kafka_common_header_internals_RecordHeader_t *`
//! handles the `Headers_t` owns, each over the header itself, so a value C
//! reads is the record's own buffer, not a copy. They stay valid until the next mutating call
//! on the same handle (`add_*`, `remove`), which rebuilds them, or until the
//! handle is destroyed; the lists holding them are owned by the caller and
//! freed with `kafka_List_destroy`, which leaves the elements alone.

pub(crate) mod internals;

use std::ffi::{CString, c_char, c_void};
use std::ptr;
use std::sync::OnceLock;

use crate::common::header::{Header, Headers};
use crate::common::{Error, LocalIllegalStateError};
use crate::ffi::common::deliver_unit;
use crate::ffi::common::header::internals::record_header::{
    RecordHeaderInner, kafka_common_header_internals_RecordHeader_t, record_header_ref,
};
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{box_list, c_str_to_string, kafka_Bytes_t, kafka_List_t, owned_c_string};

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

/// What a [`kafka_common_header_Headers_t`] points at: the implementation,
/// owned by the class handle that produced this view
/// (`RecordHeaders__as_Headers`), plus the header handles its getters hand
/// out.
pub(crate) struct HeadersInner {
    headers: *mut dyn Headers,
    /// The `RecordHeader_t` handles the getters return, borrowing the
    /// headers of `to_array()` in order, built on first request and dropped
    /// by each mutating call. The
    /// vector is never pushed to after it is built, so each element (and the
    /// `Header` view it caches, which points back into it) keeps its address.
    views: OnceLock<Vec<RecordHeaderInner>>,
}

impl HeadersInner {
    /// A view on `headers`, whose owner must outlive it.
    pub(crate) fn borrowed(headers: *mut dyn Headers) -> Self {
        Self { headers, views: OnceLock::new() }
    }

    /// The implementation.
    pub(crate) fn headers(&self) -> &dyn Headers {
        // SAFETY: `headers` points at an implementation that outlives this view, as
        // `HeadersInner::borrowed`'s rustdoc requires of its caller: the `RecordHeaders` of the
        // handle that caches this view (`RecordHeadersInner::headers_view`). The implementation is
        // mutated only through `headers_mut`, which takes `&mut self`, or by
        // `kafka_common_header_internals_RecordHeaders_set_read_only`, which requires that no other
        // call use the handle at the same time, so no mutation overlaps this shared reference,
        // bounded by `&self`.
        unsafe { &*self.headers }
    }

    /// Mutable access, dropping the views the mutation would invalidate.
    fn headers_mut(&mut self) -> &mut dyn Headers {
        self.views.take();
        // SAFETY: As in `headers`, `headers` points at an implementation that outlives this view.
        // The mutating callers reach it only through a handle whose headers may be mutated and that
        // no other call uses at the same time (the `# Safety` of the free `headers_mut`), so the
        // exclusive reference, bounded by `&mut self`, aliases nothing.
        unsafe { &mut *self.headers }
    }

    /// The header handles the getters hand out, built on first request over
    /// the headers themselves: C reads their own key and value buffers, never
    /// a copy.
    fn views(&self) -> &[RecordHeaderInner] {
        self.views.get_or_init(|| {
            self.headers()
                .to_array()
                .iter()
                // SAFETY: Each header lives in the implementation this view points at, which
                // outlives the view (`HeadersInner::borrowed`). The handles are dropped before the
                // headers can change: every mutation through this view goes through
                // `headers_mut`, which drops them first, and the other code that mutates the
                // implementation (`set_read_only`) leaves its headers untouched.
                .map(|header| unsafe { RecordHeaderInner::borrowed(header) })
                .collect()
        })
    }
}

// SAFETY: a `HeadersInner` built by `RecordHeadersInner::headers_view` points at the
// `RecordHeaders` (`Send + Sync`) of the handle that caches it, so it crosses threads with that
// handle. `views` holds `RecordHeaderInner`s, which are `Send + Sync`.
unsafe impl Send for HeadersInner {}
// SAFETY: As for `Send`: through a shared `HeadersInner` the Rust side only reads the
// implementation and fills `views` through `OnceLock`, which is `Sync`; the `RecordHeaders` and
// `RecordHeaderInner` it reaches are `Sync`.
unsafe impl Sync for HeadersInner {}

/// Delivers a mutating method's outcome: nothing on success, the owned
/// error in `*out_err` otherwise.
///
/// # Safety
///
/// `out_err` must be as `deliver_unit` requires.
unsafe fn result(result: Result<(), LocalIllegalStateError>, out_err: *mut *mut kafka_common_Error_t) {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe { deliver_unit(result.map_err(Error::LocalIllegalState), out_err) }
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
    // mutated for `'a`, so it points at a live `HeadersInner`: the view a record-headers handle
    // caches (`RecordHeadersInner::headers_view`), the only place one is built, which lives as long
    // as that handle. The shared reference is therefore sound for `'a`.
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
    // live `HeadersInner` (the view `headers_ref` describes) and the exclusive reference aliases
    // nothing for `'a`.
    unsafe { &mut *(self_ as *mut HeadersInner) }.headers_mut()
}

/// `add(Header header)`: appends a copy of `header`. Stores the translation
/// of `IllegalStateException` in `*out_err` when the headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated (not a
/// view on a handle the caller received `const`), that no other call uses at
/// the same time, and `header` a valid record-header handle. `out_err` must
/// be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_add_with_header(
    self_: *mut kafka_common_header_Headers_t,
    header: *const kafka_common_header_internals_RecordHeader_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `header` is a valid record-header handle, as
    // `record_header_ref` requires. It is cloned before `headers_mut` below drops this handle's
    // cached views, so the borrow ends before a `header` taken from those views is freed.
    let header = unsafe { record_header_ref(header) }.clone();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated and that no other call uses at the same time, as `headers_mut` requires; the
    // reference is used only within this call.
    let added = unsafe { headers_mut(self_) }.add_with_header(header);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `result` requires.
    unsafe { result(added, out_err) }
}

/// `add(String key, byte[] value)`: appends a header built from copies of
/// `key` and `value` (`value.data == NULL` is Java's null). Stores the
/// translation of `IllegalStateException` in `*out_err` when the headers are
/// read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated (not a
/// view on a handle the caller received `const`), that no other call uses at
/// the same time, `key` a valid NUL-terminated string and `value` null or a
/// valid buffer. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_add_with_key_value(
    self_: *mut kafka_common_header_Headers_t,
    key: *const c_char,
    value: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let key = unsafe { c_str_to_string(key) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated and that no other call uses at the same time, as `headers_mut` requires, and
    // `value` is null or a valid buffer, so `value.data` is null or valid for `value.len` bytes, as
    // `kafka_Bytes_t::as_slice` requires; both borrows are used only within this call, and the
    // implementation copies the bytes.
    let added = unsafe { headers_mut(self_) }.add_with_key_value(&key, unsafe { value.as_slice() });
    // SAFETY: Per this function's `# Safety`, `out_err` is as `result` requires.
    unsafe { result(added, out_err) }
}

/// `remove(String key)`: drops every header with `key`, keeping the order of
/// the rest. Stores the translation of `IllegalStateException` in
/// `*out_err` when the headers are read-only.
///
/// # Safety
///
/// `self_` must be a valid headers handle whose headers may be mutated (not a
/// view on a handle the caller received `const`), that no other call uses at
/// the same time, and `key` a valid NUL-terminated string. `out_err` must be a
/// valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_Headers_remove(
    self_: *mut kafka_common_header_Headers_t,
    key: *const c_char,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `key` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let key = unsafe { c_str_to_string(key) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid headers handle whose headers may
    // be mutated and that no other call uses at the same time, as `headers_mut` requires; the
    // reference is used only within this call.
    let removed = unsafe { headers_mut(self_) }.remove(&key);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `result` requires.
    unsafe { result(removed, out_err) }
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

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::header::{RecordHeader, RecordHeaders};
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
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size};

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
        // destroyed once, after `add_with_header` copies it; `err` is a valid slot pointing at
        // null, left null by every mutation (asserted).
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
            let mut err = ptr::null_mut();
            kafka_common_header_Headers_remove(view, a.as_ptr(), &mut err);
            assert!(err.is_null());
            assert_eq!(keys(kafka_common_header_Headers_to_array(view)), ["b"]);
            let header = kafka_common_header_internals_RecordHeader_new(b.as_ptr(), kafka_Bytes_t::from_slice(&[3]));
            kafka_common_header_Headers_add_with_header(view, header, &mut err);
            assert!(err.is_null());
            kafka_common_header_internals_RecordHeader_destroy(header);
            assert_eq!(keys(kafka_common_header_Headers_to_array(view)), ["b", "b"]);
            assert_eq!(
                record_header_ref(kafka_common_header_Headers_last_header(view, b.as_ptr())).value(),
                Some(&[3_u8][..])
            );
            assert_eq!(record_headers_ref(class).to_array().len(), 2);

            // A read-only class surfaces as the trait's error through the
            // view's mutating invokers.
            kafka_common_header_internals_RecordHeaders_set_read_only(class);
            kafka_common_header_Headers_remove(view, b.as_ptr(), &mut err);
            assert!(!err.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_state_error(err), 1);
            assert_eq!(
                CStr::from_ptr(crate::ffi::common::kafka_common_Error_message(err))
                    .to_str()
                    .unwrap(),
                "RecordHeaders has been closed."
            );
            kafka_common_Error_destroy(err);
            assert_eq!(record_headers_ref(class).to_array().len(), 2);

            kafka_common_header_internals_RecordHeaders_destroy(class);
        }
    }
}

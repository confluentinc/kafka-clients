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

use std::ffi::{c_char, c_void};
use std::sync::OnceLock;

use crate::common::header::{Headers, RecordHeaders};
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::common::header::internals::record_header::list_record_headers;
use crate::ffi::common::header::{HeadersInner, kafka_common_header_Headers_t};
use crate::ffi::ffi_guard;
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
    headers: HeadersStorage,
    /// The `Headers` view, a `Box::into_raw`'d `HeadersInner` freed by `Drop`.
    /// It is a separate allocation, never reborrowed by the handle, so a
    /// later `headers_view` call (which only reads this cell) leaves every
    /// pointer an earlier one handed out valid; the mutating `Headers_*`
    /// functions take `&mut HeadersInner` through it.
    view: OnceLock<SendPtr>,
}

/// Where a [`RecordHeadersInner`]'s headers live.
///
/// Has no Java counterpart (`definition-of-done.md` §7): Java passes the
/// record's `Headers` object itself to a serde; here a handle either owns its
/// headers or, for a serde call, points at the record's own (see
/// [`RecordHeadersInner::borrowed`]), so that C reads and changes those
/// rather than a copy.
enum HeadersStorage {
    Owned(RecordHeaders),
    /// Headers owned by the caller, which outlive the handle.
    Borrowed(*mut RecordHeaders),
}

// SAFETY: `Borrowed` points at a `RecordHeaders` (`Send + Sync`) that, per
// `RecordHeadersInner::borrowed`'s `# Safety`, outlives the handle and is reached only through it
// while it exists, so moving the handle to another thread moves the only access to it.
unsafe impl Send for HeadersStorage {}
// SAFETY: As for `Send`: through a shared handle the headers are only read, and they are
// `Sync`.
unsafe impl Sync for HeadersStorage {}

impl RecordHeadersInner {
    pub(crate) fn new(headers: RecordHeaders) -> Self {
        Self { headers: HeadersStorage::Owned(headers), view: OnceLock::new() }
    }

    /// A handle on the caller's `headers`, for the length of a serde call: C
    /// reads (and, through a `*mut` handle, changes) them in place, with no
    /// copy.
    ///
    /// # Safety
    ///
    /// `headers` must point at live headers that outlive the handle and are
    /// reached only through it while it exists; they may be written through
    /// it only if `headers` was derived from exclusive access.
    pub(crate) unsafe fn borrowed(headers: *mut RecordHeaders) -> Self {
        Self { headers: HeadersStorage::Borrowed(headers), view: OnceLock::new() }
    }

    /// A handle at a stable address, as the `Headers` view requires.
    pub(crate) fn boxed(headers: RecordHeaders) -> Box<Self> {
        Box::new(Self::new(headers))
    }

    pub(crate) fn headers(&self) -> &RecordHeaders {
        match &self.headers {
            HeadersStorage::Owned(headers) => headers,
            // SAFETY: Per `borrowed`'s `# Safety`, the headers outlive the handle and are reached
            // only through it, so the shared reference, bounded by `&self`, aliases no exclusive
            // one.
            HeadersStorage::Borrowed(headers) => unsafe { &**headers },
        }
    }

    /// Takes the owned headers out, leaving the handle empty; for a handle
    /// built by [`borrowed`](Self::borrowed), nothing to take, `None`. The
    /// `Headers` view, if any, then sees empty headers.
    pub(crate) fn take_headers(&mut self) -> Option<RecordHeaders> {
        match &mut self.headers {
            HeadersStorage::Owned(headers) => Some(std::mem::take(headers)),
            HeadersStorage::Borrowed(_) => None,
        }
    }

    /// The headers at `this`, as a raw pointer with `this`'s provenance.
    ///
    /// # Safety
    ///
    /// `this` must point at a live handle.
    unsafe fn headers_ptr(this: *mut Self) -> *mut RecordHeaders {
        // SAFETY: Per this function's `# Safety`, `this` points at a live `RecordHeadersInner`;
        // `&raw mut` takes the address of its `headers` field without creating a reference, and
        // reading the discriminant and the `Borrowed` pointer is a read through it, so no
        // reference to the field outlives this call.
        unsafe {
            let storage = &raw mut (*this).headers;
            match *storage {
                HeadersStorage::Owned(ref mut headers) => headers as *mut RecordHeaders,
                HeadersStorage::Borrowed(headers) => headers,
            }
        }
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_header_internals_RecordHeaders_t {
        self as *const Self as *const kafka_common_header_internals_RecordHeaders_t
    }

    /// A borrowed handle on `self` that C may mutate through, valid as long as
    /// `self` and while no other reference to `self` is in use.
    pub(crate) fn as_mut_ptr(&mut self) -> *mut kafka_common_header_internals_RecordHeaders_t {
        self as *mut Self as *mut kafka_common_header_internals_RecordHeaders_t
    }

    /// The `Headers` interface view on the handle at `this`, built on first
    /// request; `*mut` because the trait mutates. Works on the raw pointer so
    /// the view may later mutate the headers it points at.
    ///
    /// # Safety
    ///
    /// `this` must point at a live handle that does not move afterwards, and no
    /// other reference to its `view` field may be in use during the call.
    pub(crate) unsafe fn headers_view(this: *mut Self) -> *mut kafka_common_header_Headers_t {
        // SAFETY: Per this function's `# Safety`, `this` points at a live `RecordHeadersInner`, as
        // `headers_ptr` requires; the pointer keeps `this`'s provenance (or, for a borrowed handle,
        // the caller's), so the view built from it may later write through it.
        let headers = unsafe { Self::headers_ptr(this) } as *mut dyn Headers;
        // SAFETY: As above, `this` points at a live `RecordHeadersInner`, and per this function's
        // `# Safety` no other reference to its `view` field is in use during the call, so the shared
        // reference to that `OnceLock` (all `get_or_init` needs: its interior mutability) aliases no
        // exclusive one; it lasts for this statement only and is never exclusive, so it pops no
        // pointer an earlier call handed out. The `HeadersInner` it may build points at the
        // handle's `headers` field, which lives as long as the view, since `Drop` frees the view
        // with the handle, which per this function's `# Safety` does not move.
        let view = unsafe { &(*this).view }
            .get_or_init(|| SendPtr(Box::into_raw(Box::new(HeadersInner::borrowed(headers))) as *mut c_void));
        // The view is its own allocation (see the `view` field), so the pointer keeps the
        // provenance of the `Box` it came from: the mutating `Headers_*` functions may write
        // through it, and it lives as long as the handle.
        view.0 as *mut kafka_common_header_Headers_t
    }
}

impl Drop for RecordHeadersInner {
    fn drop(&mut self) {
        if let Some(view) = self.view.take() {
            // SAFETY: `view` is the `Box::into_raw` of the `HeadersInner` `headers_view` built, and
            // `take` hands it out once, so `from_raw` reclaims that allocation, once. A borrowed
            // view, never destroyed by C, is invalid once the handle is gone, as its rustdoc says.
            drop(unsafe { Box::from_raw(view.0 as *mut HeadersInner) });
        }
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
/// `headers` must be a valid record-headers handle, alive and not mutated for
/// `'a`.
pub(crate) unsafe fn record_headers_ref<'a>(
    headers: *const kafka_common_header_internals_RecordHeaders_t,
) -> &'a RecordHeaders {
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, alive and
    // not mutated for `'a`, so it points at a live `RecordHeadersInner`: the `Box::into_raw` of
    // `box_record_headers`, one a `ConsumerRecord`, `ProducerRecord` or
    // `RecordDeserializationError` handle embeds boxed and hands out through `as_ptr`, which lives
    // as long as that parent, or the call-local one a serializer or deserializer invoker passes
    // to C. The shared reference to its `headers` field is therefore sound for `'a`.
    unsafe { &*(headers as *const RecordHeadersInner) }.headers()
}

/// The headers behind a handle, mutable.
///
/// # Safety
///
/// `headers` must be a valid record-headers handle, alive for `'a`, with no
/// other reference to it in use for `'a`.
pub(crate) unsafe fn record_headers_mut<'a>(
    headers: *mut kafka_common_header_internals_RecordHeaders_t,
) -> &'a mut RecordHeaders {
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, so it
    // points at a live `RecordHeadersInner` (see `record_headers_ref`), as `headers_ptr` requires,
    // and no other reference to it is in use for `'a`, so the exclusive reference to its headers
    // aliases nothing.
    unsafe { &mut *RecordHeadersInner::headers_ptr(headers as *mut RecordHeadersInner) }
}

/// `new RecordHeaders()`: empty, mutable. Owned, freed with
/// [`kafka_common_header_internals_RecordHeaders_destroy`].
#[ffi_guard]
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_with_header_slice(
    headers: *const kafka_List_t,
) -> *mut kafka_common_header_internals_RecordHeaders_t {
    // SAFETY: Per this function's `# Safety`, `headers` is null or a valid list of record-header
    // handles, as `list_record_headers` requires; the headers are cloned into owned values, so
    // nothing borrows the list after this call.
    box_record_headers(RecordHeaders::with_header_slice(&unsafe { list_record_headers(headers) }))
}

/// `new RecordHeaders(Iterable<Header> headers)`: `headers` holds
/// `const kafka_common_header_internals_RecordHeader_t *`, copied.
///
/// # Safety
///
/// `headers` must be null or a valid list of record-header handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_with_header_iter(
    headers: *const kafka_List_t,
) -> *mut kafka_common_header_internals_RecordHeaders_t {
    // SAFETY: Per this function's `# Safety`, `headers` is null or a valid list of record-header
    // handles, as `list_record_headers` requires; the headers are cloned into owned values, so
    // nothing borrows the list after this call.
    box_record_headers(RecordHeaders::with_header_iter(unsafe { list_record_headers(headers) }))
}

/// `new RecordHeaders(RecordHeaders other)`: a mutable copy of `other`.
///
/// # Safety
///
/// `other` must be a valid record-headers handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_with_record_headers(
    other: *const kafka_common_header_internals_RecordHeaders_t,
) -> *mut kafka_common_header_internals_RecordHeaders_t {
    // SAFETY: Per this function's `# Safety`, `other` is a valid record-headers handle, as
    // `record_headers_ref` requires; the result is copied into an owned value before return, so the
    // borrow lasts for this call only.
    box_record_headers(RecordHeaders::with_record_headers(unsafe { record_headers_ref(other) }))
}

/// `setReadOnly()`: every later mutation through the `Headers` view fails
/// with the translation of `IllegalStateException`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle whose headers may be mutated
/// (not one a getter hands out `const`), that no other call uses at the same
/// time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_set_read_only(
    self_: *mut kafka_common_header_internals_RecordHeaders_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-headers handle whose
    // headers may be mutated, and no other call uses it at the same time, so it points at a live
    // `RecordHeadersInner` and the exclusive reference to its `headers` field aliases nothing for
    // the length of this call. The reference covers only `headers`, through a place projection, so
    // the `Headers` view that earlier `__as_Headers` calls handed out, held in the `view` field,
    // stays valid.
    unsafe { &mut *RecordHeadersInner::headers_ptr(self_ as *mut RecordHeadersInner) }.set_read_only();
}

/// `isReadOnly()`, as `0` / `1`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_is_read_only(
    self_: *const kafka_common_header_internals_RecordHeaders_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-headers handle, as
    // `record_headers_ref` requires; the reference is used only within this call.
    i8::from(unsafe { record_headers_ref(self_) }.is_read_only())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_to_string(
    self_: *const kafka_common_header_internals_RecordHeaders_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-headers handle, as
    // `record_headers_ref` requires; the result is copied into an owned value before return, so the
    // borrow lasts for this call only.
    into_c_string(&unsafe { record_headers_ref(self_) }.to_string())
}

/// The handle as the `Headers` interface it implements: a borrowed view
/// valid until the handle is destroyed, never passed to a `_destroy`
/// (`Headers_t` has none). It takes a `const` handle, so the read-only
/// handles (a deserializer's `headers`, a record's `headers()` getter) need no
/// cast; the view is non-`const` because `Headers` mutates, and the mutating
/// `Headers_*` functions may be called on it only when the handle was
/// received non-`const`.
///
/// # Safety
///
/// `self_` must be a valid record-headers handle that no other call uses at
/// the same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders__as_Headers(
    self_: *const kafka_common_header_internals_RecordHeaders_t,
) -> *mut kafka_common_header_Headers_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid record-headers handle that no
    // other call uses at the same time, so it points at a live `RecordHeadersInner` that does not
    // move (every owner keeps it boxed: a handle C owns, the box a serde invoker hands C for one
    // call, or the `CallArgs` an asynchronous serde call keeps in its boxed registry slot until
    // the report) and no other reference to its `view` is in use, as `headers_view` requires. The view returned points into the handle, so it lives until the handle is
    // destroyed, as the rustdoc states.
    unsafe { RecordHeadersInner::headers_view(self_.cast_mut() as *mut RecordHeadersInner) }
}

/// Frees an owned record-headers handle; null is a no-op. The view a
/// `kafka_common_RecordDeserializationError_headers` getter borrows out is
/// never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned record-headers handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_header_internals_RecordHeaders_destroy(
    self_: *mut kafka_common_header_internals_RecordHeaders_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // record-headers handle not yet destroyed, i.e. the `Box::into_raw` of a
        // `RecordHeadersInner` in `box_record_headers` (a view, which a parent embeds, is never
        // passed here), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut RecordHeadersInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr;

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
        // SAFETY: Test code: `header` comes from `kafka_common_header_internals_RecordHeader_new`
        // and is destroyed once, after both list constructors copy it and after the list that
        // borrows it is destroyed; each record-headers handle comes from a constructor, and every
        // one but the null-list one (left to leak) is destroyed once, after the references read
        // from it; `a` outlives the call reading it, and the string from `to_string` is destroyed
        // once, after it is read.
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
        // SAFETY: Test code: `headers` comes from `kafka_common_header_internals_RecordHeaders_new`
        // and is destroyed once, after the last use of `view`, the `Headers` view it hands out; the
        // record-header reference read from the owned list is used before the list is destroyed and
        // before the next mutation; `k` outlives the calls reading it; `err` is a valid slot
        // pointing at null before each call, and the error stored there is destroyed once, after
        // it is read.
        unsafe {
            let headers = kafka_common_header_internals_RecordHeaders_new();
            let view = kafka_common_header_internals_RecordHeaders__as_Headers(headers);
            // The view is borrowed and cached.
            assert_eq!(view, kafka_common_header_internals_RecordHeaders__as_Headers(headers));
            let mut err = ptr::null_mut();
            kafka_common_header_Headers_add_with_key_value(view, k.as_ptr(), kafka_Bytes_t::from_slice(&[7]), &mut err);
            assert!(err.is_null());
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
            kafka_common_header_Headers_add_with_key_value(view, k.as_ptr(), kafka_Bytes_t::NULL, &mut err);
            assert!(!err.is_null());
            assert_eq!(kafka_common_Error_is_local_illegal_state_error(err), 1);
            kafka_common_Error_destroy(err);
            kafka_common_header_internals_RecordHeaders_destroy(headers);
        }
    }
}

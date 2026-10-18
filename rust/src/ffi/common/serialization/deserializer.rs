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

//! `kafka_common_serialization_Deserializer_t`: the
//! `org.apache.kafka.common.serialization.Deserializer<T>` interface
//! (CLAUDE.md §4, "Traits").
//!
//! `T` is a `void *` (§4, "Generic types"): the functions return whatever
//! the implementation produces. A `void *` a C implementation
//! produces is owned by the C side and never freed by Rust; what a built-in
//! class produces is documented by that class. The `kafka_Bytes_t` arrays an
//! implementation receives are borrowed for the call, and `data == NULL` is
//! read as an empty array: the Rust trait takes a slice, never Java's
//! `null`.
//!
//! `configure` and `close` take `&mut self`, so a handle guards its
//! implementation with a mutex (see [`SharedDeserializer`]) and the two
//! invokers take a `*mut` handle.

#![expect(non_camel_case_types)]

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};
use std::ops::Range;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Deserializer;
use crate::ffi::common::deliver_value;
use crate::ffi::common::fail_with;
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::{kafka_common_Error_t, take_error};
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    GenericValue, box_string_keyed_map, c_str_to_string, destroy_string_element, into_c_string, kafka_Bytes_t,
    kafka_Map_destroy, kafka_Map_t, map_entries, owned_c_string,
};

/// Opaque handle to a [`Deserializer`] implementation over the `void *`
/// representation.
#[repr(C)]
pub struct kafka_common_serialization_Deserializer_t {
    _private: [u8; 0],
}

/// What every deserializer handle points at: the implementation behind the
/// mutex its `&mut self` methods (`configure`, `close`) need.
pub(crate) type SharedDeserializer = Mutex<dyn Deserializer<GenericValue>>;

/// `T deserialize(String topic, byte[] data)` of a C implementation: `data`
/// is borrowed for the call; the `void *` it returns is owned by the C side.
/// On failure it stores an owned error in `*out_err`, which points at `NULL`
/// on entry, and its return value is ignored.
pub type kafka_common_serialization_Deserializer_deserialize_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    topic: *const c_char,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void;

/// `T deserialize(String topic, Headers headers, byte[] data)` of a C
/// implementation; `headers` is a copy borrowed for the call. `NULL` is the
/// Java default: `deserialize(topic, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void,
>;

/// The Rust-only `deserialize_from_shared` of a C implementation: `data` is
/// a sub-array of `source`, the buffer it was sliced from, both borrowed for
/// the call. `NULL` is the default: `deserialize(topic, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_from_shared_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        source: kafka_Bytes_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void,
>;

/// The Rust-only `deserialize_from_shared_with_headers` of a C
/// implementation; `headers` is a copy borrowed for the call. `NULL` is the
/// default: `deserialize_with_headers(topic, headers, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        source: kafka_Bytes_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void,
>;

/// `configure(Map<String, ?> configs, boolean isKey)` of a C implementation:
/// `configs` maps NUL-terminated keys to NUL-terminated values, sorted by
/// key and borrowed for the call. `NULL` is the Java default: nothing.
pub type kafka_common_serialization_Deserializer_configure_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8)>;

/// `close()` of a C implementation. `NULL` is the Java default: nothing.
pub type kafka_common_serialization_Deserializer_close_fn_t = Option<unsafe extern "C" fn(self_: *mut c_void)>;

/// The handle behind a pointer: the mutex guarding the implementation.
///
/// # Safety
///
/// `deserializer` must be a valid deserializer handle, alive for `'a`.
pub(crate) unsafe fn deserializer_ref<'a>(
    deserializer: *const kafka_common_serialization_Deserializer_t,
) -> &'a SharedDeserializer {
    // SAFETY: Per this function's `# Safety`, `deserializer` is a valid deserializer handle alive
    // for `'a`, as `Interface::from_ptr` requires: the `Interface::owned` box of
    // `kafka_common_serialization_Deserializer_new`, or the `Interface` a `DeserializerHandle`
    // caches in its `as_deserializer` `OnceLock`, which lives as long as that class handle. An
    // `Interface` is never mutated after it is built, so the shared reference is sound for `'a`.
    unsafe { Interface::<SharedDeserializer>::from_ptr(deserializer as *const Interface<SharedDeserializer>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]); the consumer is `kafka_consumer_KafkaConsumer_new`
/// (CLAUDE.md §4 rule 6).
///
/// # Safety
///
/// `deserializer` must be a valid deserializer handle, not used again by
/// the caller when it was owned.
pub(crate) unsafe fn take_deserializer(
    deserializer: *mut kafka_common_serialization_Deserializer_t,
) -> Arc<SharedDeserializer> {
    // SAFETY: Per this function's `# Safety`, `deserializer` is a valid deserializer handle, not
    // used again by the caller when it was owned, which is what `Interface::take` requires: an
    // owned one is the `Interface::owned` box of `kafka_common_serialization_Deserializer_new`,
    // which `take` frees, and a view is the one a `DeserializerHandle` caches, which `take` only
    // clones the `Arc` of.
    unsafe { Interface::take(deserializer as *mut Interface<SharedDeserializer>) }
}

/// A C implementation of [`Deserializer`] registered through
/// [`kafka_common_serialization_Deserializer_new`]; a null optional method
/// is the Java default.
struct CDeserializer {
    self_: *mut c_void,
    deserialize: kafka_common_serialization_Deserializer_deserialize_fn_t,
    deserialize_with_headers: kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t,
    deserialize_from_shared: kafka_common_serialization_Deserializer_deserialize_from_shared_fn_t,
    deserialize_from_shared_with_headers:
        kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers_fn_t,
    configure: kafka_common_serialization_Deserializer_configure_fn_t,
    close: kafka_common_serialization_Deserializer_close_fn_t,
}

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_serialization_Deserializer_new`: the C user is responsible for the thread-safety
// of `self_`, which that function's rustdoc states, and the function pointers are trivially
// shareable. `CDeserializer` itself never dereferences `self_`; it only passes it back to the
// functions registered with it.
unsafe impl Send for CDeserializer {}
// SAFETY: `Sync` is required by the `Deserializer` trait. A `&CDeserializer` exposes only the
// opaque `self_` and the `Copy` function pointers, which the type never dereferences; it is reached
// only through the `SharedDeserializer` mutex `kafka_common_serialization_Deserializer_new` wraps
// it in, so no two threads use one at once, and the C user is responsible for the thread-safety of
// `self_` across the threads that take that lock in turn.
unsafe impl Sync for CDeserializer {}

/// Reads a C implementation's result: the error it stored, or the `void *`
/// it returned.
///
/// # Safety
///
/// `error` must be null or an owned error.
unsafe fn collect(error: *mut kafka_common_Error_t, out: *mut c_void) -> Result<GenericValue, Error> {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error handle, the
    // `box_error` handle of a `kafka_common_Error_*` constructor or a fallible FFI function that
    // the C implementation handed over, not yet destroyed, as `take_error` requires; it is consumed
    // here.
    match unsafe { take_error(error) } {
        Some(error) => Err(error),
        None => Ok(GenericValue::new(out)),
    }
}

impl Deserializer<GenericValue> for CDeserializer {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        let topic = owned_c_string(topic);
        let mut error = ptr::null_mut();
        // SAFETY: `self.deserialize` and `self.self_` are the pair the C caller registered
        // together through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle, or the client that consumed it, is
        // destroyed, which is after this `&self` call. A `CDeserializer` lives only inside the
        // `SharedDeserializer` mutex that function builds, and every caller locks it, so the
        // registered functions run one at a time. The arguments are borrowed for the call, as
        // the `_fn_t` rustdoc states: `topic` is a `CString` local, `data` a view over the
        // `&[u8]` argument, and `out_err` a local slot pointing at null.
        let out =
            unsafe { (self.deserialize)(self.self_, topic.as_ptr(), kafka_Bytes_t::from_slice(data), &raw mut error) };
        // SAFETY: `error` is what the C function stored in its slot: null or an owned error handle,
        // as its `_fn_t` rustdoc states and `collect` requires; `out` is the `void *` it returned.
        unsafe { collect(error, out) }
    }

    fn deserialize_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        let Some(deserialize_with_headers) = self.deserialize_with_headers else {
            return self.deserialize(topic, data);
        };
        let topic = owned_c_string(topic);
        // A copy borrowed for the call, as the serializer's invoker makes: the
        // class handle owns its headers.
        let headers = RecordHeadersInner::new(headers.clone());
        let mut error = ptr::null_mut();
        // SAFETY: `deserialize_with_headers` and `self.self_` are the pair the C caller registered
        // together through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle, or the client that consumed it, is destroyed,
        // which is after this `&self` call. A `CDeserializer` lives only inside the
        // `SharedDeserializer` mutex that function builds, and every caller locks it, so the
        // registered functions run one at a time. The arguments are borrowed for the call, as the
        // `_fn_t` rustdoc states: `topic` is a `CString` local, `headers` the call-local
        // `RecordHeadersInner` built from a copy of the argument, `data` a view over the `&[u8]`
        // argument, and `out_err` a local slot pointing at null.
        let out = unsafe {
            deserialize_with_headers(
                self.self_,
                topic.as_ptr(),
                headers.as_ptr(),
                kafka_Bytes_t::from_slice(data),
                &raw mut error,
            )
        };
        // SAFETY: `error` is what the C function stored in its slot: null or an owned error handle,
        // as its `_fn_t` rustdoc states and `collect` requires; `out` is the `void *` it returned.
        unsafe { collect(error, out) }
    }

    fn deserialize_from_shared(&self, topic: &str, source: &Bytes, data: &[u8]) -> Result<GenericValue, Error> {
        let Some(deserialize_from_shared) = self.deserialize_from_shared else {
            return self.deserialize(topic, data);
        };
        let topic = owned_c_string(topic);
        let mut error = ptr::null_mut();
        // SAFETY: `deserialize_from_shared` and `self.self_` are the pair the C caller registered
        // together through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the
        // caller keep `self_` alive until the handle, or the client that consumed it, is destroyed,
        // which is after this `&self` call. A `CDeserializer` lives only inside the
        // `SharedDeserializer` mutex that function builds, and every caller locks it, so the
        // registered functions run one at a time. The arguments are borrowed for the call, as the
        // `_fn_t` rustdoc states: `topic` is a `CString` local, `source` and `data` views over the
        // `&Bytes` and `&[u8]` arguments, and `out_err` a local slot pointing at null.
        let out = unsafe {
            deserialize_from_shared(
                self.self_,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(data),
                &raw mut error,
            )
        };
        // SAFETY: `error` is what the C function stored in its slot: null or an owned error handle,
        // as its `_fn_t` rustdoc states and `collect` requires; `out` is the `void *` it returned.
        unsafe { collect(error, out) }
    }

    fn deserialize_from_shared_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        source: &Bytes,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        let Some(deserialize_from_shared_with_headers) = self.deserialize_from_shared_with_headers else {
            return self.deserialize_with_headers(topic, headers, data);
        };
        let topic = owned_c_string(topic);
        // A copy borrowed for the call, as the serializer's invoker makes.
        let headers = RecordHeadersInner::new(headers.clone());
        let mut error = ptr::null_mut();
        // SAFETY: `deserialize_from_shared_with_headers` and `self.self_` are the pair the C caller
        // registered together through `kafka_common_serialization_Deserializer_new`, whose rustdoc
        // has the caller keep `self_` alive until the handle, or the client that consumed it, is
        // destroyed, which is after this `&self` call. A `CDeserializer` lives only inside the
        // `SharedDeserializer` mutex that function builds, and every caller locks it, so the
        // registered functions run one at a time. The arguments are borrowed for the call, as the
        // `_fn_t` rustdoc states: `topic` is a `CString` local, `headers` the call-local
        // `RecordHeadersInner` built from a copy of the argument, `source` and `data` views over
        // the `&Bytes` and `&[u8]` arguments, and `out_err` a local slot pointing at null.
        let out = unsafe {
            deserialize_from_shared_with_headers(
                self.self_,
                topic.as_ptr(),
                headers.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(data),
                &raw mut error,
            )
        };
        // SAFETY: `error` is what the C function stored in its slot: null or an owned error handle,
        // as its `_fn_t` rustdoc states and `collect` requires; `out` is the `void *` it returned.
        unsafe { collect(error, out) }
    }

    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        let Some(configure) = self.configure else { return };
        // Sorted by key: Java's map is unordered, index addressing through
        // the C map is not.
        let sorted: BTreeMap<&str, &str> = configs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let configs = box_string_keyed_map(
            sorted.into_iter().map(|(k, v)| (k, into_c_string(v) as *mut c_void)),
            Some(destroy_string_element),
        );
        // SAFETY: `configure` and `self.self_` are the pair the C caller registered together
        // through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle, or the client that consumed it, is destroyed, which is
        // after this `&self` call. A `CDeserializer` lives only inside the `SharedDeserializer`
        // mutex that function builds, and every caller locks it, so the registered functions run
        // one at a time. `configs` is the owned map `box_string_keyed_map` just built, borrowed by
        // the call as the `_fn_t` rustdoc states, and destroyed once the call returns.
        unsafe { configure(self.self_, configs, i8::from(is_key)) };
        // SAFETY: `configs` is the owned map `box_string_keyed_map` built above, not yet destroyed,
        // as `kafka_Map_destroy` requires; the C `configure` only borrowed it for its call. The map
        // owns its keys and its values, owned strings from `into_c_string` that
        // `destroy_string_element` frees.
        unsafe { kafka_Map_destroy(configs) };
    }

    fn close(&mut self) {
        let Some(close) = self.close else { return };
        // SAFETY: `close` and `self.self_` are the pair the C caller registered together through
        // `kafka_common_serialization_Deserializer_new`, whose rustdoc has the caller keep `self_`
        // alive until the handle, or the client that consumed it, is destroyed, which is after this
        // `&self` call. A `CDeserializer` lives only inside the `SharedDeserializer` mutex that
        // function builds, and every caller locks it, so the registered functions run one at a
        // time.
        unsafe { close(self.self_) };
    }
}

/// Registers a C implementation of `Deserializer`; `deserialize` is
/// required, the other pointers may be `NULL` for the Java default. The
/// caller owns `self_` and keeps it alive until the handle is destroyed
/// with [`kafka_common_serialization_Deserializer_destroy`] or, once a
/// client consumed it, until that client is destroyed. The functions are
/// called one at a time, under the handle's lock, but from whichever thread
/// makes the call, so the C caller is responsible for `self_` being usable
/// from any thread.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_Deserializer_new(
    self_: *mut c_void,
    deserialize: kafka_common_serialization_Deserializer_deserialize_fn_t,
    deserialize_with_headers: kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t,
    deserialize_from_shared: kafka_common_serialization_Deserializer_deserialize_from_shared_fn_t,
    deserialize_from_shared_with_headers: kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers_fn_t,
    configure: kafka_common_serialization_Deserializer_configure_fn_t,
    close: kafka_common_serialization_Deserializer_close_fn_t,
) -> *mut kafka_common_serialization_Deserializer_t {
    let deserializer: Arc<SharedDeserializer> = Arc::new(Mutex::new(CDeserializer {
        self_,
        deserialize,
        deserialize_with_headers,
        deserialize_from_shared,
        deserialize_from_shared_with_headers,
        configure,
        close,
    }));
    Interface::owned(deserializer) as *mut kafka_common_serialization_Deserializer_t
}

/// Returns a deserialized value, or stores the owned error in `*out_err`.
///
/// # Safety
///
/// `out_err` must be valid for one write and point at null.
unsafe fn deliver(result: Result<GenericValue, Error>, out_err: *mut *mut kafka_common_Error_t) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. The
    // `void *` returned is the implementation's, owned as its class or C implementation documents.
    unsafe { deliver_value(result, out_err, |value| value.as_ptr()) }
}

/// The bytes behind `data`, an empty array for Java's `null`.
///
/// # Safety
///
/// As [`kafka_Bytes_t::as_slice`].
unsafe fn slice<'a>(data: kafka_Bytes_t) -> &'a [u8] {
    // SAFETY: This function's `# Safety` is `kafka_Bytes_t::as_slice`'s: `data.data` is null or
    // points at `len` readable bytes that outlive `'a` and are not written during it.
    unsafe { data.as_slice() }.unwrap_or(&[])
}

/// Where `data` lies inside `source`, or `None` when it is not a sub-array
/// of it; a null `data` is the empty sub-array at the start.
fn subslice_range(source: kafka_Bytes_t, data: kafka_Bytes_t) -> Option<Range<usize>> {
    if data.data.is_null() {
        return Some(0..0);
    }
    let start = (data.data as usize).checked_sub(source.data as usize)?;
    let end = start.checked_add(usize::try_from(data.len).ok()?)?;
    (!source.data.is_null() && end <= usize::try_from(source.len).ok()?).then_some(start..end)
}

/// The owned copy of `source` the shared-buffer invokers hand to the
/// implementation, with `data` relocated into it; an error when `data` is
/// not a sub-array of `source`.
///
/// # Safety
///
/// As [`kafka_Bytes_t::as_slice`] for both arrays.
unsafe fn shared(source: kafka_Bytes_t, data: kafka_Bytes_t) -> Result<(Bytes, Range<usize>), Error> {
    let range = subslice_range(source, data)
        .ok_or_else(|| Error::local_illegal_argument("data must be a sub-array of source"))?;
    // SAFETY: Per this function's `# Safety`, `source` meets `kafka_Bytes_t::as_slice`'s contract,
    // as `slice` requires; the borrow lasts only until `Bytes::copy_from_slice` copies it.
    Ok((Bytes::copy_from_slice(unsafe { slice(source) }), range))
}

/// `T deserialize(String topic, byte[] data)`: `data` is borrowed for the
/// call, `data.data == NULL` read as an empty array. Returns the
/// implementation's `void *`; on failure stores the owned error in
/// `*out_err` and returns `NULL`.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `data` null or a valid array and
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; it stays alive for this call, and the guard on its mutex is
    // released before return.
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver`
    // requires, and `data` is null or a valid array, i.e. `len` readable bytes that live through
    // this call and are not written during it, as `slice` (`kafka_Bytes_t::as_slice`) requires; the
    // implementation reads the slice only during this call.
    unsafe { deliver(deserializer.deserialize(&topic, slice(data)), out_err) }
}

/// `T deserialize(String topic, Headers headers, byte[] data)`; the value
/// is delivered as for [`kafka_common_serialization_Deserializer_deserialize`].
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid record-headers handle, `data`
/// null or a valid array and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_with_headers(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, which the
    // caller keeps alive and does not mutate for this call, as `record_headers_ref` requires; the
    // reference is used only within this call.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; it stays alive for this call, and the guard on its mutex is
    // released before return.
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as
    // `deliver` requires, and `data` is null or a valid array, i.e. `len` readable bytes that live
    // through this call and are not written during it, as `slice` (`kafka_Bytes_t::as_slice`)
    // requires; the implementation reads the slice only during this call.
    unsafe { deliver(deserializer.deserialize_with_headers(&topic, headers, slice(data)), out_err) }
}

/// The Rust-only `deserialize_from_shared`: `data` must be a sub-array of
/// `source` (the translation of `IllegalArgumentException` otherwise), both
/// borrowed for the call. `source` is copied once so the implementation can
/// slice it; a `BytesDeserializer` then returns a slice of that copy.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `source` and `data` null or valid arrays and
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_from_shared(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    source: kafka_Bytes_t,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `source` and `data` are null or valid arrays, i.e.
    // `len` readable bytes that live through this call and are not written during it, as `shared`
    // requires; `shared` copies `source` before returning and reads only the address and length of
    // `data`.
    let (source, range) = match unsafe { shared(source, data) } {
        Ok(shared) => shared,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => return unsafe { fail_with(out_err, error) },
    };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; it stays alive for this call, and the guard on its mutex is
    // released before return.
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot, as
    // `deliver` requires; the arrays the implementation reads are the owned copy `shared` made.
    unsafe { deliver(deserializer.deserialize_from_shared(&topic, &source, &source[range]), out_err) }
}

/// The Rust-only `deserialize_from_shared_with_headers`; `source` and
/// `data` as for [`kafka_common_serialization_Deserializer_deserialize_from_shared`].
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid NUL-terminated
/// string, `headers` a valid record-headers handle, `source` and `data` null
/// or valid arrays and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    source: kafka_Bytes_t,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, which the
    // caller keeps alive and does not mutate for this call, as `record_headers_ref` requires; the
    // reference is used only within this call.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `source` and `data` are null or valid arrays, i.e.
    // `len` readable bytes that live through this call and are not written during it, as `shared`
    // requires; `shared` copies `source` before returning and reads only the address and length of
    // `data`.
    let (source, range) = match unsafe { shared(source, data) } {
        Ok(shared) => shared,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => return unsafe { fail_with(out_err, error) },
    };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; it stays alive for this call, and the guard on its mutex is
    // released before return.
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver`
    // requires; the arrays the implementation reads are the owned copy `shared`
    // made.
    unsafe {
        deliver(
            deserializer.deserialize_from_shared_with_headers(&topic, headers, &source, &source[range]),
            out_err,
        )
    }
}

/// `configure(Map<String, ?> configs, boolean isKey)`: `configs` maps
/// NUL-terminated keys to NUL-terminated values, copied during the call.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle and `configs` null or a
/// valid map of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_configure(
    self_: *mut kafka_common_serialization_Deserializer_t,
    configs: *const kafka_Map_t,
    is_key: i8,
) {
    // SAFETY: Per this function's `# Safety`, `configs` is null or a valid map, which the caller
    // keeps alive and does not mutate for this call, as `map_entries` requires (null reads as
    // empty); the borrow of its entries ends when they are collected.
    let configs: HashMap<String, String> = unsafe { map_entries(configs) }
        .iter()
        .map(|&(k, v)| {
            // SAFETY: Per this function's `# Safety`, every key and value of `configs` is a valid
            // NUL-terminated string, as `c_str_to_string` requires; both are copied into owned
            // `String`s, so nothing borrows them after this call.
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                c_str_to_string(v as *const c_char)
            })
        })
        .collect();
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; the reference and the guard on its mutex are used only within
    // this call.
    unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .configure(&configs, is_key != 0);
}

/// `close()`.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_close(
    self_: *mut kafka_common_serialization_Deserializer_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; the reference and the guard on its mutex are used only within
    // this call.
    unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .close();
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out (`__as_Deserializer`) is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned deserializer handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_destroy(
    self_: *mut kafka_common_serialization_Deserializer_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned deserializer handle not
    // yet destroyed, as `Interface::destroy` requires: the `Interface::owned` box of
    // `kafka_common_serialization_Deserializer_new`. The view a class handle hands out is never
    // passed here, as the rustdoc states.
    unsafe { Interface::<SharedDeserializer>::destroy(self_ as *mut Interface<SharedDeserializer>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::header::Headers;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::{
        kafka_common_Error_destroy, kafka_common_Error_message, kafka_common_Error_serialization,
    };
    use crate::ffi::util::{
        kafka_Map_key, kafka_Map_new, kafka_Map_put, kafka_Map_size, kafka_Map_value, kafka_string_destroy,
    };

    // A C deserializer producing an owned `char *` with the bytes
    // upper-cased, logging what it was asked.
    #[derive(Default)]
    struct Upper {
        calls: Mutex<Vec<String>>,
    }
    fn upper<'a>(self_: *mut c_void) -> &'a Upper {
        // SAFETY: Test code: `self_` is the `&imp` the tests register, which outlives every call
        // that passes it back.
        unsafe { &*(self_ as *const Upper) }
    }
    fn log(self_: *mut c_void, entry: String) {
        upper(self_).calls.lock().unwrap().push(entry);
    }
    fn text(data: kafka_Bytes_t) -> String {
        // SAFETY: Test code: `data` is a view the invoker built over a live slice, read during the
        // call.
        String::from_utf8(unsafe { slice(data) }.to_vec()).unwrap()
    }
    fn write_upper(data: kafka_Bytes_t, out_err: *mut *mut kafka_common_Error_t) -> *mut c_void {
        let text = text(data);
        if text == "fail" {
            let message = CString::new("cannot deserialize fail").unwrap();
            // SAFETY: Test code: `message` outlives the constructor, which copies it; `out_err` is
            // the valid slot pointing at null the invoker passes, and the error stored there is
            // handed to Rust, which consumes it.
            unsafe { *out_err = kafka_common_Error_serialization(message.as_ptr()) };
            return ptr::null_mut();
        }
        into_c_string(&text.to_uppercase()) as *mut c_void
    }
    unsafe extern "C" fn c_deserialize(
        self_: *mut c_void,
        topic: *const c_char,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        log(
            self_,
            format!(
                "deserialize {} {}",
                // SAFETY: Test code: `topic` is the `CString` the invoker passes, alive for the
                // call.
                unsafe { CStr::from_ptr(topic) }.to_str().unwrap(),
                text(data)
            ),
        );
        write_upper(data, out_err)
    }
    unsafe extern "C" fn c_deserialize_with_headers(
        self_: *mut c_void,
        _topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: `headers` is the record-headers copy the invoker passes, alive and
        // unchanged for the call.
        let has_k = unsafe { record_headers_ref(headers) }.last_header("k").is_some();
        log(self_, format!("deserialize_with_headers k={has_k}"));
        write_upper(data, out_err)
    }
    unsafe extern "C" fn c_deserialize_from_shared(
        self_: *mut c_void,
        _topic: *const c_char,
        source: kafka_Bytes_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        log(self_, format!("deserialize_from_shared {} in {}", text(data), text(source)));
        write_upper(data, out_err)
    }
    unsafe extern "C" fn c_configure(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8) {
        // SAFETY: Test code: `configs` is the map the invoker passes, alive and unchanged for the
        // call, whose keys and values are NUL-terminated strings.
        let entries: Vec<String> = (0..unsafe { kafka_Map_size(configs) })
            // SAFETY: Test code: `configs` is the map the invoker passes, alive and unchanged for
            // the call, and `i` is below its size, so each key and value is a NUL-terminated
            // string.
            .map(|i| unsafe {
                format!(
                    "{}={}",
                    CStr::from_ptr(kafka_Map_key(configs, i) as *const c_char).to_str().unwrap(),
                    CStr::from_ptr(kafka_Map_value(configs, i) as *const c_char).to_str().unwrap()
                )
            })
            .collect();
        log(self_, format!("configure {} is_key={is_key}", entries.join(",")));
    }
    unsafe extern "C" fn c_close(self_: *mut c_void) {
        log(self_, "close".to_string());
    }

    unsafe fn take_string(out: *mut c_void) -> String {
        // SAFETY: Test code: `out` is an owned `char *` a C deserializer of these tests returned from
        // `into_c_string`, read before `kafka_string_destroy` frees it once.
        let s = unsafe { CStr::from_ptr(out as *const c_char) }.to_str().unwrap().to_string();
        // SAFETY: Test code: `out` is an owned `char *` from `into_c_string`, not yet freed, and
        // not used after this call.
        unsafe { kafka_string_destroy(out as *mut c_char) };
        s
    }

    #[test]
    fn c_deserializer_is_reached_through_the_invokers() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Deserializer_new(
            &imp as *const Upper as *mut c_void,
            c_deserialize,
            Some(c_deserialize_with_headers),
            Some(c_deserialize_from_shared),
            None,
            Some(c_configure),
            Some(c_close),
        );
        let topic = CString::new("t").unwrap();
        let mut record_headers = RecordHeaders::new();
        record_headers.add_with_key_value("k", Some(b"v")).unwrap();
        let headers = box_record_headers(record_headers);
        let source = b"hello world";
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_Deserializer_new` and
        // is destroyed once, at the end; `imp` outlives it; `headers` comes from
        // `box_record_headers` and is destroyed once, after the last call reading it; `topic`,
        // `source` and the arrays outlive the calls reading them; `err` is a valid slot pointing at
        // null, left null by every call (asserted), and every value returned is an owned string
        // that `take_string` frees once; `map` comes from `kafka_Map_new`, borrows the `CString`s,
        // which outlive it, and is destroyed once.
        unsafe {
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"hello"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "HELLO");

            let out = kafka_common_serialization_Deserializer_deserialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"hello"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "HELLO");

            let out = kafka_common_serialization_Deserializer_deserialize_from_shared(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[6..]),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "WORLD");

            // No C method for the header-aware shared variant: it falls
            // back to `deserialize_with_headers`, the Rust default.
            let out = kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[..5]),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "HELLO");

            // Null data is an empty array.
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::NULL,
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "");

            let map = kafka_Map_new();
            let (k1, v1) = (CString::new("zeta").unwrap(), CString::new("1").unwrap());
            let (k2, v2) = (CString::new("alpha").unwrap(), CString::new("2").unwrap());
            kafka_Map_put(map, k1.as_ptr() as *mut c_void, v1.as_ptr() as *mut c_void);
            kafka_Map_put(map, k2.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void);
            kafka_common_serialization_Deserializer_configure(handle, map, 1);
            kafka_Map_destroy(map);
            kafka_common_serialization_Deserializer_close(handle);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Deserializer_destroy(handle);
            kafka_common_serialization_Deserializer_destroy(ptr::null_mut());
        }
        assert_eq!(
            *imp.calls.lock().unwrap(),
            [
                "deserialize t hello",
                "deserialize_with_headers k=true",
                "deserialize_from_shared world in hello world",
                "deserialize_with_headers k=true",
                "deserialize t ",
                "configure alpha=2,zeta=1 is_key=1",
                "close",
            ]
        );
    }

    #[test]
    fn c_error_is_stored_owned_and_null_optional_methods_are_the_java_default() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Deserializer_new(
            &imp as *const Upper as *mut c_void,
            c_deserialize,
            None,
            None,
            None,
            None,
            None,
        );
        let topic = CString::new("t").unwrap();
        let headers = box_record_headers(RecordHeaders::new());
        let source = b"fail ok";
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_Deserializer_new` and
        // is destroyed once, at the end; `imp` outlives it; `headers` comes from
        // `box_record_headers` and is destroyed once, after the last call reading it; `topic`,
        // `source` and the arrays outlive the calls reading them; `err` is a valid slot pointing at
        // null before every call (reset after each error is destroyed), every value returned is an
        // owned string that `take_string` frees once, and each error is destroyed once, after it is
        // read.
        unsafe {
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"fail"),
                &raw mut err,
            );
            assert!(!err.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "cannot deserialize fail"
            );
            assert!(out.is_null(), "a failure returns NULL");
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();

            // Every optional method falls back to `deserialize`; one call at
            // a time, since each one owns the string it returns.
            let err_slot = &raw mut err;
            let fallbacks: [&dyn Fn() -> *mut c_void; 3] = [
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_with_headers(
                        handle,
                        topic.as_ptr(),
                        headers,
                        kafka_Bytes_t::from_slice(b"ok"),
                        err_slot,
                    )
                },
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_from_shared(
                        handle,
                        topic.as_ptr(),
                        kafka_Bytes_t::from_slice(source),
                        kafka_Bytes_t::from_slice(&source[5..]),
                        err_slot,
                    )
                },
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                        handle,
                        topic.as_ptr(),
                        headers,
                        kafka_Bytes_t::from_slice(source),
                        kafka_Bytes_t::from_slice(&source[5..]),
                        err_slot,
                    )
                },
            ];
            for fallback in fallbacks {
                let out = fallback();
                assert!(err.is_null());
                assert_eq!(take_string(out), "OK");
            }
            // No-ops: nothing to observe, nothing crashes.
            kafka_common_serialization_Deserializer_configure(handle, ptr::null(), 0);
            kafka_common_serialization_Deserializer_close(handle);

            // Data outside the source is rejected before reaching C.
            let out = kafka_common_serialization_Deserializer_deserialize_from_shared(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(b"elsewhere"),
                &raw mut err,
            );
            assert!(!err.is_null());
            assert!(out.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "data must be a sub-array of source"
            );
            kafka_common_Error_destroy(err);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Deserializer_destroy(handle);
        }
        assert_eq!(
            *imp.calls.lock().unwrap(),
            [
                "deserialize t fail",
                "deserialize t ok",
                "deserialize t ok",
                "deserialize t ok"
            ]
        );
    }

    #[test]
    fn subslice_range_locates_data_inside_source() {
        let source = b"0123456789";
        let s = kafka_Bytes_t::from_slice(source);
        assert_eq!(subslice_range(s, kafka_Bytes_t::from_slice(&source[3..7])), Some(3..7));
        assert_eq!(subslice_range(s, kafka_Bytes_t::from_slice(source)), Some(0..10));
        assert_eq!(subslice_range(s, kafka_Bytes_t::from_slice(&source[10..])), Some(10..10));
        assert_eq!(subslice_range(s, kafka_Bytes_t::NULL), Some(0..0));
        assert_eq!(subslice_range(kafka_Bytes_t::NULL, kafka_Bytes_t::NULL), Some(0..0));
        assert_eq!(subslice_range(s, kafka_Bytes_t::from_slice(b"elsewhere")), None);
        assert_eq!(subslice_range(kafka_Bytes_t::NULL, kafka_Bytes_t::from_slice(source)), None);
        // Starts inside, runs past the end.
        let past = kafka_Bytes_t { data: source[8..].as_ptr(), len: 5 };
        assert_eq!(subslice_range(s, past), None);
    }
}

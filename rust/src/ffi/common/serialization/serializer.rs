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

//! `kafka_common_serialization_Serializer_t`: the
//! `org.apache.kafka.common.serialization.Serializer<T>` interface
//! (CLAUDE.md §4, "Traits").
//!
//! `T` is a `void *` (§4, "Generic types"): `data` is whatever the
//! implementation reads, `NULL` being Java's `null`. A C implementation
//! writes the serialized bytes into its `out_*` slot as a `kafka_Bytes_t`
//! that Rust copies before the call returns, so the buffer behind it only
//! needs to live for the call; `data == NULL` in the slot is Java's `null`
//! array.
//!
//! The invokers deliver the bytes the other way round: the `kafka_Bytes_t`
//! they write borrows a buffer the handle keeps, valid until the next
//! `serialize*` call on the same handle or its destruction — the same
//! lifetime a `const char *` getter gives.
//!
//! `Serializer.configure` and `Serializer.close` have no Rust counterpart
//! (the Rust trait omits them), so neither has a C one.

#![expect(non_camel_case_types)]

use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Serializer;
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::{box_error, kafka_common_Error_t, take_error};
use crate::ffi::util::{GenericValue, c_str_to_string, kafka_Bytes_t, owned_c_string};

/// Opaque handle to a [`Serializer`] implementation over the `void *`
/// representation.
#[repr(C)]
pub struct kafka_common_serialization_Serializer_t {
    _private: [u8; 0],
}

/// The trait object every serializer handle points at.
pub(crate) type DynSerializer = dyn Serializer<GenericValue> + Send + Sync;

/// `byte[] serialize(String topic, T data)` of a C implementation: `data` is
/// the `void *` (`NULL` for Java's `null`); the bytes written to
/// `out_serialize` are copied before the call returns, `data == NULL` in the
/// slot standing for a `null` array. Returns `NULL` or an owned error.
pub type kafka_common_serialization_Serializer_serialize_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    topic: *const c_char,
    data: *const c_void,
    out_serialize: *mut kafka_Bytes_t,
) -> *mut kafka_common_Error_t;

/// `byte[] serialize(String topic, Headers headers, T data)` of a C
/// implementation; `headers` is a copy borrowed for the call. `NULL` is the
/// Java default: `serialize(topic, data)`.
pub type kafka_common_serialization_Serializer_serialize_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        out_serialize_with_headers: *mut kafka_Bytes_t,
    ) -> *mut kafka_common_Error_t,
>;

/// The Rust-only `serialize_owned_with_headers` of a C implementation: the
/// same call as `serialize_with_headers` for a `void *`, which is never
/// borrowed or owned differently. `NULL` is the default:
/// `serialize_with_headers(topic, headers, data)`.
pub type kafka_common_serialization_Serializer_serialize_owned_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        out_serialize_owned_with_headers: *mut kafka_Bytes_t,
    ) -> *mut kafka_common_Error_t,
>;

/// What a [`kafka_common_serialization_Serializer_t`] points at: the
/// implementation, who owns the handle, and the bytes the last invoker
/// returned. The last two are what keeps this from being a plain
/// [`Interface`](crate::ffi::common::metrics::Interface): the `kafka_Bytes_t`
/// an invoker writes is a view, so the handle must own what it points into.
pub(crate) struct SerializerInner {
    imp: Arc<DynSerializer>,
    /// `true` for a handle the C caller owns (`_new`), `false` for the view
    /// a class handle caches (`__as_Serializer`); see `Interface::owned`.
    owned: bool,
    /// The bytes the last `serialize*` invoker returned, borrowed by the
    /// `kafka_Bytes_t` it wrote: valid until the next invoker call on this
    /// handle or its destruction.
    output: Mutex<Option<Vec<u8>>>,
}

impl SerializerInner {
    /// A handle the C caller owns.
    pub(crate) fn owned(imp: Arc<DynSerializer>) -> *mut kafka_common_serialization_Serializer_t {
        Box::into_raw(Box::new(Self { imp, owned: true, output: Mutex::new(None) }))
            as *mut kafka_common_serialization_Serializer_t
    }

    /// The view a class handle caches.
    pub(crate) fn view(imp: Arc<DynSerializer>) -> Self {
        Self { imp, owned: false, output: Mutex::new(None) }
    }

    /// The implementation.
    pub(crate) fn get(&self) -> &DynSerializer {
        &*self.imp
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_serialization_Serializer_t {
        self as *const Self as *const kafka_common_serialization_Serializer_t
    }

    /// Keeps `bytes` as the last output and returns the view over them.
    fn store(&self, bytes: Option<Vec<u8>>) -> kafka_Bytes_t {
        let mut output = self.output.lock().unwrap_or_else(PoisonError::into_inner);
        *output = bytes;
        kafka_Bytes_t::from_option(output.as_deref())
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `serializer` must be a valid serializer handle.
pub(crate) unsafe fn serializer_ref<'a>(
    serializer: *const kafka_common_serialization_Serializer_t,
) -> &'a SerializerInner {
    unsafe { &*(serializer as *const SerializerInner) }
}

/// Takes the implementation a `*mut` parameter received: an owned handle is
/// freed and its implementation moved out, a view shares its implementation
/// and stays with its class handle. The consumer is `KafkaProducer_new`
/// (CLAUDE.md §4 rule 6), which lands with the producer bindings; until then
/// only the tests exercise it.
///
/// # Safety
///
/// `serializer` must be a valid serializer handle, not used again by the
/// caller when it was owned.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) unsafe fn take_serializer(serializer: *mut kafka_common_serialization_Serializer_t) -> Arc<DynSerializer> {
    let inner = serializer as *mut SerializerInner;
    if unsafe { &*inner }.owned {
        unsafe { Box::from_raw(inner) }.imp
    } else {
        Arc::clone(&unsafe { &*inner }.imp)
    }
}

/// A C implementation of [`Serializer`] registered through
/// [`kafka_common_serialization_Serializer_new`]; a null optional method is
/// the Java default.
struct CSerializer {
    self_: *mut c_void,
    serialize: kafka_common_serialization_Serializer_serialize_fn_t,
    serialize_with_headers: kafka_common_serialization_Serializer_serialize_with_headers_fn_t,
    serialize_owned_with_headers: kafka_common_serialization_Serializer_serialize_owned_with_headers_fn_t,
}

// SAFETY: `self_` is what the C caller registered, whose thread-safety is
// the caller's responsibility as for every interface implementation
// (CLAUDE.md §4).
unsafe impl Send for CSerializer {}
unsafe impl Sync for CSerializer {}

/// The `void *` for `data`: null for Java's `null`.
fn data_ptr(data: Option<&GenericValue>) -> *const c_void {
    data.map_or(ptr::null(), |value| value.as_ptr().cast_const())
}

/// Reads a C implementation's result: its error, or a copy of the bytes it
/// wrote (`None` for a `NULL` array).
///
/// # Safety
///
/// `out` must be what the C implementation wrote: null or `len` readable
/// bytes.
unsafe fn collect(error: *mut kafka_common_Error_t, out: kafka_Bytes_t) -> Result<Option<Vec<u8>>, Error> {
    match unsafe { take_error(error) } {
        Some(error) => Err(error),
        None => Ok(unsafe { out.as_slice() }.map(<[u8]>::to_vec)),
    }
}

impl Serializer<GenericValue> for CSerializer {
    fn serialize(&self, topic: &str, data: Option<&GenericValue>) -> Result<Option<Vec<u8>>, Error> {
        let topic = owned_c_string(topic);
        let mut out = kafka_Bytes_t::NULL;
        let error = unsafe { (self.serialize)(self.self_, topic.as_ptr(), data_ptr(data), &raw mut out) };
        unsafe { collect(error, out) }
    }

    fn serialize_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<&GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(serialize_with_headers) = self.serialize_with_headers else {
            return self.serialize(topic, data);
        };
        let topic = owned_c_string(topic);
        // A copy borrowed for the call: the class handle owns its headers.
        let headers = RecordHeadersInner::new(headers.clone());
        let mut out = kafka_Bytes_t::NULL;
        let error = unsafe {
            serialize_with_headers(self.self_, topic.as_ptr(), headers.as_ptr(), data_ptr(data), &raw mut out)
        };
        unsafe { collect(error, out) }
    }

    fn serialize_owned_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        let Some(serialize_owned_with_headers) = self.serialize_owned_with_headers else {
            return self.serialize_with_headers(topic, headers, data.as_ref());
        };
        let topic = owned_c_string(topic);
        let headers = RecordHeadersInner::new(headers.clone());
        let mut out = kafka_Bytes_t::NULL;
        let error = unsafe {
            serialize_owned_with_headers(
                self.self_,
                topic.as_ptr(),
                headers.as_ptr(),
                data_ptr(data.as_ref()),
                &raw mut out,
            )
        };
        unsafe { collect(error, out) }
    }
}

/// Registers a C implementation of `Serializer`; `serialize` is required,
/// the other pointers may be `NULL` for the Java default. The caller owns
/// `self_` and keeps it alive until the handle is destroyed with
/// [`kafka_common_serialization_Serializer_destroy`] or, once a client
/// consumed it, until that client is destroyed.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_Serializer_new(
    self_: *mut c_void,
    serialize: kafka_common_serialization_Serializer_serialize_fn_t,
    serialize_with_headers: kafka_common_serialization_Serializer_serialize_with_headers_fn_t,
    serialize_owned_with_headers: kafka_common_serialization_Serializer_serialize_owned_with_headers_fn_t,
) -> *mut kafka_common_serialization_Serializer_t {
    let serializer: Arc<DynSerializer> =
        Arc::new(CSerializer { self_, serialize, serialize_with_headers, serialize_owned_with_headers });
    SerializerInner::owned(serializer)
}

/// Runs `serialize` on the implementation behind `self_` and writes its
/// bytes to `out`, borrowed from the handle until its next call.
///
/// # Safety
///
/// `self_` must be a valid serializer handle and `out` a valid slot.
unsafe fn invoke(
    self_: *const kafka_common_serialization_Serializer_t,
    out: *mut kafka_Bytes_t,
    serialize: impl FnOnce(&DynSerializer) -> Result<Option<Vec<u8>>, Error>,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { serializer_ref(self_) };
    match serialize(inner.get()) {
        Ok(bytes) => {
            unsafe { *out = inner.store(bytes) };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// The `Option<&GenericValue>` for a `void *` parameter.
fn generic(data: *const c_void) -> Option<GenericValue> {
    (!data.is_null()).then(|| GenericValue::new(data.cast_mut()))
}

/// `byte[] serialize(String topic, T data)`: `data` is the `void *` the
/// implementation reads, `NULL` for Java's `null`. On success writes the
/// bytes to `out_serialize` (`data == NULL` for a `null` array), borrowed
/// from the handle until its next `serialize*` call or its destruction;
/// returns the owned error otherwise.
///
/// # Safety
///
/// `self_` must be a valid serializer handle, `topic` a valid
/// NUL-terminated string and `out_serialize` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    data: *const c_void,
    out_serialize: *mut kafka_Bytes_t,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let data = generic(data);
    unsafe { invoke(self_, out_serialize, |serializer| serializer.serialize(&topic, data.as_ref())) }
}

/// `byte[] serialize(String topic, Headers headers, T data)`; the bytes
/// are delivered as for [`kafka_common_serialization_Serializer_serialize`].
///
/// # Safety
///
/// `self_` must be a valid serializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid record-headers handle and
/// `out_serialize_with_headers` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize_with_headers(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: *const c_void,
    out_serialize_with_headers: *mut kafka_Bytes_t,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let headers = unsafe { record_headers_ref(headers) };
    let data = generic(data);
    unsafe {
        invoke(self_, out_serialize_with_headers, |serializer| {
            serializer.serialize_with_headers(&topic, headers, data.as_ref())
        })
    }
}

/// The Rust-only `serialize_owned_with_headers`: for a `void *` the same
/// call as [`kafka_common_serialization_Serializer_serialize_with_headers`],
/// kept so a C caller reaches an implementation that overrides it.
///
/// # Safety
///
/// `self_` must be a valid serializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid record-headers handle and
/// `out_serialize_owned_with_headers` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize_owned_with_headers(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: *const c_void,
    out_serialize_owned_with_headers: *mut kafka_Bytes_t,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let headers = unsafe { record_headers_ref(headers) };
    let data = generic(data);
    unsafe {
        invoke(self_, out_serialize_owned_with_headers, |serializer| {
            serializer.serialize_owned_with_headers(&topic, headers, data)
        })
    }
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out (`__as_Serializer`) is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned serializer handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_destroy(
    self_: *mut kafka_common_serialization_Serializer_t,
) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut SerializerInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::sync::Mutex;

    use super::*;
    use crate::common::header::Headers;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders_destroy,
        kafka_common_header_internals_RecordHeaders_to_string,
    };
    use crate::ffi::common::{
        kafka_common_Error_destroy, kafka_common_Error_message, kafka_common_Error_serialization,
    };

    // A C serializer: `data` is a `const char *`, serialized upper-cased,
    // logging what it was asked.
    #[derive(Default)]
    struct Upper {
        calls: Mutex<Vec<String>>,
        // The buffer behind the `kafka_Bytes_t` it writes, alive for the
        // call only — Rust copies before returning.
        buffer: Mutex<Vec<u8>>,
    }
    fn upper<'a>(self_: *mut c_void) -> &'a Upper {
        unsafe { &*(self_ as *const Upper) }
    }
    fn write_upper(self_: *mut c_void, data: *const c_void, out: *mut kafka_Bytes_t) -> *mut kafka_common_Error_t {
        if data.is_null() {
            unsafe { *out = kafka_Bytes_t::NULL };
            return ptr::null_mut();
        }
        let text = unsafe { CStr::from_ptr(data as *const c_char) }.to_str().unwrap();
        if text == "fail" {
            let message = CString::new("cannot serialize fail").unwrap();
            return unsafe { kafka_common_Error_serialization(message.as_ptr()) };
        }
        let mut buffer = upper(self_).buffer.lock().unwrap();
        *buffer = text.to_uppercase().into_bytes();
        unsafe { *out = kafka_Bytes_t::from_slice(&buffer) };
        ptr::null_mut()
    }
    unsafe extern "C" fn c_serialize(
        self_: *mut c_void,
        topic: *const c_char,
        data: *const c_void,
        out: *mut kafka_Bytes_t,
    ) -> *mut kafka_common_Error_t {
        let topic = unsafe { CStr::from_ptr(topic) }.to_str().unwrap();
        upper(self_).calls.lock().unwrap().push(format!("serialize {topic}"));
        write_upper(self_, data, out)
    }
    unsafe extern "C" fn c_serialize_with_headers(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        out: *mut kafka_Bytes_t,
    ) -> *mut kafka_common_Error_t {
        let topic = unsafe { CStr::from_ptr(topic) }.to_str().unwrap();
        let headers = unsafe { kafka_common_header_internals_RecordHeaders_to_string(headers) };
        let text = unsafe { CStr::from_ptr(headers) }.to_str().unwrap().to_string();
        unsafe { crate::ffi::util::kafka_string_destroy(headers) };
        upper(self_)
            .calls
            .lock()
            .unwrap()
            .push(format!("serialize_with_headers {topic} {text}"));
        write_upper(self_, data, out)
    }

    unsafe fn bytes_of(out: kafka_Bytes_t) -> Option<Vec<u8>> {
        unsafe { out.as_slice() }.map(<[u8]>::to_vec)
    }

    #[test]
    fn c_serializer_is_reached_through_the_invokers_and_its_bytes_are_copied() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Serializer_new(
            &imp as *const Upper as *mut c_void,
            c_serialize,
            Some(c_serialize_with_headers),
            None,
        );
        let topic = CString::new("t").unwrap();
        let hello = CString::new("hello").unwrap();
        let mut headers = RecordHeaders::new();
        headers.add_with_key_value("k", Some(b"v")).unwrap();
        let headers = box_record_headers(headers);
        unsafe {
            let mut out = kafka_Bytes_t::NULL;
            let error = kafka_common_serialization_Serializer_serialize(
                handle,
                topic.as_ptr(),
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(bytes_of(out), Some(b"HELLO".to_vec()));
            // The C buffer is overwritten by the next call; the handle's copy
            // is what the first view pointed at, so it is a different buffer.
            assert_ne!(out.data, imp.buffer.lock().unwrap().as_ptr());

            let error = kafka_common_serialization_Serializer_serialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(bytes_of(out), Some(b"HELLO".to_vec()));

            // The owned variant has no C method: it falls back to
            // `serialize_with_headers`, the Java default chain.
            let error = kafka_common_serialization_Serializer_serialize_owned_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(bytes_of(out), Some(b"HELLO".to_vec()));

            // Null data is Java's null.
            let error =
                kafka_common_serialization_Serializer_serialize(handle, topic.as_ptr(), ptr::null(), &raw mut out);
            assert!(error.is_null());
            assert!(out.data.is_null());

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Serializer_destroy(handle);
            kafka_common_serialization_Serializer_destroy(ptr::null_mut());
        }
        // The headers reach C as the same `RecordHeaders`, printed by the
        // Rust type's own `to_string`.
        let mut expected_headers = RecordHeaders::new();
        expected_headers.add_with_key_value("k", Some(b"v")).unwrap();
        let with_headers = format!("serialize_with_headers t {expected_headers}");
        assert_eq!(
            *imp.calls.lock().unwrap(),
            [
                "serialize t",
                with_headers.as_str(),
                with_headers.as_str(),
                "serialize t"
            ]
        );
    }

    #[test]
    fn c_error_is_returned_owned_and_null_optional_methods_are_the_java_default() {
        let imp = Upper::default();
        let handle =
            kafka_common_serialization_Serializer_new(&imp as *const Upper as *mut c_void, c_serialize, None, None);
        let topic = CString::new("t").unwrap();
        let fail = CString::new("fail").unwrap();
        let headers = box_record_headers(RecordHeaders::new());
        unsafe {
            let mut out = kafka_Bytes_t::from_slice(b"untouched");
            let error = kafka_common_serialization_Serializer_serialize(
                handle,
                topic.as_ptr(),
                fail.as_ptr().cast(),
                &raw mut out,
            );
            assert!(!error.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "cannot serialize fail"
            );
            assert_eq!(bytes_of(out), Some(b"untouched".to_vec()), "the slot is left alone on error");
            kafka_common_Error_destroy(error);

            // Both header variants reach `serialize`.
            let hello = CString::new("hi").unwrap();
            let error = kafka_common_serialization_Serializer_serialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            let error = kafka_common_serialization_Serializer_serialize_owned_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(bytes_of(out), Some(b"HI".to_vec()));
            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Serializer_destroy(handle);
        }
        assert_eq!(*imp.calls.lock().unwrap(), ["serialize t", "serialize t", "serialize t"]);
    }

    #[test]
    fn owned_handles_are_consumed_and_views_are_shared() {
        let imp: Arc<DynSerializer> = Arc::new(CSerializer {
            self_: ptr::null_mut(),
            serialize: c_serialize,
            serialize_with_headers: None,
            serialize_owned_with_headers: None,
        });
        let owned = SerializerInner::owned(Arc::clone(&imp));
        assert_eq!(Arc::strong_count(&imp), 2);
        let taken = unsafe { take_serializer(owned) };
        assert!(ptr::addr_eq(Arc::as_ptr(&taken), Arc::as_ptr(&imp)));
        assert_eq!(Arc::strong_count(&imp), 2, "the owned handle was freed");
        drop(taken);

        let view = SerializerInner::view(Arc::clone(&imp));
        let shared = unsafe { take_serializer(view.as_ptr() as *mut _) };
        assert!(ptr::addr_eq(Arc::as_ptr(&shared), Arc::as_ptr(&imp)));
        assert_eq!(Arc::strong_count(&imp), 3, "the view keeps its own reference");
        drop(view);
        drop(shared);
        assert_eq!(Arc::strong_count(&imp), 1);
    }
}

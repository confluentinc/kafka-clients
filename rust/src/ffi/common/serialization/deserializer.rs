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
//! `T` is a `void *` (§4, "Generic types"): the `out_*` slot receives
//! whatever the implementation produces. A `void *` a C implementation
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
use std::mem;
use std::ops::Range;
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::Headers;
use crate::common::serialization::Deserializer;
use crate::ffi::common::header::{HeadersInner, headers_ref, kafka_common_header_Headers_t};
use crate::ffi::common::metrics::Interface;
use crate::ffi::common::{box_error, kafka_common_Error_t, take_error};
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
/// is borrowed for the call; the `void *` written to `out_deserialize` is
/// owned by the C side. Returns `NULL` or an owned error.
pub type kafka_common_serialization_Deserializer_deserialize_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    topic: *const c_char,
    data: kafka_Bytes_t,
    out_deserialize: *mut *mut c_void,
) -> *mut kafka_common_Error_t;

/// `T deserialize(String topic, Headers headers, byte[] data)` of a C
/// implementation; `headers` is a view borrowed for the call. `NULL` is the
/// Java default: `deserialize(topic, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_Headers_t,
        data: kafka_Bytes_t,
        out_deserialize_with_headers: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t,
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
        out_deserialize_from_shared: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t,
>;

/// The Rust-only `deserialize_from_shared_with_headers` of a C
/// implementation. `NULL` is the default:
/// `deserialize_with_headers(topic, headers, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_Headers_t,
        source: kafka_Bytes_t,
        data: kafka_Bytes_t,
        out_deserialize_from_shared_with_headers: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t,
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
/// `deserializer` must be a valid deserializer handle.
pub(crate) unsafe fn deserializer_ref<'a>(
    deserializer: *const kafka_common_serialization_Deserializer_t,
) -> &'a SharedDeserializer {
    unsafe { Interface::<SharedDeserializer>::from_ptr(deserializer as *const Interface<SharedDeserializer>) }.get()
}

/// Takes the implementation a `*mut` parameter received (see
/// [`Interface::take`]). The consumer is `KafkaConsumer_new` (CLAUDE.md §4
/// rule 6), which lands with the consumer bindings; until then only the
/// tests exercise it.
///
/// # Safety
///
/// `deserializer` must be a valid deserializer handle, not used again by
/// the caller when it was owned.
#[cfg_attr(not(test), expect(dead_code))]
pub(crate) unsafe fn take_deserializer(
    deserializer: *mut kafka_common_serialization_Deserializer_t,
) -> Arc<SharedDeserializer> {
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

// SAFETY: `self_` is what the C caller registered, whose thread-safety is
// the caller's responsibility as for every interface implementation
// (CLAUDE.md §4).
unsafe impl Send for CDeserializer {}
unsafe impl Sync for CDeserializer {}

/// Reads a C implementation's result: its error, or the `void *` it wrote.
///
/// # Safety
///
/// `error` must be null or an owned error.
unsafe fn collect(error: *mut kafka_common_Error_t, out: *mut c_void) -> Result<GenericValue, Error> {
    match unsafe { take_error(error) } {
        Some(error) => Err(error),
        None => Ok(GenericValue::new(out)),
    }
}

/// A `Headers_t` view on `headers`, borrowed for the call. The C side
/// receives it `const`; the mutating `Headers_*` functions take `*mut`.
///
/// # Safety
///
/// The view must be dropped before `headers` is: `HeadersInner::borrowed`
/// takes a `'static` trait-object pointer, so the borrow's lifetime is
/// erased here and re-established by the caller keeping the view local to
/// the call.
unsafe fn headers_view(headers: &dyn Headers) -> HeadersInner {
    // SAFETY: a fat pointer's layout does not depend on the trait object's
    // lifetime bound; only the bound is erased.
    let erased: *mut (dyn Headers + 'static) =
        unsafe { mem::transmute::<*const (dyn Headers + '_), *mut (dyn Headers + 'static)>(headers) };
    HeadersInner::borrowed(erased)
}

impl Deserializer<GenericValue> for CDeserializer {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        let topic = owned_c_string(topic);
        let mut out = ptr::null_mut();
        let error =
            unsafe { (self.deserialize)(self.self_, topic.as_ptr(), kafka_Bytes_t::from_slice(data), &raw mut out) };
        unsafe { collect(error, out) }
    }

    fn deserialize_with_headers(&self, topic: &str, headers: &dyn Headers, data: &[u8]) -> Result<GenericValue, Error> {
        let Some(deserialize_with_headers) = self.deserialize_with_headers else {
            return self.deserialize(topic, data);
        };
        let topic = owned_c_string(topic);
        let headers = unsafe { headers_view(headers) };
        let mut out = ptr::null_mut();
        let error = unsafe {
            deserialize_with_headers(
                self.self_,
                topic.as_ptr(),
                headers.as_ptr(),
                kafka_Bytes_t::from_slice(data),
                &raw mut out,
            )
        };
        unsafe { collect(error, out) }
    }

    fn deserialize_from_shared(&self, topic: &str, source: &Bytes, data: &[u8]) -> Result<GenericValue, Error> {
        let Some(deserialize_from_shared) = self.deserialize_from_shared else {
            return self.deserialize(topic, data);
        };
        let topic = owned_c_string(topic);
        let mut out = ptr::null_mut();
        let error = unsafe {
            deserialize_from_shared(
                self.self_,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(data),
                &raw mut out,
            )
        };
        unsafe { collect(error, out) }
    }

    fn deserialize_from_shared_with_headers(
        &self,
        topic: &str,
        headers: &dyn Headers,
        source: &Bytes,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        let Some(deserialize_from_shared_with_headers) = self.deserialize_from_shared_with_headers else {
            return self.deserialize_with_headers(topic, headers, data);
        };
        let topic = owned_c_string(topic);
        let headers = unsafe { headers_view(headers) };
        let mut out = ptr::null_mut();
        let error = unsafe {
            deserialize_from_shared_with_headers(
                self.self_,
                topic.as_ptr(),
                headers.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(data),
                &raw mut out,
            )
        };
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
        unsafe { configure(self.self_, configs, i8::from(is_key)) };
        unsafe { kafka_Map_destroy(configs) };
    }

    fn close(&mut self) {
        let Some(close) = self.close else { return };
        unsafe { close(self.self_) };
    }
}

/// Registers a C implementation of `Deserializer`; `deserialize` is
/// required, the other pointers may be `NULL` for the Java default. The
/// caller owns `self_` and keeps it alive until the handle is destroyed
/// with [`kafka_common_serialization_Deserializer_destroy`] or, once a
/// client consumed it, until that client is destroyed.
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

/// Writes a deserialized value to `out`, or returns the owned error.
///
/// # Safety
///
/// `out` must be a valid slot.
unsafe fn deliver(result: Result<GenericValue, Error>, out: *mut *mut c_void) -> *mut kafka_common_Error_t {
    match result {
        Ok(value) => {
            unsafe { *out = value.as_ptr() };
            ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// The bytes behind `data`, an empty array for Java's `null`.
///
/// # Safety
///
/// As [`kafka_Bytes_t::as_slice`].
unsafe fn slice<'a>(data: kafka_Bytes_t) -> &'a [u8] {
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
    Ok((Bytes::copy_from_slice(unsafe { slice(source) }), range))
}

/// `T deserialize(String topic, byte[] data)`: `data` is borrowed for the
/// call, `data.data == NULL` read as an empty array. On success writes the
/// implementation's `void *` to `out_deserialize`; returns the owned error
/// otherwise.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `data` null or a valid array and
/// `out_deserialize` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    data: kafka_Bytes_t,
    out_deserialize: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    unsafe { deliver(deserializer.deserialize(&topic, slice(data)), out_deserialize) }
}

/// `T deserialize(String topic, Headers headers, byte[] data)`; the value
/// is delivered as for [`kafka_common_serialization_Deserializer_deserialize`].
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid headers handle, `data` null or a
/// valid array and `out_deserialize_with_headers` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_with_headers(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_Headers_t,
    data: kafka_Bytes_t,
    out_deserialize_with_headers: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let headers = unsafe { headers_ref(headers) }.headers();
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    unsafe {
        deliver(
            deserializer.deserialize_with_headers(&topic, headers, slice(data)),
            out_deserialize_with_headers,
        )
    }
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
/// `out_deserialize_from_shared` a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_from_shared(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    source: kafka_Bytes_t,
    data: kafka_Bytes_t,
    out_deserialize_from_shared: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let (source, range) = match unsafe { shared(source, data) } {
        Ok(shared) => shared,
        Err(error) => return box_error(error),
    };
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    unsafe {
        deliver(
            deserializer.deserialize_from_shared(&topic, &source, &source[range]),
            out_deserialize_from_shared,
        )
    }
}

/// The Rust-only `deserialize_from_shared_with_headers`; `source` and
/// `data` as for [`kafka_common_serialization_Deserializer_deserialize_from_shared`].
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid headers handle, `source` and
/// `data` null or valid arrays and `out_deserialize_from_shared_with_headers`
/// a valid slot.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_Headers_t,
    source: kafka_Bytes_t,
    data: kafka_Bytes_t,
    out_deserialize_from_shared_with_headers: *mut *mut c_void,
) -> *mut kafka_common_Error_t {
    let topic = unsafe { c_str_to_string(topic) };
    let headers = unsafe { headers_ref(headers) }.headers();
    let (source, range) = match unsafe { shared(source, data) } {
        Ok(shared) => shared,
        Err(error) => return box_error(error),
    };
    let deserializer = unsafe { deserializer_ref(self_) }
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    unsafe {
        deliver(
            deserializer.deserialize_from_shared_with_headers(&topic, headers, &source, &source[range]),
            out_deserialize_from_shared_with_headers,
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_configure(
    self_: *mut kafka_common_serialization_Deserializer_t,
    configs: *const kafka_Map_t,
    is_key: i8,
) {
    let configs: HashMap<String, String> = unsafe { map_entries(configs) }
        .iter()
        .map(|&(k, v)| {
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                c_str_to_string(v as *const c_char)
            })
        })
        .collect();
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_close(
    self_: *mut kafka_common_serialization_Deserializer_t,
) {
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
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_destroy(
    self_: *mut kafka_common_serialization_Deserializer_t,
) {
    unsafe { Interface::<SharedDeserializer>::destroy(self_ as *mut Interface<SharedDeserializer>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::header::RecordHeaders;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders__as_Headers,
        kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::header::kafka_common_header_Headers_last_header;
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
        unsafe { &*(self_ as *const Upper) }
    }
    fn log(self_: *mut c_void, entry: String) {
        upper(self_).calls.lock().unwrap().push(entry);
    }
    fn text(data: kafka_Bytes_t) -> String {
        String::from_utf8(unsafe { slice(data) }.to_vec()).unwrap()
    }
    fn write_upper(data: kafka_Bytes_t, out: *mut *mut c_void) -> *mut kafka_common_Error_t {
        let text = text(data);
        if text == "fail" {
            let message = CString::new("cannot deserialize fail").unwrap();
            return unsafe { kafka_common_Error_serialization(message.as_ptr()) };
        }
        unsafe { *out = into_c_string(&text.to_uppercase()) as *mut c_void };
        ptr::null_mut()
    }
    unsafe extern "C" fn c_deserialize(
        self_: *mut c_void,
        topic: *const c_char,
        data: kafka_Bytes_t,
        out: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t {
        log(
            self_,
            format!(
                "deserialize {} {}",
                unsafe { CStr::from_ptr(topic) }.to_str().unwrap(),
                text(data)
            ),
        );
        write_upper(data, out)
    }
    unsafe extern "C" fn c_deserialize_with_headers(
        self_: *mut c_void,
        _topic: *const c_char,
        headers: *const kafka_common_header_Headers_t,
        data: kafka_Bytes_t,
        out: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t {
        let key = CString::new("k").unwrap();
        let has_k = !unsafe { kafka_common_header_Headers_last_header(headers, key.as_ptr()) }.is_null();
        log(self_, format!("deserialize_with_headers k={has_k}"));
        write_upper(data, out)
    }
    unsafe extern "C" fn c_deserialize_from_shared(
        self_: *mut c_void,
        _topic: *const c_char,
        source: kafka_Bytes_t,
        data: kafka_Bytes_t,
        out: *mut *mut c_void,
    ) -> *mut kafka_common_Error_t {
        log(self_, format!("deserialize_from_shared {} in {}", text(data), text(source)));
        write_upper(data, out)
    }
    unsafe extern "C" fn c_configure(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8) {
        let entries: Vec<String> = (0..unsafe { kafka_Map_size(configs) })
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
        let s = unsafe { CStr::from_ptr(out as *const c_char) }.to_str().unwrap().to_string();
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
        let record_headers = box_record_headers(record_headers);
        let source = b"hello world";
        unsafe {
            let headers = kafka_common_header_internals_RecordHeaders__as_Headers(record_headers);
            let mut out: *mut c_void = ptr::null_mut();
            let error = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"hello"),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "HELLO");

            let error = kafka_common_serialization_Deserializer_deserialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"hello"),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "HELLO");

            let error = kafka_common_serialization_Deserializer_deserialize_from_shared(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[6..]),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "WORLD");

            // No C method for the header-aware shared variant: it falls
            // back to `deserialize_with_headers`, the Rust default.
            let error = kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[..5]),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "HELLO");

            // Null data is an empty array.
            let error = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::NULL,
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "");

            let map = kafka_Map_new();
            let (k1, v1) = (CString::new("zeta").unwrap(), CString::new("1").unwrap());
            let (k2, v2) = (CString::new("alpha").unwrap(), CString::new("2").unwrap());
            kafka_Map_put(map, k1.as_ptr() as *mut c_void, v1.as_ptr() as *mut c_void);
            kafka_Map_put(map, k2.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void);
            kafka_common_serialization_Deserializer_configure(handle, map, 1);
            kafka_Map_destroy(map);
            kafka_common_serialization_Deserializer_close(handle);

            kafka_common_header_internals_RecordHeaders_destroy(record_headers);
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
    fn c_error_is_returned_owned_and_null_optional_methods_are_the_java_default() {
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
        let record_headers = box_record_headers(RecordHeaders::new());
        let source = b"fail ok";
        unsafe {
            let headers = kafka_common_header_internals_RecordHeaders__as_Headers(record_headers);
            let mut out: *mut c_void = ptr::dangling_mut();
            let error = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"fail"),
                &raw mut out,
            );
            assert!(!error.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "cannot deserialize fail"
            );
            assert_eq!(out, ptr::dangling_mut(), "the slot is left alone on error");
            kafka_common_Error_destroy(error);

            // Every optional method falls back to `deserialize`; one call at
            // a time, since each one owns the string it leaves in `out`.
            let out_slot = &raw mut out;
            let fallbacks: [&dyn Fn() -> *mut kafka_common_Error_t; 3] = [
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_with_headers(
                        handle,
                        topic.as_ptr(),
                        headers,
                        kafka_Bytes_t::from_slice(b"ok"),
                        out_slot,
                    )
                },
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_from_shared(
                        handle,
                        topic.as_ptr(),
                        kafka_Bytes_t::from_slice(source),
                        kafka_Bytes_t::from_slice(&source[5..]),
                        out_slot,
                    )
                },
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                        handle,
                        topic.as_ptr(),
                        headers,
                        kafka_Bytes_t::from_slice(source),
                        kafka_Bytes_t::from_slice(&source[5..]),
                        out_slot,
                    )
                },
            ];
            for fallback in fallbacks {
                let error = fallback();
                assert!(error.is_null());
                assert_eq!(take_string(out), "OK");
            }
            // No-ops: nothing to observe, nothing crashes.
            kafka_common_serialization_Deserializer_configure(handle, ptr::null(), 0);
            kafka_common_serialization_Deserializer_close(handle);

            // Data outside the source is rejected before reaching C.
            let error = kafka_common_serialization_Deserializer_deserialize_from_shared(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(b"elsewhere"),
                &raw mut out,
            );
            assert!(!error.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap(),
                "data must be a sub-array of source"
            );
            kafka_common_Error_destroy(error);

            kafka_common_header_internals_RecordHeaders_destroy(record_headers);
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

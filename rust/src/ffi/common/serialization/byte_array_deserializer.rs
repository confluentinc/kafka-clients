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

//! `kafka_common_serialization_ByteArrayDeserializer_t`:
//! `org.apache.kafka.common.serialization.ByteArrayDeserializer` (CLAUDE.md
//! §4).
//!
//! Its `void *` is an owned `kafka_Bytes_t *` — a copy of the input, as
//! Java's `byte[]` is — freed with `kafka_Bytes_destroy`.

use std::collections::HashMap;
use std::ffi::c_void;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::Headers;
use crate::common::serialization::{ByteArrayDeserializer, Deserializer};
use crate::ffi::common::serialization::DeserializerHandle;
use crate::ffi::common::serialization::deserializer::kafka_common_serialization_Deserializer_t;
use crate::ffi::ffi_guard;
use crate::ffi::util::{GenericValue, box_bytes};

/// Opaque handle to a [`ByteArrayDeserializer`].
#[repr(C)]
pub struct kafka_common_serialization_ByteArrayDeserializer_t {
    _private: [u8; 0],
}

/// A [`ByteArrayDeserializer`] over the `void *` representation: an owned
/// `kafka_Bytes_t *` freed with `kafka_Bytes_destroy`.
#[derive(Default)]
pub(crate) struct GenericByteArrayDeserializer(ByteArrayDeserializer);

/// The `void *` for a deserialized array.
fn bytes_value(result: Result<Vec<u8>, Error>) -> Result<GenericValue, Error> {
    result.map(|bytes| GenericValue::new(box_bytes(Bytes::from(bytes)) as *mut c_void))
}

impl Deserializer<GenericValue> for GenericByteArrayDeserializer {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        bytes_value(self.0.deserialize(topic, data))
    }

    fn deserialize_with_headers(&self, topic: &str, headers: &dyn Headers, data: &[u8]) -> Result<GenericValue, Error> {
        bytes_value(self.0.deserialize_with_headers(topic, headers, data))
    }

    fn deserialize_from_shared(&self, topic: &str, source: &Bytes, data: &[u8]) -> Result<GenericValue, Error> {
        bytes_value(self.0.deserialize_from_shared(topic, source, data))
    }

    fn deserialize_from_shared_with_headers(
        &self,
        topic: &str,
        headers: &dyn Headers,
        source: &Bytes,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        bytes_value(self.0.deserialize_from_shared_with_headers(topic, headers, source, data))
    }

    fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        self.0.configure(configs, is_key);
    }

    fn close(&mut self) {
        self.0.close();
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid byte-array-deserializer handle, alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_common_serialization_ByteArrayDeserializer_t,
) -> &'a DeserializerHandle<GenericByteArrayDeserializer> {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid byte-array-deserializer handle
    // alive for `'a`, the `DeserializerHandle::boxed` of
    // `kafka_common_serialization_ByteArrayDeserializer_new`, as `DeserializerHandle::from_ptr`
    // requires.
    unsafe { DeserializerHandle::from_ptr(self_ as *const DeserializerHandle<GenericByteArrayDeserializer>) }
}

/// `new ByteArrayDeserializer()`. Owned, freed with
/// [`kafka_common_serialization_ByteArrayDeserializer_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_ByteArrayDeserializer_new()
-> *mut kafka_common_serialization_ByteArrayDeserializer_t {
    DeserializerHandle::boxed(GenericByteArrayDeserializer(ByteArrayDeserializer::new()))
        as *mut kafka_common_serialization_ByteArrayDeserializer_t
}

/// The deserializer as a `Deserializer`: a borrowed view valid as long as
/// the handle, never passed to `Deserializer_destroy`; `*mut` because the
/// interface mutates. The `void *` it produces is an owned `kafka_Bytes_t *`
/// freed with `kafka_Bytes_destroy`.
///
/// # Safety
///
/// `self_` must be a valid byte-array-deserializer handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_ByteArrayDeserializer__as_Deserializer(
    self_: *mut kafka_common_serialization_ByteArrayDeserializer_t,
) -> *mut kafka_common_serialization_Deserializer_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid byte-array-deserializer handle, as
    // `handle` requires; the reference is used only within this call, and the view returned lives
    // in the handle's `OnceLock`, as long as the handle, which is the rustdoc's "valid as long as
    // the handle".
    unsafe { handle(self_) }.as_deserializer()
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned byte-array-deserializer handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_ByteArrayDeserializer_destroy(
    self_: *mut kafka_common_serialization_ByteArrayDeserializer_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned byte-array-deserializer
    // handle not yet destroyed, as `DeserializerHandle::destroy` requires: the
    // `DeserializerHandle::boxed` of `kafka_common_serialization_ByteArrayDeserializer_new`.
    unsafe { DeserializerHandle::destroy(self_ as *mut DeserializerHandle<GenericByteArrayDeserializer>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use super::*;
    use crate::common::header::RecordHeaders;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders__as_Headers,
        kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::serialization::deserializer::{
        kafka_common_serialization_Deserializer_deserialize,
        kafka_common_serialization_Deserializer_deserialize_from_shared,
        kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers,
        kafka_common_serialization_Deserializer_deserialize_with_headers,
    };
    use crate::ffi::util::{kafka_Bytes_destroy, kafka_Bytes_t};

    unsafe fn take_bytes(out: *mut c_void) -> Vec<u8> {
        let bytes = out as *mut kafka_Bytes_t;
        // SAFETY: Test code: `out` is an owned `kafka_Bytes_t *` from `box_bytes` that the
        // deserializer just returned, read before `kafka_Bytes_destroy` frees it.
        let copy = unsafe { (*bytes).as_slice() }.unwrap().to_vec();
        // SAFETY: Test code: `out` is an owned `kafka_Bytes_t *` from `box_bytes` that the
        // deserializer just returned, not yet freed and not used after this call.
        unsafe { kafka_Bytes_destroy(bytes) };
        copy
    }

    #[test]
    fn produces_an_owned_copy_through_the_view() {
        let handle = kafka_common_serialization_ByteArrayDeserializer_new();
        let topic = CString::new("t").unwrap();
        let record_headers = box_record_headers(RecordHeaders::new());
        let source = b"hello world";
        // SAFETY: Test code: `handle` comes from the class constructor and is destroyed once, at
        // the end, after the last use of its view `view`; `record_headers` comes from
        // `box_record_headers` and is destroyed once, after the last use of its `Headers` view
        // `headers`; `topic`, `source` and the arrays outlive the calls reading them; `err` is a
        // valid slot pointing at null, left null by every call (asserted), and every value returned
        // is an owned array that `take_bytes` frees once.
        unsafe {
            let view = kafka_common_serialization_ByteArrayDeserializer__as_Deserializer(handle);
            let headers = kafka_common_header_internals_RecordHeaders__as_Headers(record_headers);
            let mut err = ptr::null_mut();

            let out = kafka_common_serialization_Deserializer_deserialize(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"raw"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_ne!((*(out as *const kafka_Bytes_t)).data, b"raw".as_ptr(), "a copy");
            assert_eq!(take_bytes(out), b"raw");

            let out = kafka_common_serialization_Deserializer_deserialize_with_headers(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"raw"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_bytes(out), b"raw");

            let out = kafka_common_serialization_Deserializer_deserialize_from_shared(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[6..]),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_bytes(out), b"world");

            let out = kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[..5]),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_bytes(out), b"hello");

            // Null data is an empty array, not Java's null.
            let out = kafka_common_serialization_Deserializer_deserialize(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::NULL,
                &raw mut err,
            );
            assert!(err.is_null());
            assert!(!(*(out as *const kafka_Bytes_t)).data.is_null());
            assert_eq!(take_bytes(out), b"");

            kafka_common_header_internals_RecordHeaders_destroy(record_headers);
            kafka_common_serialization_ByteArrayDeserializer_destroy(handle);
            kafka_common_serialization_ByteArrayDeserializer_destroy(ptr::null_mut());
        }
    }
}

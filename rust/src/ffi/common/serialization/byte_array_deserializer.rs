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
use futures_util::FutureExt;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::{ByteArrayDeserializer, Deserializer};
use crate::ffi::common::serialization::DeserializerHandle;
use crate::ffi::common::serialization::deserializer::{FfiDeserializer, kafka_common_serialization_Deserializer_t};
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
    // `map` rather than `async fn`: the future holds only the inner one, which
    // keeps the consumer's wrapper within `InlineFuture`'s buffer.
    fn deserialize<'a>(
        &'a self,
        topic: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<GenericValue, Error>> + Send + 'a {
        self.0.deserialize(topic, data).map(bytes_value)
    }

    fn deserialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a [u8],
    ) -> impl Future<Output = Result<GenericValue, Error>> + Send + 'a {
        self.0.deserialize_with_headers(topic, headers, data).map(bytes_value)
    }

    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a Bytes,
    ) -> impl Future<Output = Result<GenericValue, Error>> + Send + 'a {
        self.0.deserialize_with_headers_bytes(topic, headers, data).map(bytes_value)
    }

    async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        self.0.configure(configs, is_key).await;
    }

    async fn close(&mut self) {
        self.0.close().await;
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid byte-array-deserializer handle, alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_common_serialization_ByteArrayDeserializer_t) -> &'a DeserializerHandle {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid byte-array-deserializer handle
    // alive for `'a`, the `DeserializerHandle::boxed` of
    // `kafka_common_serialization_ByteArrayDeserializer_new`, as `DeserializerHandle::from_ptr`
    // requires.
    unsafe { DeserializerHandle::from_ptr(self_ as *const DeserializerHandle) }
}

/// `new ByteArrayDeserializer()`.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_serialization_ByteArrayDeserializer_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_ByteArrayDeserializer_new()
-> *mut kafka_common_serialization_ByteArrayDeserializer_t {
    DeserializerHandle::boxed(FfiDeserializer::ByteArray(GenericByteArrayDeserializer(
        ByteArrayDeserializer::new(),
    ))) as *mut kafka_common_serialization_ByteArrayDeserializer_t
}

/// Returns the deserializer as a `Deserializer`; `*mut` because the interface mutates. The `void *` it produces is an owned `kafka_Bytes_t *`
/// freed with `kafka_Bytes_destroy`.
///
/// # Safety
///
/// `self_` must be a valid byte-array-deserializer handle.
///
/// The returned view is borrowed, valid as long as `self_`, and is never passed to
/// `kafka_common_serialization_Deserializer_destroy`.
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
    unsafe { DeserializerHandle::destroy(self_ as *mut DeserializerHandle) }
}

#[cfg(test)]
mod tests {
    use std::ffi::CString;
    use std::ptr;

    use super::*;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::serialization::deserializer::{
        kafka_common_serialization_Deserializer_deserialize,
        kafka_common_serialization_Deserializer_deserialize_with_headers,
        kafka_common_serialization_Deserializer_deserialize_with_headers_bytes,
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
        let headers = box_record_headers(RecordHeaders::new());
        let source = b"hello world";
        // SAFETY: Test code: `handle` comes from the class constructor and is destroyed once, at
        // the end, after the last use of its view `view`; `headers` comes from
        // `box_record_headers` and is destroyed once, after the last call reading it; `topic`,
        // `source` and the arrays outlive the calls reading them; `err` is a valid slot pointing at
        // null, left null by every call (asserted), and every value returned is an owned array that
        // `take_bytes` frees once.
        unsafe {
            let view = kafka_common_serialization_ByteArrayDeserializer__as_Deserializer(handle);
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

            let out = kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(&source[6..]),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_bytes(out), b"world");

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

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_ByteArrayDeserializer_destroy(handle);
            kafka_common_serialization_ByteArrayDeserializer_destroy(ptr::null_mut());
        }
    }
}

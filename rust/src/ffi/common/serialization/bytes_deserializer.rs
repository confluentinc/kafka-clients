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

//! `kafka_common_serialization_BytesDeserializer_t`:
//! `org.apache.kafka.common.serialization.BytesDeserializer` (CLAUDE.md
//! §4).
//!
//! Its `void *` is an owned `kafka_Bytes_t *` freed with
//! `kafka_Bytes_destroy`. Through the plain `deserialize` it is a copy; through
//! `deserialize_from_shared` it is a slice sharing the source buffer the
//! invoker copied once, so many records from one source cost one copy.

use std::collections::HashMap;
use std::ffi::c_void;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::Headers;
use crate::common::serialization::{BytesDeserializer, Deserializer};
use crate::ffi::common::serialization::DeserializerHandle;
use crate::ffi::common::serialization::deserializer::kafka_common_serialization_Deserializer_t;
use crate::ffi::util::{GenericValue, box_bytes};

/// Opaque handle to a [`BytesDeserializer`].
#[repr(C)]
pub struct kafka_common_serialization_BytesDeserializer_t {
    _private: [u8; 0],
}

/// A [`BytesDeserializer`] over the `void *` representation: an owned
/// `kafka_Bytes_t *` freed with `kafka_Bytes_destroy`.
#[derive(Default)]
pub(crate) struct GenericBytesDeserializer(BytesDeserializer);

/// The `void *` for deserialized bytes.
fn bytes_value(result: Result<Bytes, Error>) -> Result<GenericValue, Error> {
    result.map(|bytes| GenericValue::new(box_bytes(bytes) as *mut c_void))
}

impl Deserializer<GenericValue> for GenericBytesDeserializer {
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
/// `self_` must be a valid bytes-deserializer handle.
unsafe fn handle<'a>(
    self_: *const kafka_common_serialization_BytesDeserializer_t,
) -> &'a DeserializerHandle<GenericBytesDeserializer> {
    unsafe { DeserializerHandle::from_ptr(self_ as *const DeserializerHandle<GenericBytesDeserializer>) }
}

/// `new BytesDeserializer()`. Owned, freed with
/// [`kafka_common_serialization_BytesDeserializer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_BytesDeserializer_new()
-> *mut kafka_common_serialization_BytesDeserializer_t {
    DeserializerHandle::boxed(GenericBytesDeserializer(BytesDeserializer::new()))
        as *mut kafka_common_serialization_BytesDeserializer_t
}

/// The deserializer as a `Deserializer`: a borrowed view valid as long as
/// the handle, never passed to `Deserializer_destroy`; `*mut` because the
/// interface mutates. The `void *` it produces is an owned `kafka_Bytes_t *`
/// freed with `kafka_Bytes_destroy`.
///
/// # Safety
///
/// `self_` must be a valid bytes-deserializer handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_BytesDeserializer__as_Deserializer(
    self_: *mut kafka_common_serialization_BytesDeserializer_t,
) -> *mut kafka_common_serialization_Deserializer_t {
    unsafe { handle(self_) }.as_deserializer()
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned bytes-deserializer handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_BytesDeserializer_destroy(
    self_: *mut kafka_common_serialization_BytesDeserializer_t,
) {
    unsafe { DeserializerHandle::destroy(self_ as *mut DeserializerHandle<GenericBytesDeserializer>) }
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
        let copy = unsafe { (*bytes).as_slice() }.unwrap().to_vec();
        unsafe { kafka_Bytes_destroy(bytes) };
        copy
    }

    #[test]
    fn produces_owned_bytes_through_the_view() {
        let handle = kafka_common_serialization_BytesDeserializer_new();
        let topic = CString::new("t").unwrap();
        let record_headers = box_record_headers(RecordHeaders::new());
        let source = b"hello world";
        unsafe {
            let view = kafka_common_serialization_BytesDeserializer__as_Deserializer(handle);
            let headers = kafka_common_header_internals_RecordHeaders__as_Headers(record_headers);
            let mut out: *mut c_void = ptr::null_mut();

            let error = kafka_common_serialization_Deserializer_deserialize(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"raw"),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_bytes(out), b"raw");

            let error = kafka_common_serialization_Deserializer_deserialize_with_headers(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"raw"),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_bytes(out), b"raw");

            // The shared path slices the invoker's copy of the source: the
            // two results point into one buffer, `len` bytes apart.
            let mut out2: *mut c_void = ptr::null_mut();
            let error = kafka_common_serialization_Deserializer_deserialize_from_shared(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[6..]),
                &raw mut out,
            );
            assert!(error.is_null());
            let error = kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[..5]),
                &raw mut out2,
            );
            assert!(error.is_null());
            assert_eq!(take_bytes(out), b"world");
            assert_eq!(take_bytes(out2), b"hello");

            kafka_common_header_internals_RecordHeaders_destroy(record_headers);
            kafka_common_serialization_BytesDeserializer_destroy(handle);
            kafka_common_serialization_BytesDeserializer_destroy(ptr::null_mut());
        }
    }
}

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

//! `kafka_common_serialization_ByteArraySerializer_t`:
//! `org.apache.kafka.common.serialization.ByteArraySerializer` (CLAUDE.md
//! §4).
//!
//! Its `void *` is a `const kafka_Bytes_t *`; a `NULL` pointer or a
//! `NULL` `data` inside the array is Java's `null`. One `__as_Serializer`
//! view serves both Rust impls (`Serializer<[u8]>` and `Serializer<Vec<u8>>`):
//! over a `void *` the bytes are always copied, so the zero-copy owned path
//! has nothing to pass through.

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::{ByteArraySerializer, Serializer};
use crate::ffi::common::serialization::SerializerHandle;
use crate::ffi::common::serialization::serializer::kafka_common_serialization_Serializer_t;
use crate::ffi::util::{GenericValue, kafka_Bytes_t};

/// Opaque handle to a [`ByteArraySerializer`].
#[repr(C)]
pub struct kafka_common_serialization_ByteArraySerializer_t {
    _private: [u8; 0],
}

/// A [`ByteArraySerializer`] over the `void *` representation: a
/// `const kafka_Bytes_t *`.
#[derive(Default)]
pub(crate) struct GenericByteArraySerializer(ByteArraySerializer);

/// The bytes behind `data`, `None` for Java's `null`.
///
/// # Safety
///
/// `data` must be null or point at a `kafka_Bytes_t` whose `data` is null
/// or points at `len` readable bytes.
unsafe fn bytes<'a>(data: Option<&GenericValue>) -> Option<&'a [u8]> {
    let data = data.filter(|value| !value.is_null())?;
    unsafe { (*(data.as_ptr() as *const kafka_Bytes_t)).as_slice() }
}

impl Serializer<GenericValue> for GenericByteArraySerializer {
    fn serialize(&self, topic: &str, data: Option<&GenericValue>) -> Result<Option<Vec<u8>>, Error> {
        Serializer::<[u8]>::serialize(&self.0, topic, unsafe { bytes(data) })
    }

    fn serialize_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<&GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        Serializer::<[u8]>::serialize_with_headers(&self.0, topic, headers, unsafe { bytes(data) })
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid byte-array-serializer handle.
unsafe fn handle<'a>(
    self_: *const kafka_common_serialization_ByteArraySerializer_t,
) -> &'a SerializerHandle<GenericByteArraySerializer> {
    unsafe { SerializerHandle::from_ptr(self_ as *const SerializerHandle<GenericByteArraySerializer>) }
}

/// `new ByteArraySerializer()`. Owned, freed with
/// [`kafka_common_serialization_ByteArraySerializer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_ByteArraySerializer_new()
-> *mut kafka_common_serialization_ByteArraySerializer_t {
    SerializerHandle::boxed(GenericByteArraySerializer(ByteArraySerializer::new()))
        as *mut kafka_common_serialization_ByteArraySerializer_t
}

/// The serializer as a `Serializer`: a borrowed view valid as long as the
/// handle, never passed to `Serializer_destroy`. Its `data` is a
/// `const kafka_Bytes_t *`, copied during the call.
///
/// # Safety
///
/// `self_` must be a valid byte-array-serializer handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_ByteArraySerializer__as_Serializer(
    self_: *const kafka_common_serialization_ByteArraySerializer_t,
) -> *const kafka_common_serialization_Serializer_t {
    unsafe { handle(self_) }.as_serializer()
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned byte-array-serializer handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_ByteArraySerializer_destroy(
    self_: *mut kafka_common_serialization_ByteArraySerializer_t,
) {
    unsafe { SerializerHandle::destroy(self_ as *mut SerializerHandle<GenericByteArraySerializer>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CString, c_void};
    use std::ptr;

    use super::*;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::serialization::serializer::{
        kafka_common_serialization_Serializer_serialize,
        kafka_common_serialization_Serializer_serialize_owned_with_headers,
        kafka_common_serialization_Serializer_serialize_with_headers,
    };

    #[test]
    fn copies_the_array_through_the_view() {
        let handle = kafka_common_serialization_ByteArraySerializer_new();
        let topic = CString::new("t").unwrap();
        let payload = kafka_Bytes_t::from_slice(b"raw");
        let headers = box_record_headers(RecordHeaders::new());
        unsafe {
            let view = kafka_common_serialization_ByteArraySerializer__as_Serializer(handle);
            let mut out = kafka_Bytes_t::NULL;
            for error in [
                kafka_common_serialization_Serializer_serialize(
                    view,
                    topic.as_ptr(),
                    &raw const payload as *const c_void,
                    &raw mut out,
                ),
                kafka_common_serialization_Serializer_serialize_with_headers(
                    view,
                    topic.as_ptr(),
                    headers,
                    &raw const payload as *const c_void,
                    &raw mut out,
                ),
                kafka_common_serialization_Serializer_serialize_owned_with_headers(
                    view,
                    topic.as_ptr(),
                    headers,
                    &raw const payload as *const c_void,
                    &raw mut out,
                ),
            ] {
                assert!(error.is_null());
                assert_eq!(out.as_slice(), Some(&b"raw"[..]));
                assert_ne!(out.data, payload.data, "a copy, not the caller's buffer");
            }

            // Java's null, either way.
            let error =
                kafka_common_serialization_Serializer_serialize(view, topic.as_ptr(), ptr::null(), &raw mut out);
            assert!(error.is_null());
            assert!(out.data.is_null());
            let null_array = kafka_Bytes_t::NULL;
            let error = kafka_common_serialization_Serializer_serialize(
                view,
                topic.as_ptr(),
                &raw const null_array as *const c_void,
                &raw mut out,
            );
            assert!(error.is_null());
            assert!(out.data.is_null());

            // An empty array stays an empty array.
            let empty = kafka_Bytes_t::from_slice(b"");
            let error = kafka_common_serialization_Serializer_serialize(
                view,
                topic.as_ptr(),
                &raw const empty as *const c_void,
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(out.as_slice(), Some(&b""[..]));

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_ByteArraySerializer_destroy(handle);
            kafka_common_serialization_ByteArraySerializer_destroy(ptr::null_mut());
        }
    }
}

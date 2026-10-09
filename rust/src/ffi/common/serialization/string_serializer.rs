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

//! `kafka_common_serialization_StringSerializer_t`:
//! `org.apache.kafka.common.serialization.StringSerializer` (CLAUDE.md §4).
//!
//! Its `void *` is a NUL-terminated UTF-8 `const char *`, `NULL` being
//! Java's `null`. One `__as_Serializer` view serves both Rust impls
//! (`Serializer<str>` and `Serializer<String>`): over a `void *` they are
//! the same call.

use std::ffi::{CStr, c_char};

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::{Serializer, StringSerializer};
use crate::ffi::common::serialization::SerializerHandle;
use crate::ffi::common::serialization::serializer::kafka_common_serialization_Serializer_t;
use crate::ffi::util::GenericValue;

/// Opaque handle to a [`StringSerializer`].
#[repr(C)]
pub struct kafka_common_serialization_StringSerializer_t {
    _private: [u8; 0],
}

/// A [`StringSerializer`] over the `void *` representation: a
/// NUL-terminated UTF-8 `const char *`.
#[derive(Default)]
pub(crate) struct GenericStringSerializer(StringSerializer);

/// The string behind `data`, `None` for Java's `null`; the translation of
/// `SerializationException` when the bytes are not UTF-8.
///
/// # Safety
///
/// `data` must be null or point at a NUL-terminated string.
unsafe fn string<'a>(data: Option<&GenericValue>) -> Result<Option<&'a str>, Error> {
    let Some(data) = data.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    unsafe { CStr::from_ptr(data.as_ptr() as *const c_char) }
        .to_str()
        .map(Some)
        .map_err(|e| Error::serialization(format!("Error when serializing string to byte[]: {e}")))
}

impl Serializer<GenericValue> for GenericStringSerializer {
    fn serialize(&self, topic: &str, data: Option<&GenericValue>) -> Result<Option<Vec<u8>>, Error> {
        Serializer::<str>::serialize(&self.0, topic, unsafe { string(data) }?)
    }

    fn serialize_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: Option<&GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        Serializer::<str>::serialize_with_headers(&self.0, topic, headers, unsafe { string(data) }?)
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid string-serializer handle.
unsafe fn handle<'a>(
    self_: *const kafka_common_serialization_StringSerializer_t,
) -> &'a SerializerHandle<GenericStringSerializer> {
    unsafe { SerializerHandle::from_ptr(self_ as *const SerializerHandle<GenericStringSerializer>) }
}

/// `new StringSerializer()`. Owned, freed with
/// [`kafka_common_serialization_StringSerializer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_StringSerializer_new() -> *mut kafka_common_serialization_StringSerializer_t
{
    SerializerHandle::boxed(GenericStringSerializer(StringSerializer::new()))
        as *mut kafka_common_serialization_StringSerializer_t
}

/// The serializer as a `Serializer`: a borrowed view valid as long as the
/// handle, never passed to `Serializer_destroy`. Its `data` is a
/// NUL-terminated UTF-8 `const char *`.
///
/// # Safety
///
/// `self_` must be a valid string-serializer handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_StringSerializer__as_Serializer(
    self_: *const kafka_common_serialization_StringSerializer_t,
) -> *const kafka_common_serialization_Serializer_t {
    unsafe { handle(self_) }.as_serializer()
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned string-serializer handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_StringSerializer_destroy(
    self_: *mut kafka_common_serialization_StringSerializer_t,
) {
    unsafe { SerializerHandle::destroy(self_ as *mut SerializerHandle<GenericStringSerializer>) }
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
        kafka_common_serialization_Serializer_serialize, kafka_common_serialization_Serializer_serialize_with_headers,
    };
    use crate::ffi::common::{kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::util::kafka_Bytes_t;

    #[test]
    fn serializes_a_c_string_through_the_view() {
        let handle = kafka_common_serialization_StringSerializer_new();
        let topic = CString::new("t").unwrap();
        let hello = CString::new("héllo").unwrap();
        let headers = box_record_headers(RecordHeaders::new());
        unsafe {
            let view = kafka_common_serialization_StringSerializer__as_Serializer(handle);
            assert_eq!(view, kafka_common_serialization_StringSerializer__as_Serializer(handle));
            let mut out = kafka_Bytes_t::NULL;
            let error = kafka_common_serialization_Serializer_serialize(
                view,
                topic.as_ptr(),
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(out.as_slice(), Some("héllo".as_bytes()));

            let error = kafka_common_serialization_Serializer_serialize_with_headers(
                view,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(out.as_slice(), Some("héllo".as_bytes()));

            // Null is Java's null, serialized to a null array.
            let error =
                kafka_common_serialization_Serializer_serialize(view, topic.as_ptr(), ptr::null(), &raw mut out);
            assert!(error.is_null());
            assert!(out.data.is_null());

            // Not UTF-8: the translation of `SerializationException`.
            let latin1 = [b'h', 0xE9, b'l', 0];
            let error = kafka_common_serialization_Serializer_serialize(
                view,
                topic.as_ptr(),
                latin1.as_ptr() as *const c_void,
                &raw mut out,
            );
            assert!(!error.is_null());
            let message = CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap();
            assert!(message.starts_with("Error when serializing string to byte[]: "), "{message}");
            kafka_common_Error_destroy(error);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_StringSerializer_destroy(handle);
            kafka_common_serialization_StringSerializer_destroy(ptr::null_mut());
        }
    }
}

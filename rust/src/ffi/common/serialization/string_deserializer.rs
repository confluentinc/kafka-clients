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

//! `kafka_common_serialization_StringDeserializer_t`:
//! `org.apache.kafka.common.serialization.StringDeserializer` (CLAUDE.md
//! §4).
//!
//! Its `void *` is an owned NUL-terminated `char *` freed with
//! `kafka_string_destroy`. Malformed UTF-8 is replaced with U+FFFD as in
//! Java's `new String(data, UTF_8)`; an interior NUL, which a C string
//! cannot carry, truncates the string.

use std::collections::HashMap;
use std::ffi::c_void;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::Headers;
use crate::common::serialization::{Deserializer, StringDeserializer};
use crate::ffi::common::serialization::DeserializerHandle;
use crate::ffi::common::serialization::deserializer::kafka_common_serialization_Deserializer_t;
use crate::ffi::util::{GenericValue, into_c_string};

/// Opaque handle to a [`StringDeserializer`].
#[repr(C)]
pub struct kafka_common_serialization_StringDeserializer_t {
    _private: [u8; 0],
}

/// A [`StringDeserializer`] over the `void *` representation: an owned
/// `char *` freed with `kafka_string_destroy`.
#[derive(Default)]
pub(crate) struct GenericStringDeserializer(StringDeserializer);

/// The `void *` for a deserialized string.
fn string_value(result: Result<String, Error>) -> Result<GenericValue, Error> {
    result.map(|s| GenericValue::new(into_c_string(&s) as *mut c_void))
}

impl Deserializer<GenericValue> for GenericStringDeserializer {
    fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        string_value(self.0.deserialize(topic, data))
    }

    fn deserialize_with_headers(&self, topic: &str, headers: &dyn Headers, data: &[u8]) -> Result<GenericValue, Error> {
        string_value(self.0.deserialize_with_headers(topic, headers, data))
    }

    fn deserialize_from_shared(&self, topic: &str, source: &Bytes, data: &[u8]) -> Result<GenericValue, Error> {
        string_value(self.0.deserialize_from_shared(topic, source, data))
    }

    fn deserialize_from_shared_with_headers(
        &self,
        topic: &str,
        headers: &dyn Headers,
        source: &Bytes,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        string_value(self.0.deserialize_from_shared_with_headers(topic, headers, source, data))
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
/// `self_` must be a valid string-deserializer handle.
unsafe fn handle<'a>(
    self_: *const kafka_common_serialization_StringDeserializer_t,
) -> &'a DeserializerHandle<GenericStringDeserializer> {
    unsafe { DeserializerHandle::from_ptr(self_ as *const DeserializerHandle<GenericStringDeserializer>) }
}

/// `new StringDeserializer()`. Owned, freed with
/// [`kafka_common_serialization_StringDeserializer_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_StringDeserializer_new()
-> *mut kafka_common_serialization_StringDeserializer_t {
    DeserializerHandle::boxed(GenericStringDeserializer(StringDeserializer::new()))
        as *mut kafka_common_serialization_StringDeserializer_t
}

/// The deserializer as a `Deserializer`: a borrowed view valid as long as
/// the handle, never passed to `Deserializer_destroy`; `*mut` because the
/// interface mutates. The `void *` it produces is an owned `char *` freed
/// with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid string-deserializer handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_StringDeserializer__as_Deserializer(
    self_: *mut kafka_common_serialization_StringDeserializer_t,
) -> *mut kafka_common_serialization_Deserializer_t {
    unsafe { handle(self_) }.as_deserializer()
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned string-deserializer handle not yet
/// destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_StringDeserializer_destroy(
    self_: *mut kafka_common_serialization_StringDeserializer_t,
) {
    unsafe { DeserializerHandle::destroy(self_ as *mut DeserializerHandle<GenericStringDeserializer>) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString, c_char};
    use std::ptr;

    use super::*;
    use crate::common::header::RecordHeaders;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders__as_Headers,
        kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::serialization::deserializer::{
        kafka_common_serialization_Deserializer_close, kafka_common_serialization_Deserializer_configure,
        kafka_common_serialization_Deserializer_deserialize,
        kafka_common_serialization_Deserializer_deserialize_from_shared,
        kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers,
        kafka_common_serialization_Deserializer_deserialize_with_headers,
    };
    use crate::ffi::util::{kafka_Bytes_t, kafka_string_destroy};

    unsafe fn take_string(out: *mut c_void) -> String {
        let s = unsafe { CStr::from_ptr(out as *const c_char) }.to_str().unwrap().to_string();
        unsafe { kafka_string_destroy(out as *mut c_char) };
        s
    }

    #[test]
    fn produces_an_owned_c_string_through_the_view() {
        let handle = kafka_common_serialization_StringDeserializer_new();
        let topic = CString::new("t").unwrap();
        let record_headers = box_record_headers(RecordHeaders::new());
        let source = b"hello world";
        unsafe {
            let view = kafka_common_serialization_StringDeserializer__as_Deserializer(handle);
            assert_eq!(view, kafka_common_serialization_StringDeserializer__as_Deserializer(handle));
            let headers = kafka_common_header_internals_RecordHeaders__as_Headers(record_headers);
            let mut out: *mut c_void = ptr::null_mut();

            let error = kafka_common_serialization_Deserializer_deserialize(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice("héllo".as_bytes()),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "héllo");

            let error = kafka_common_serialization_Deserializer_deserialize_with_headers(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"hi"),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "hi");

            let error = kafka_common_serialization_Deserializer_deserialize_from_shared(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[6..]),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "world");

            let error = kafka_common_serialization_Deserializer_deserialize_from_shared_with_headers(
                view,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(source),
                kafka_Bytes_t::from_slice(&source[..5]),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "hello");

            // Malformed UTF-8 is replaced, as Java does; an interior NUL
            // truncates, as a C string must.
            let error = kafka_common_serialization_Deserializer_deserialize(
                view,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(&[b'a', 0xFF, b'b', 0, b'c']),
                &raw mut out,
            );
            assert!(error.is_null());
            assert_eq!(take_string(out), "a\u{FFFD}b");

            // The Java defaults: nothing to observe, nothing crashes.
            kafka_common_serialization_Deserializer_configure(view, ptr::null(), 1);
            kafka_common_serialization_Deserializer_close(view);

            kafka_common_header_internals_RecordHeaders_destroy(record_headers);
            kafka_common_serialization_StringDeserializer_destroy(handle);
            kafka_common_serialization_StringDeserializer_destroy(ptr::null_mut());
        }
    }
}

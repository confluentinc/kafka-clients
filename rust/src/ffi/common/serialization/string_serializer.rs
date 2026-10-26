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

use std::borrow::Cow;
use std::collections::HashMap;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::{Serializer, StringSerializer};
use crate::ffi::common::serialization::SerializerHandle;
use crate::ffi::common::serialization::serializer::{FfiSerializer, kafka_common_serialization_Serializer_t};
use crate::ffi::ffi_guard;
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
/// `data` must be null or point at a NUL-terminated string that outlives
/// `'a` and is not written during it.
unsafe fn string<'a>(data: Option<&GenericValue>) -> Result<Option<&'a str>, Error> {
    let Some(data) = data.filter(|value| !value.is_null()) else {
        return Ok(None);
    };
    // SAFETY: Per this function's `# Safety`, `data` is non-null here and points at a
    // NUL-terminated string that outlives `'a` and is not written during it, as `CStr::from_ptr`
    // requires.
    unsafe { CStr::from_ptr(data.as_ptr() as *const c_char) }
        .to_str()
        .map(Some)
        .map_err(|e| Error::serialization(format!("Error when serializing string to byte[]: {e}")))
}

impl Serializer<GenericValue> for GenericStringSerializer {
    async fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        // SAFETY: A `GenericValue` reaching this implementation is the `void *` a C caller passed
        // with this serializer: through a `kafka_common_serialization_Serializer_serialize*`
        // invoker on the view `kafka_common_serialization_StringSerializer__as_Serializer`
        // returns, whose rustdoc documents a NUL-terminated UTF-8 `const char *` as its `data`, and whose `# Safety` requires
        // `data` to be null or that representation, alive and unchanged for the call, which is `'a`
        // here, so `string`'s `# Safety` holds for `'a`. The bytes returned may borrow it for `'a`:
        // the invoker copies them out before it returns.
        let data: Option<&'a str> = unsafe { string(data) }?;
        Serializer::<str>::serialize(&self.0, topic, data).await
    }

    async fn serialize_with_headers<'a, 'h>(
        &'a self,
        topic: &'a str,
        headers: &'h mut RecordHeaders,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error>
    where
        'a: 'h,
    {
        // SAFETY: A `GenericValue` reaching this implementation is the `void *` a C caller passed
        // with this serializer: through a `kafka_common_serialization_Serializer_serialize*`
        // invoker on the view `kafka_common_serialization_StringSerializer__as_Serializer`
        // returns, whose rustdoc documents a NUL-terminated UTF-8 `const char *` as its `data`, and whose `# Safety` requires
        // `data` to be null or that representation, alive and unchanged for the call, which is `'a`
        // here, so `string`'s `# Safety` holds for `'a`. The bytes returned may borrow it for `'a`:
        // the invoker copies them out before it returns.
        let data: Option<&'a str> = unsafe { string(data) }?;
        Serializer::<str>::serialize_with_headers(&self.0, topic, headers, data).await
    }
    async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        Serializer::<str>::configure(&mut self.0, configs, is_key).await;
    }

    async fn close(&self) {
        Serializer::<str>::close(&self.0).await;
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `self_` must be a valid string-serializer handle, alive for `'a`.
unsafe fn handle<'a>(self_: *const kafka_common_serialization_StringSerializer_t) -> &'a SerializerHandle {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid string-serializer handle alive for
    // `'a`, the `SerializerHandle::boxed` of `kafka_common_serialization_StringSerializer_new`, as
    // `SerializerHandle::from_ptr` requires.
    unsafe { SerializerHandle::from_ptr(self_ as *const SerializerHandle) }
}

/// `new StringSerializer()`.
///
/// WARNING: the encoding is always UTF-8; the `key.serializer.encoding`,
/// `value.serializer.encoding` and `serializer.encoding` properties are
/// ignored.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_serialization_StringSerializer_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_StringSerializer_new() -> *mut kafka_common_serialization_StringSerializer_t
{
    SerializerHandle::boxed(FfiSerializer::String(GenericStringSerializer(StringSerializer::new())))
        as *mut kafka_common_serialization_StringSerializer_t
}

/// Returns the serializer as a `Serializer`. Its `data` is a
/// NUL-terminated UTF-8 `const char *`.
///
/// # Safety
///
/// `self_` must be a valid string-serializer handle.
///
/// The returned view is borrowed, valid as long as `self_`, and is never passed to
/// `kafka_common_serialization_Serializer_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_StringSerializer__as_Serializer(
    self_: *mut kafka_common_serialization_StringSerializer_t,
) -> *mut kafka_common_serialization_Serializer_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid string-serializer handle, as
    // `handle` requires; the reference is used only within this call, and the view returned lives
    // in the handle's `OnceLock`, as long as the handle, which is the rustdoc's "valid as long as
    // the handle".
    unsafe { handle(self_) }.as_serializer().cast_mut()
}

/// Frees an owned handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned string-serializer handle not yet
/// destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_StringSerializer_destroy(
    self_: *mut kafka_common_serialization_StringSerializer_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned string-serializer handle
    // not yet destroyed, as `SerializerHandle::destroy` requires: the `SerializerHandle::boxed` of
    // `kafka_common_serialization_StringSerializer_new`.
    unsafe { SerializerHandle::destroy(self_ as *mut SerializerHandle) }
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

    #[test]
    fn serializes_a_c_string_through_the_view() {
        let handle = kafka_common_serialization_StringSerializer_new();
        let topic = CString::new("t").unwrap();
        let hello = CString::new("héllo").unwrap();
        let headers = box_record_headers(RecordHeaders::new());
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_StringSerializer_new`
        // and is destroyed once, at the end, after the last use of its view `view`; `headers` comes
        // from `box_record_headers` and is destroyed once, after its last use; `topic`, `hello` and
        // `latin1` outlive the calls reading them, and `latin1` is NUL-terminated; `err` is a valid
        // slot pointing at null, left null by every call but the last (asserted); each returned
        // view is read before the next call on the handle; the error is destroyed once, after it is
        // read.
        unsafe {
            let view = kafka_common_serialization_StringSerializer__as_Serializer(handle);
            assert_eq!(view, kafka_common_serialization_StringSerializer__as_Serializer(handle));
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Serializer_serialize(
                view,
                topic.as_ptr(),
                hello.as_ptr().cast(),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(out.as_slice(), Some("héllo".as_bytes()));

            let out = kafka_common_serialization_Serializer_serialize_with_headers(
                view,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(out.as_slice(), Some("héllo".as_bytes()));

            // Null is Java's null, serialized to a null array.
            let out = kafka_common_serialization_Serializer_serialize(view, topic.as_ptr(), ptr::null(), &raw mut err);
            assert!(err.is_null());
            assert!(out.data.is_null());

            // Not UTF-8: the translation of `SerializationException`.
            let latin1 = [b'h', 0xE9, b'l', 0];
            let out = kafka_common_serialization_Serializer_serialize(
                view,
                topic.as_ptr(),
                latin1.as_ptr() as *const c_void,
                &raw mut err,
            );
            assert!(!err.is_null());
            assert!(out.data.is_null(), "a failure returns the null array");
            let message = CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap();
            assert!(message.starts_with("Error when serializing string to byte[]: "), "{message}");
            kafka_common_Error_destroy(err);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_StringSerializer_destroy(handle);
            kafka_common_serialization_StringSerializer_destroy(ptr::null_mut());
        }
    }
}

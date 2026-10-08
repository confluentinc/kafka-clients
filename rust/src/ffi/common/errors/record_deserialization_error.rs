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

//! `kafka_common_RecordDeserializationError_t`:
//! `org.apache.kafka.common.errors.RecordDeserializationException`, with its
//! nested `DeserializationExceptionOrigin` enum as
//! `kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t`
//! (CLAUDE.md §4, "Nested types" and "Enums").
//!
//! The structured getters (`topic_partition`, `headers`) return handles the
//! payload owns, valid as long as it is.

#![expect(non_camel_case_types)]

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::common::errors::{DeserializationErrorOrigin, RecordDeserializationError};
use crate::ffi::common::errors::{Payload, PayloadClass, take_source};
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::record::timestamp_type::{self, kafka_common_record_TimestampType_t};
use crate::ffi::common::topic_partition::{TopicPartitionInner, kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::util::{c_str_to_string, kafka_Bytes_t};

// ---------------------------------------------------------------------------
// RecordDeserializationException.DeserializationExceptionOrigin
// ---------------------------------------------------------------------------

/// Opaque handle to a [`DeserializationErrorOrigin`] singleton.
#[repr(C)]
pub struct kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t {
    _private: [u8; 0],
}

/// The values of [`DeserializationErrorOrigin`], for a C `switch`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum kafka_common_RecordDeserializationError_DeserializationErrorOrigin_e {
    kafka_common_RecordDeserializationError_DeserializationErrorOrigin_KEY,
    kafka_common_RecordDeserializationError_DeserializationErrorOrigin_VALUE,
}

/// One static instance per value, indexed by
/// [`kafka_common_RecordDeserializationError_DeserializationErrorOrigin_e`].
static ORIGINS: [DeserializationErrorOrigin; 2] = [DeserializationErrorOrigin::Key, DeserializationErrorOrigin::Value];

/// Exhaustive, so a value Java adds fails to compile until it has its C
/// enumerator and singleton.
fn origin_enum_of(
    origin: DeserializationErrorOrigin,
) -> kafka_common_RecordDeserializationError_DeserializationErrorOrigin_e {
    match origin {
        DeserializationErrorOrigin::Key => kafka_common_RecordDeserializationError_DeserializationErrorOrigin_e::kafka_common_RecordDeserializationError_DeserializationErrorOrigin_KEY,
        DeserializationErrorOrigin::Value => {
            kafka_common_RecordDeserializationError_DeserializationErrorOrigin_e::kafka_common_RecordDeserializationError_DeserializationErrorOrigin_VALUE
        },
    }
}

/// The borrowed singleton standing for `origin`.
pub(crate) fn origin_singleton(
    origin: DeserializationErrorOrigin,
) -> *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t {
    &ORIGINS[origin_enum_of(origin) as usize] as *const DeserializationErrorOrigin
        as *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t
}

/// The value behind a singleton.
///
/// # Safety
///
/// `origin` must be a singleton returned by this module.
pub(crate) unsafe fn origin_value_of(
    origin: *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t,
) -> DeserializationErrorOrigin {
    unsafe { *(origin as *const DeserializationErrorOrigin) }
}

/// `DeserializationExceptionOrigin.KEY`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_RecordDeserializationError_DeserializationErrorOrigin_key()
-> *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t {
    origin_singleton(DeserializationErrorOrigin::Key)
}

/// `DeserializationExceptionOrigin.VALUE`.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_RecordDeserializationError_DeserializationErrorOrigin_value()
-> *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t {
    origin_singleton(DeserializationErrorOrigin::Value)
}

/// The C enumerator of a singleton, for a `switch`.
///
/// # Safety
///
/// `self_` must be a singleton returned by this module.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_DeserializationErrorOrigin__enum(
    self_: *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t,
) -> kafka_common_RecordDeserializationError_DeserializationErrorOrigin_e {
    origin_enum_of(unsafe { origin_value_of(self_) })
}

// ---------------------------------------------------------------------------
// RecordDeserializationException
// ---------------------------------------------------------------------------

/// Opaque handle to a [`RecordDeserializationError`].
#[repr(C)]
pub struct kafka_common_RecordDeserializationError_t {
    _private: [u8; 0],
}

impl PayloadClass for RecordDeserializationError {
    fn message(&self) -> &str {
        RecordDeserializationError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        RecordDeserializationError::source(self)
    }
}

/// The handles the structured getters borrow out, built once per payload.
struct Views {
    topic_partition: TopicPartitionInner,
    /// Boxed: the `Headers` view it may later build points back into it.
    headers: Option<Box<RecordHeadersInner>>,
}

fn views(payload: &Payload<RecordDeserializationError>) -> &Views {
    payload.views(|e| Views {
        topic_partition: TopicPartitionInner::new(e.topic_partition().clone()),
        headers: e.headers().map(|headers| RecordHeadersInner::boxed(headers.clone())),
    })
}

unsafe fn payload<'a>(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> &'a Payload<RecordDeserializationError> {
    unsafe { Payload::<RecordDeserializationError>::from_ptr(self_) }
}

/// The payload of a `RecordDeserializationException` error, borrowed from
/// the error handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_record_deserialization(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_RecordDeserializationError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::RecordDeserialization(e) => inner.payload_view(&**e) as *const kafka_common_RecordDeserializationError_t,
        _ => ptr::null(),
    }
}

/// `new RecordDeserializationException(DeserializationExceptionOrigin origin,
/// TopicPartition partition, long offset, long timestamp, TimestampType
/// timestampType, ByteBuffer keyBuffer, ByteBuffer valueBuffer, Headers
/// headers, String message, Throwable cause)` without the cause, which
/// [`kafka_common_RecordDeserializationError_with_source`] adds. The buffers'
/// `data == NULL` and a null `headers` are Java's nulls; everything is
/// copied. Owned, freed with
/// [`kafka_common_RecordDeserializationError_destroy`].
///
/// # Safety
///
/// `origin` and `timestamp_type` must be singletons, `partition` a valid
/// topic-partition handle, the buffers null or valid, `headers` null or a
/// valid record-headers handle and `message` a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_new(
    origin: *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    timestamp: i64,
    timestamp_type: *const kafka_common_record_TimestampType_t,
    key_buffer: kafka_Bytes_t,
    value_buffer: kafka_Bytes_t,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    message: *const c_char,
) -> *mut kafka_common_RecordDeserializationError_t {
    let headers = if headers.is_null() {
        None
    } else {
        Some(unsafe { record_headers_ref(headers) }.clone())
    };
    Payload::boxed(RecordDeserializationError::new(
        unsafe { origin_value_of(origin) },
        unsafe { topic_partition_ref(partition) }.clone(),
        offset,
        timestamp,
        unsafe { timestamp_type::value_of(timestamp_type) },
        unsafe { key_buffer.as_slice() }.map(<[u8]>::to_vec),
        unsafe { value_buffer.as_slice() }.map(<[u8]>::to_vec),
        headers,
        unsafe { c_str_to_string(message) },
    ))
}

/// `with_source(Error source)`: sets `source` as the cause, in place on an
/// owned handle; `source` is consumed. Strings and the cause previously
/// borrowed from the handle are invalidated, the structured views
/// (`topic_partition`, `headers`) stay valid.
///
/// # Safety
///
/// `self_` must be an owned handle not yet destroyed, never a view borrowed
/// from a `kafka_common_Error_t`, and `source` null or an owned error handle
/// not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_with_source(
    self_: *mut kafka_common_RecordDeserializationError_t,
    source: *mut kafka_common_Error_t,
) {
    let source = unsafe { take_source(source) };
    unsafe { Payload::<RecordDeserializationError>::from_ptr_mut(self_) }
        .replace_value(|value| value.with_source(source));
}

/// `origin()`: the borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_origin(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t {
    origin_singleton(unsafe { payload(self_) }.value().origin())
}

/// `topicPartition()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_topic_partition(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *const kafka_common_TopicPartition_t {
    views(unsafe { payload(self_) }).topic_partition.as_ptr()
}

/// `offset()`.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_offset(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> i64 {
    unsafe { payload(self_) }.value().offset()
}

/// `timestamp()`.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_timestamp(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> i64 {
    unsafe { payload(self_) }.value().timestamp()
}

/// `timestampType()`: the borrowed singleton.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_timestamp_type(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *const kafka_common_record_TimestampType_t {
    timestamp_type::singleton(unsafe { payload(self_) }.value().timestamp_type())
}

/// `keyBuffer()`: a view over the raw key, valid as long as the handle;
/// `data` is null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_key_buffer(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> kafka_Bytes_t {
    kafka_Bytes_t::from_option(unsafe { payload(self_) }.value().key_buffer())
}

/// `valueBuffer()`: a view over the raw value, valid as long as the handle;
/// `data` is null for Java's null.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_value_buffer(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> kafka_Bytes_t {
    kafka_Bytes_t::from_option(unsafe { payload(self_) }.value().value_buffer())
}

/// `headers()`: borrowed from the handle, valid as long as it is, or null
/// for Java's null. Read through
/// `kafka_common_header_internals_RecordHeaders__as_Headers`.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_headers(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *const kafka_common_header_internals_RecordHeaders_t {
    views(unsafe { payload(self_) })
        .headers
        .as_ref()
        .map_or(ptr::null(), |headers| headers.as_ptr())
}

/// `getMessage()`: borrowed from the handle, valid as long as it is.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_message(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *const c_char {
    unsafe { payload(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_source(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *const kafka_common_Error_t {
    unsafe { payload(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid record-deserialization handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_to_string(
    self_: *const kafka_common_RecordDeserializationError_t,
) -> *mut c_char {
    unsafe { payload(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_RecordDeserializationError_destroy(
    self_: *mut kafka_common_RecordDeserializationError_t,
) {
    unsafe { Payload::<RecordDeserializationError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};

    use super::*;
    use crate::common::TopicPartition;
    use crate::common::header::{Header, RecordHeader, RecordHeaders};
    use crate::common::record::TimestampType;
    use crate::ffi::common::header::internals::record_header::{
        kafka_common_header_internals_RecordHeader_t, record_header_ref,
    };
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders__as_Headers,
        kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::header::kafka_common_header_Headers_to_array;
    use crate::ffi::common::record::timestamp_type::{
        kafka_common_record_TimestampType_create_time, kafka_common_record_TimestampType_log_append_time,
    };
    use crate::ffi::common::topic_partition::{
        box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_partition,
        kafka_common_TopicPartition_topic,
    };
    use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_message};
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size, kafka_string_destroy};

    /// A `RecordDeserializationError` with every optional field populated.
    fn full() -> RecordDeserializationError {
        RecordDeserializationError::new(
            DeserializationErrorOrigin::Key,
            TopicPartition::new("t2", 5),
            42,
            99,
            TimestampType::LogAppendTime,
            Some(vec![1, 2, 3]),
            Some(vec![4, 5]),
            Some(RecordHeaders::with_header_iter([RecordHeader::new(
                "h1".to_string(),
                Some(vec![9, 9]),
            )])),
            "m",
        )
    }

    /// A `RecordDeserializationError` with the minimum context.
    fn sparse() -> RecordDeserializationError {
        RecordDeserializationError::new(
            DeserializationErrorOrigin::Value,
            TopicPartition::new("t", 0),
            0,
            0,
            TimestampType::CreateTime,
            None,
            None,
            None,
            "m",
        )
    }

    #[test]
    fn origin_singletons_match_their_enumerator() {
        for (index, &origin) in ORIGINS.iter().enumerate() {
            let handle = origin_singleton(origin);
            unsafe {
                assert_eq!(origin_value_of(handle), origin);
                assert_eq!(
                    kafka_common_RecordDeserializationError_DeserializationErrorOrigin__enum(handle) as usize,
                    index
                );
            }
        }
        assert_eq!(
            kafka_common_RecordDeserializationError_DeserializationErrorOrigin_key(),
            origin_singleton(DeserializationErrorOrigin::Key)
        );
        assert_eq!(
            kafka_common_RecordDeserializationError_DeserializationErrorOrigin_value(),
            origin_singleton(DeserializationErrorOrigin::Value)
        );
    }

    #[test]
    fn view_exposes_every_field() {
        let error = box_error(Error::RecordDeserialization(Box::new(full())));
        unsafe {
            let view = kafka_common_Error_record_deserialization(error);
            assert!(!view.is_null());
            assert_eq!(view, kafka_common_Error_record_deserialization(error), "the view is cached");

            assert_eq!(
                kafka_common_RecordDeserializationError_origin(view),
                kafka_common_RecordDeserializationError_DeserializationErrorOrigin_key()
            );
            let partition = kafka_common_RecordDeserializationError_topic_partition(view);
            assert_eq!(
                partition,
                kafka_common_RecordDeserializationError_topic_partition(view),
                "borrowed and cached"
            );
            assert_eq!(
                CStr::from_ptr(kafka_common_TopicPartition_topic(partition)).to_str().unwrap(),
                "t2"
            );
            assert_eq!(kafka_common_TopicPartition_partition(partition), 5);
            assert_eq!(kafka_common_RecordDeserializationError_offset(view), 42);
            assert_eq!(kafka_common_RecordDeserializationError_timestamp(view), 99);
            assert_eq!(
                kafka_common_RecordDeserializationError_timestamp_type(view),
                kafka_common_record_TimestampType_log_append_time()
            );
            assert_eq!(
                kafka_common_RecordDeserializationError_key_buffer(view).as_slice(),
                Some(&[1_u8, 2, 3][..])
            );
            assert_eq!(
                kafka_common_RecordDeserializationError_value_buffer(view).as_slice(),
                Some(&[4_u8, 5][..])
            );

            let headers = kafka_common_RecordDeserializationError_headers(view);
            assert!(!headers.is_null());
            let array = kafka_common_header_Headers_to_array(kafka_common_header_internals_RecordHeaders__as_Headers(
                headers as *mut _,
            ));
            assert_eq!(kafka_List_size(array), 1);
            let header =
                record_header_ref(kafka_List_get(array, 0) as *const kafka_common_header_internals_RecordHeader_t);
            assert_eq!(header.key(), "h1");
            assert_eq!(header.value(), Some(&[9_u8, 9][..]));
            kafka_List_destroy(array);

            assert_eq!(
                CStr::from_ptr(kafka_common_RecordDeserializationError_message(view))
                    .to_str()
                    .unwrap(),
                "m"
            );
            assert!(kafka_common_RecordDeserializationError_source(view).is_null());
            let s = kafka_common_RecordDeserializationError_to_string(view);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), full().to_string());
            kafka_string_destroy(s);
            kafka_common_Error_destroy(error);

            // Absent key, value and headers are nulls.
            let error = box_error(Error::RecordDeserialization(Box::new(sparse())));
            let view = kafka_common_Error_record_deserialization(error);
            assert_eq!(
                kafka_common_RecordDeserializationError_origin(view),
                kafka_common_RecordDeserializationError_DeserializationErrorOrigin_value()
            );
            assert!(kafka_common_RecordDeserializationError_key_buffer(view).data.is_null());
            assert!(kafka_common_RecordDeserializationError_value_buffer(view).data.is_null());
            assert!(kafka_common_RecordDeserializationError_headers(view).is_null());
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_record_deserialization(other).is_null());
            kafka_common_Error_destroy(other);
        }
    }

    #[test]
    fn constructor_copies_its_input_and_with_source_mutates_in_place() {
        let message = CString::new("m").unwrap();
        let partition = box_topic_partition(TopicPartition::new("t2", 5));
        let headers = box_record_headers(RecordHeaders::with_header_iter([RecordHeader::new(
            "h1".to_string(),
            Some(vec![9, 9]),
        )]));
        unsafe {
            let built = kafka_common_RecordDeserializationError_new(
                kafka_common_RecordDeserializationError_DeserializationErrorOrigin_key(),
                partition,
                42,
                99,
                kafka_common_record_TimestampType_log_append_time(),
                kafka_Bytes_t::from_slice(&[1, 2, 3]),
                kafka_Bytes_t::from_slice(&[4, 5]),
                headers,
                message.as_ptr(),
            );
            kafka_common_TopicPartition_destroy(partition);
            kafka_common_header_internals_RecordHeaders_destroy(headers);
            let s = kafka_common_RecordDeserializationError_to_string(built);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), full().to_string());
            kafka_string_destroy(s);

            // `with_source` mutates the owned handle in place: the structured
            // views survive, the cause appears.
            let partition_before = kafka_common_RecordDeserializationError_topic_partition(built);
            let cause = box_error(Error::kafka_message("cause"));
            kafka_common_RecordDeserializationError_with_source(built, cause);
            let source = kafka_common_RecordDeserializationError_source(built);
            assert!(!source.is_null());
            assert_eq!(CStr::from_ptr(kafka_common_Error_message(source)).to_str().unwrap(), "cause");
            assert_eq!(kafka_common_RecordDeserializationError_topic_partition(built), partition_before);
            assert_eq!(
                CStr::from_ptr(kafka_common_RecordDeserializationError_message(built))
                    .to_str()
                    .unwrap(),
                "m"
            );
            kafka_common_RecordDeserializationError_destroy(built);

            let sparse_tp = box_topic_partition(TopicPartition::new("t", 0));
            let sparse_built = kafka_common_RecordDeserializationError_new(
                kafka_common_RecordDeserializationError_DeserializationErrorOrigin_value(),
                sparse_tp,
                0,
                0,
                kafka_common_record_TimestampType_create_time(),
                kafka_Bytes_t::NULL,
                kafka_Bytes_t::NULL,
                ptr::null(),
                message.as_ptr(),
            );
            kafka_common_TopicPartition_destroy(sparse_tp);
            assert!(kafka_common_RecordDeserializationError_headers(sparse_built).is_null());
            let s = kafka_common_RecordDeserializationError_to_string(sparse_built);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), sparse().to_string());
            kafka_string_destroy(s);
            kafka_common_RecordDeserializationError_destroy(sparse_built);
            kafka_common_RecordDeserializationError_destroy(ptr::null_mut());
        }
    }
}

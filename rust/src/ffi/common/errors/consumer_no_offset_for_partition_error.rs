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

//! `kafka_common_ConsumerNoOffsetForPartitionError_t`:
//! `org.apache.kafka.clients.consumer.NoOffsetForPartitionException`.

use std::ffi::c_char;
use std::ptr;

use crate::common::Error;
use crate::consumer::ConsumerNoOffsetForPartitionError;
use crate::ffi::common::errors::{Payload, PayloadClass};
use crate::ffi::common::topic_partition::{
    kafka_common_TopicPartition_t, list_topic_partitions, sorted_topic_partition_list, topic_partition_ref,
};
use crate::ffi::common::{error_ref, kafka_common_Error_t};
use crate::ffi::ffi_guard;
use crate::ffi::util::kafka_List_t;

/// Opaque handle to a [`ConsumerNoOffsetForPartitionError`].
#[repr(C)]
pub struct kafka_common_ConsumerNoOffsetForPartitionError_t {
    _private: [u8; 0],
}

impl PayloadClass for ConsumerNoOffsetForPartitionError {
    fn message(&self) -> &str {
        ConsumerNoOffsetForPartitionError::message(self)
    }

    fn source(&self) -> Option<&Error> {
        ConsumerNoOffsetForPartitionError::source(self)
    }
}

/// Returns the payload of a `NoOffsetForPartitionException` error, or `NULL` when the error is
/// another class.
///
/// # Parameters
///
/// - `error` - The error handle.
///
/// # Safety
///
/// `error` must be a valid error handle.
/// The returned view is borrowed, valid as long as `error`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_no_offset_for_partition(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerNoOffsetForPartitionError_t {
    // SAFETY: Per this function's `# Safety`, `error` is a valid error handle: non-null and live,
    // an owned one from `box_error` or one borrowed from a live owner, as `error_ref` requires. The
    // reference is used only within this call. The view returned is a
    // `Payload<ConsumerNoOffsetForPartitionError>` cloned from the variant's payload into the
    // `ErrorInner`'s payload slot, so it lives as long as the error handle: until the error, or the
    // owner it is borrowed from, is destroyed.
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerNoOffsetForPartition(e) => {
            inner.payload_view(e) as *const kafka_common_ConsumerNoOffsetForPartitionError_t
        },
        _ => ptr::null(),
    }
}

/// `new NoOffsetForPartitionException(TopicPartition partition)`; the partition is copied.
///
/// # Parameters
///
/// - `partition` - The partition for which no offset is defined
///
/// # Safety
///
/// `partition` must be a valid topic-partition handle.
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_ConsumerNoOffsetForPartitionError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_new(
    partition: *const kafka_common_TopicPartition_t,
) -> *mut kafka_common_ConsumerNoOffsetForPartitionError_t {
    Payload::boxed(ConsumerNoOffsetForPartitionError::new(
        // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, as
        // `topic_partition_ref` requires; the value is cloned, so the borrow lasts for this call
        // only.
        unsafe { topic_partition_ref(partition) }.clone(),
    ))
}

/// `new NoOffsetForPartitionException(Collection<TopicPartition>
/// partitions)`; the list holds `const kafka_common_TopicPartition_t *`,
/// copied.
///
/// # Parameters
///
/// - `partitions` - The partitions for which no offset is defined
///
/// # Safety
///
/// `partitions` must be `NULL` or a valid list of topic-partition handles.
///
/// The returned handle is owned by the caller, freed with
/// `kafka_common_ConsumerNoOffsetForPartitionError_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_with_partitions(
    partitions: *const kafka_List_t,
) -> *mut kafka_common_ConsumerNoOffsetForPartitionError_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // topic-partition handles, as `list_topic_partitions` requires; they are cloned into owned
    // values, so nothing borrows the list after this call.
    Payload::boxed(ConsumerNoOffsetForPartitionError::with_partitions(unsafe {
        list_topic_partitions(partitions)
    }))
}

/// `partitions()`: a list of `kafka_common_TopicPartition_t *`, ordered by topic then partition.
///
/// # Returns
///
/// All partitions without offsets
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
///
/// The returned list is owned by the caller, freed with `kafka_List_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_partitions(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(
        // SAFETY: Per this function's `# Safety`, `self_` is a valid no-offset-for-partition
        // handle: a view whose error is alive or an owned handle not yet destroyed, as
        // `Payload::from_ptr` requires (no call mutates a payload of this class). The borrow lasts
        // for this call only: the field is copied into the owned container returned.
        unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }
            .value()
            .partitions(),
    )
}

/// Returns the detail message of the error.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
/// The returned string is borrowed, valid until `self_` is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_message(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid no-offset-for-partition handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `message_ptr` points at the payload's own
    // C string, so the string lives as long as the handle; that is the rustdoc's "borrowed from the
    // handle".
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }.message_ptr()
}

/// Returns the cause of the error, or `NULL` if there is none.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
/// The returned handle is borrowed, valid until `self_` is destroyed.
#[ffi_guard(fallback = ::core::ptr::null())]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_source(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *const kafka_common_Error_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid no-offset-for-partition handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). `source_ptr` returns the cause handle the
    // payload's `source` slot owns, so it lives as long as the handle; that is the rustdoc's
    // "borrowed from the handle".
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }.source_ptr()
}

/// Returns the text form of the error.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_to_string(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid no-offset-for-partition handle: a
    // view whose error is alive or an owned handle not yet destroyed, as `Payload::from_ptr`
    // requires (no call mutates a payload of this class). The borrow lasts for this call only:
    // `to_c_string` copies the text into an owned string.
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; `NULL` is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be `NULL` or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_destroy(
    self_: *mut kafka_common_ConsumerNoOffsetForPartitionError_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned handle not yet destroyed,
    // as `Payload::destroy` requires. Owned `kafka_common_ConsumerNoOffsetForPartitionError_t`
    // handles come only from this class's constructors, which box a
    // `Payload<ConsumerNoOffsetForPartitionError>`, and a view is never passed here.
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::destroy(self_) }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::common::TopicPartition;
    use crate::ffi::common::topic_partition::{
        box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_partition,
    };
    use crate::ffi::common::{box_error, kafka_common_Error_destroy};
    use crate::ffi::util::{kafka_List_destroy, kafka_List_get, kafka_List_size};

    #[test]
    fn partitions_cross_as_a_sorted_list() {
        let error = box_error(Error::ConsumerNoOffsetForPartition(
            ConsumerNoOffsetForPartitionError::with_partitions([
                TopicPartition::new("t", 3),
                TopicPartition::new("t", 1),
            ]),
        ));
        // SAFETY: Test code: `box_error`, `box_topic_partition` and this module's constructors and
        // getters make every handle used. Each owned handle (the error, the payloads, the lists,
        // the topic-partition, the string) is destroyed once and the view is used only while its
        // error is alive.
        unsafe {
            let view = kafka_common_Error_consumer_no_offset_for_partition(error);
            assert!(!view.is_null());
            let list = kafka_common_ConsumerNoOffsetForPartitionError_partitions(view);
            assert_eq!(kafka_List_size(list), 2);
            assert_eq!(kafka_common_TopicPartition_partition(kafka_List_get(list, 0) as *const _), 1);
            assert_eq!(kafka_common_TopicPartition_partition(kafka_List_get(list, 1) as *const _), 3);

            // Feeding the list back builds an equal value.
            let rebuilt = kafka_common_ConsumerNoOffsetForPartitionError_with_partitions(list);
            kafka_List_destroy(list);
            let again = kafka_common_ConsumerNoOffsetForPartitionError_partitions(rebuilt);
            assert_eq!(kafka_List_size(again), 2);
            kafka_List_destroy(again);
            assert!(kafka_common_ConsumerNoOffsetForPartitionError_source(rebuilt).is_null());
            kafka_common_ConsumerNoOffsetForPartitionError_destroy(rebuilt);
            kafka_common_Error_destroy(error);

            let other = box_error(Error::kafka_message("other"));
            assert!(kafka_common_Error_consumer_no_offset_for_partition(other).is_null());
            kafka_common_Error_destroy(other);

            let tp = box_topic_partition(TopicPartition::new("t", 7));
            let single = kafka_common_ConsumerNoOffsetForPartitionError_new(tp);
            kafka_common_TopicPartition_destroy(tp);
            assert_eq!(
                CStr::from_ptr(kafka_common_ConsumerNoOffsetForPartitionError_message(single))
                    .to_str()
                    .unwrap(),
                ConsumerNoOffsetForPartitionError::new(TopicPartition::new("t", 7)).message()
            );
            let s = kafka_common_ConsumerNoOffsetForPartitionError_to_string(single);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                ConsumerNoOffsetForPartitionError::new(TopicPartition::new("t", 7)).to_string()
            );
            crate::ffi::util::kafka_string_destroy(s);
            kafka_common_ConsumerNoOffsetForPartitionError_destroy(single);
            kafka_common_ConsumerNoOffsetForPartitionError_destroy(ptr::null_mut());
        }
    }
}

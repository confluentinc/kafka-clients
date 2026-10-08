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

/// The payload of a `NoOffsetForPartitionException` error, borrowed from
/// the error handle, or null when the error is another class.
///
/// # Safety
///
/// `error` must be a valid error handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_Error_consumer_no_offset_for_partition(
    error: *const kafka_common_Error_t,
) -> *const kafka_common_ConsumerNoOffsetForPartitionError_t {
    let inner = unsafe { error_ref(error) };
    match &inner.error {
        Error::ConsumerNoOffsetForPartition(e) => {
            inner.payload_view(e) as *const kafka_common_ConsumerNoOffsetForPartitionError_t
        },
        _ => ptr::null(),
    }
}

/// `new NoOffsetForPartitionException(TopicPartition partition)`; the
/// partition is copied. Owned, freed with
/// [`kafka_common_ConsumerNoOffsetForPartitionError_destroy`].
///
/// # Safety
///
/// `partition` must be a valid topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_new(
    partition: *const kafka_common_TopicPartition_t,
) -> *mut kafka_common_ConsumerNoOffsetForPartitionError_t {
    Payload::boxed(ConsumerNoOffsetForPartitionError::new(
        unsafe { topic_partition_ref(partition) }.clone(),
    ))
}

/// `new NoOffsetForPartitionException(Collection<TopicPartition>
/// partitions)`; the list holds `const kafka_common_TopicPartition_t *`,
/// copied.
///
/// # Safety
///
/// `partitions` must be null or a valid list of topic-partition handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_with_partitions(
    partitions: *const kafka_List_t,
) -> *mut kafka_common_ConsumerNoOffsetForPartitionError_t {
    Payload::boxed(ConsumerNoOffsetForPartitionError::with_partitions(unsafe {
        list_topic_partitions(partitions)
    }))
}

/// `partitions()`: an owned list of `kafka_common_TopicPartition_t *`,
/// ordered by topic then partition, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_partitions(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(
        unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }
            .value()
            .partitions(),
    )
}

/// `getMessage()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_message(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *const c_char {
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }.message_ptr()
}

/// `getCause()`: borrowed from the handle, or null.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_source(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *const kafka_common_Error_t {
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }.source_ptr()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid no-offset-for-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_to_string(
    self_: *const kafka_common_ConsumerNoOffsetForPartitionError_t,
) -> *mut c_char {
    unsafe { Payload::<ConsumerNoOffsetForPartitionError>::from_ptr(self_) }.to_c_string()
}

/// Frees an owned handle; null is a no-op. A view borrowed from a
/// `kafka_common_Error_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_ConsumerNoOffsetForPartitionError_destroy(
    self_: *mut kafka_common_ConsumerNoOffsetForPartitionError_t,
) {
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

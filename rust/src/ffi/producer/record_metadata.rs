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

//! `kafka_producer_RecordMetadata_t`:
//! `org.apache.kafka.clients.producer.RecordMetadata` (CLAUDE.md §4).
//!
//! The value behind the `void *` a `kafka_producer_Producer_send` future
//! resolves to: `kafka_common_KafkaFuture_get` hands out a
//! `kafka_producer_RecordMetadata_t *` borrowed from the future, valid until
//! the future is destroyed. The handles a `kafka_producer_Callback_t`
//! receives are borrowed for the callback's duration, and the ones built by
//! [`kafka_producer_RecordMetadata_new`] are the caller's.

use std::ffi::{CString, c_char};

use crate::common::TopicPartition;
use crate::ffi::common::topic_partition::{kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::util::{into_c_string, owned_c_string};
use crate::producer::RecordMetadata;

/// Opaque handle to a [`RecordMetadata`].
#[repr(C)]
pub struct kafka_producer_RecordMetadata_t {
    _private: [u8; 0],
}

pub(crate) struct RecordMetadataInner {
    metadata: RecordMetadata,
    topic_c: CString,
}

impl RecordMetadataInner {
    fn new(metadata: RecordMetadata) -> Self {
        let topic_c = owned_c_string(metadata.topic());
        Self { metadata, topic_c }
    }
}

/// Boxes metadata into an owned handle.
pub(crate) fn box_record_metadata(metadata: RecordMetadata) -> *mut kafka_producer_RecordMetadata_t {
    Box::into_raw(Box::new(RecordMetadataInner::new(metadata))) as *mut kafka_producer_RecordMetadata_t
}

/// The metadata behind a handle.
///
/// # Safety
///
/// `metadata` must be a live handle.
pub(crate) unsafe fn record_metadata_ref<'a>(metadata: *const kafka_producer_RecordMetadata_t) -> &'a RecordMetadata {
    &unsafe { inner(metadata) }.metadata
}

unsafe fn inner<'a>(metadata: *const kafka_producer_RecordMetadata_t) -> &'a RecordMetadataInner {
    unsafe { &*(metadata as *const RecordMetadataInner) }
}

/// `new RecordMetadata(TopicPartition, long baseOffset, int batchIndex, long
/// timestamp, int serializedKeySize, int serializedValueSize)`: owned, freed
/// with [`kafka_producer_RecordMetadata_destroy`].
///
/// # Safety
///
/// `topic_partition` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_new(
    topic_partition: *const kafka_common_TopicPartition_t,
    base_offset: i64,
    batch_index: i32,
    timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
) -> *mut kafka_producer_RecordMetadata_t {
    let topic_partition: TopicPartition = unsafe { topic_partition_ref(topic_partition) }.clone();
    box_record_metadata(RecordMetadata::new(
        topic_partition,
        base_offset,
        batch_index,
        timestamp,
        serialized_key_size,
        serialized_value_size,
    ))
}

/// `RecordMetadata.hasOffset()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_has_offset(self_: *const kafka_producer_RecordMetadata_t) -> i8 {
    i8::from(unsafe { record_metadata_ref(self_) }.has_offset())
}

/// `RecordMetadata.offset()`: `-1` when unknown.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_offset(self_: *const kafka_producer_RecordMetadata_t) -> i64 {
    unsafe { record_metadata_ref(self_) }.offset()
}

/// `RecordMetadata.hasTimestamp()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_has_timestamp(
    self_: *const kafka_producer_RecordMetadata_t,
) -> i8 {
    i8::from(unsafe { record_metadata_ref(self_) }.has_timestamp())
}

/// `RecordMetadata.timestamp()`: `-1` when unknown.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_timestamp(self_: *const kafka_producer_RecordMetadata_t) -> i64 {
    unsafe { record_metadata_ref(self_) }.timestamp()
}

/// `RecordMetadata.serializedKeySize()`: `-1` for a `null` key.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_serialized_key_size(
    self_: *const kafka_producer_RecordMetadata_t,
) -> i32 {
    unsafe { record_metadata_ref(self_) }.serialized_key_size()
}

/// `RecordMetadata.serializedValueSize()`: `-1` for a `null` value.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_serialized_value_size(
    self_: *const kafka_producer_RecordMetadata_t,
) -> i32 {
    unsafe { record_metadata_ref(self_) }.serialized_value_size()
}

/// `RecordMetadata.topic()`: borrowed, valid until the handle is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_topic(
    self_: *const kafka_producer_RecordMetadata_t,
) -> *const c_char {
    unsafe { inner(self_) }.topic_c.as_ptr()
}

/// `RecordMetadata.partition()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_partition(self_: *const kafka_producer_RecordMetadata_t) -> i32 {
    unsafe { record_metadata_ref(self_) }.partition()
}

/// `RecordMetadata.toString()`: owned, freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_to_string(
    self_: *const kafka_producer_RecordMetadata_t,
) -> *mut c_char {
    into_c_string(&unsafe { record_metadata_ref(self_) }.to_string())
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_destroy(self_: *mut kafka_producer_RecordMetadata_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut RecordMetadataInner) });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ffi::common::topic_partition::{box_topic_partition, kafka_common_TopicPartition_destroy};
    use crate::ffi::util::{c_str_to_string, kafka_string_destroy};

    #[test]
    fn getters_mirror_the_rust_value() {
        let tp = box_topic_partition(TopicPartition::new("topic", 2));
        unsafe {
            let metadata = kafka_producer_RecordMetadata_new(tp, 100, 3, 1_700_000_000_000, 4, 8);
            assert_eq!(kafka_producer_RecordMetadata_has_offset(metadata), 1);
            assert_eq!(kafka_producer_RecordMetadata_offset(metadata), 103);
            assert_eq!(kafka_producer_RecordMetadata_has_timestamp(metadata), 1);
            assert_eq!(kafka_producer_RecordMetadata_timestamp(metadata), 1_700_000_000_000);
            assert_eq!(kafka_producer_RecordMetadata_serialized_key_size(metadata), 4);
            assert_eq!(kafka_producer_RecordMetadata_serialized_value_size(metadata), 8);
            assert_eq!(c_str_to_string(kafka_producer_RecordMetadata_topic(metadata)), "topic");
            assert_eq!(kafka_producer_RecordMetadata_partition(metadata), 2);
            let text = kafka_producer_RecordMetadata_to_string(metadata);
            assert_eq!(c_str_to_string(text), "topic-2@103");
            kafka_string_destroy(text);
            kafka_producer_RecordMetadata_destroy(metadata);

            // Unknown offset and timestamp: the batch index is ignored, as in Java.
            let unknown = kafka_producer_RecordMetadata_new(tp, -1, 3, -1, -1, -1);
            assert_eq!(kafka_producer_RecordMetadata_has_offset(unknown), 0);
            assert_eq!(kafka_producer_RecordMetadata_offset(unknown), -1);
            assert_eq!(kafka_producer_RecordMetadata_has_timestamp(unknown), 0);
            kafka_producer_RecordMetadata_destroy(unknown);
            kafka_producer_RecordMetadata_destroy(std::ptr::null_mut());
            kafka_common_TopicPartition_destroy(tp);
        }
    }
}

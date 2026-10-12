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
use crate::ffi::ffi_guard;
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
/// `metadata` must be a live handle, kept alive for `'a`.
pub(crate) unsafe fn record_metadata_ref<'a>(metadata: *const kafka_producer_RecordMetadata_t) -> &'a RecordMetadata {
    // SAFETY: Per this function's `# Safety`, `metadata` is a live handle kept alive for `'a`, as
    // `inner` requires; the reference returned borrows the handle's `metadata` field, so it lives
    // as long as the handle does.
    &unsafe { inner(metadata) }.metadata
}

/// The `RecordMetadataInner` behind a handle.
///
/// # Safety
///
/// `metadata` must be a live handle, kept alive for `'a`.
unsafe fn inner<'a>(metadata: *const kafka_producer_RecordMetadata_t) -> &'a RecordMetadataInner {
    // SAFETY: Per this function's `# Safety`, `metadata` is a live handle kept alive for `'a`.
    // Every `kafka_producer_RecordMetadata_t` is the `Box::into_raw` of a `RecordMetadataInner` in
    // `box_record_metadata`, whoever owns it (the caller of `kafka_producer_RecordMetadata_new`, a
    // send future, or a queued delivery-callback job), so the cast targets a live, correctly typed
    // value. Nothing writes a `RecordMetadataInner` after it is built, so the shared reference is
    // sound.
    unsafe { &*(metadata as *const RecordMetadataInner) }
}

/// `new RecordMetadata(TopicPartition, long baseOffset, int batchIndex, long
/// timestamp, int serializedKeySize, int serializedValueSize)`: owned, freed
/// with [`kafka_producer_RecordMetadata_destroy`].
///
/// # Safety
///
/// `topic_partition` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_new(
    topic_partition: *const kafka_common_TopicPartition_t,
    base_offset: i64,
    batch_index: i32,
    timestamp: i64,
    serialized_key_size: i32,
    serialized_value_size: i32,
) -> *mut kafka_producer_RecordMetadata_t {
    // SAFETY: Per this function's `# Safety`, `topic_partition` is a live handle, as
    // `topic_partition_ref` requires; the topic-partition is cloned before return, so the borrow
    // lasts only this call.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_has_offset(self_: *const kafka_producer_RecordMetadata_t) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { record_metadata_ref(self_) }.has_offset())
}

/// `RecordMetadata.offset()`: `-1` when unknown.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_offset(self_: *const kafka_producer_RecordMetadata_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    unsafe { record_metadata_ref(self_) }.offset()
}

/// `RecordMetadata.hasTimestamp()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_has_timestamp(
    self_: *const kafka_producer_RecordMetadata_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    i8::from(unsafe { record_metadata_ref(self_) }.has_timestamp())
}

/// `RecordMetadata.timestamp()`: `-1` when unknown.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_timestamp(self_: *const kafka_producer_RecordMetadata_t) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    unsafe { record_metadata_ref(self_) }.timestamp()
}

/// `RecordMetadata.serializedKeySize()`: `-1` for a `null` key.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_serialized_key_size(
    self_: *const kafka_producer_RecordMetadata_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    unsafe { record_metadata_ref(self_) }.serialized_key_size()
}

/// `RecordMetadata.serializedValueSize()`: `-1` for a `null` value.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_serialized_value_size(
    self_: *const kafka_producer_RecordMetadata_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    unsafe { record_metadata_ref(self_) }.serialized_value_size()
}

/// `RecordMetadata.topic()`: borrowed, valid until the handle is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_topic(
    self_: *const kafka_producer_RecordMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `inner` requires. The
    // pointer returned points into the handle's `topic_c`, which is neither moved nor written while
    // the handle lives, so it stays valid until the handle is freed, as the rustdoc states.
    unsafe { inner(self_) }.topic_c.as_ptr()
}

/// `RecordMetadata.partition()`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_partition(self_: *const kafka_producer_RecordMetadata_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call.
    unsafe { record_metadata_ref(self_) }.partition()
}

/// `RecordMetadata.toString()`: owned, freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_to_string(
    self_: *const kafka_producer_RecordMetadata_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `record_metadata_ref`
    // requires; the reference is used only within this call, and the text is copied into an owned
    // string.
    into_c_string(&unsafe { record_metadata_ref(self_) }.to_string())
}

/// Frees an owned handle, such as one from
/// [`kafka_producer_RecordMetadata_new`]; null is a no-op. A handle borrowed
/// from a future or handed to a `kafka_producer_Callback_t` is never passed
/// here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_RecordMetadata_destroy(self_: *mut kafka_producer_RecordMetadata_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an
        // owned handle not yet destroyed. Every handle is the `Box::into_raw` of
        // `box_record_metadata`; an owned one is held by the C caller of
        // `kafka_producer_RecordMetadata_new`, or by the send future or queued delivery-callback
        // job that frees it here, so `Box::from_raw` reclaims it exactly once. A handle borrowed
        // from a future or a delivery callback is never passed here, as the rustdoc states.
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
        // SAFETY: Test code: `tp` is an owned handle from `box_topic_partition`, destroyed once at
        // the end; each metadata handle comes from `kafka_producer_RecordMetadata_new` and is
        // destroyed once after its last use, the borrowed topic is read while its handle lives, the
        // string from `to_string` is destroyed once, and destroying null is a no-op.
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

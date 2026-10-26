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

//! `kafka_common_TopicIdPartition_t`:
//! `org.apache.kafka.common.TopicIdPartition` (CLAUDE.md §4).

use std::ffi::c_char;

use crate::common::TopicIdPartition;
use crate::ffi::common::topic_partition::{TopicPartitionInner, kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_t, uuid_of};
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string};

/// Opaque handle to a [`TopicIdPartition`].
#[repr(C)]
pub struct kafka_common_TopicIdPartition_t {
    _private: [u8; 0],
}

/// What a `kafka_common_TopicIdPartition_t` points at: the value plus a
/// cached topic-partition handle, which also carries the NUL-terminated
/// topic the string getter borrows out.
struct TopicIdPartitionInner {
    tip: TopicIdPartition,
    topic_partition: TopicPartitionInner,
}

impl TopicIdPartitionInner {
    fn new(tip: TopicIdPartition) -> Self {
        let topic_partition = TopicPartitionInner::new(tip.topic_partition().clone());
        Self { tip, topic_partition }
    }
}

/// What a handle points at.
///
/// # Safety
///
/// `tip` must be a valid topic-id-partition handle, alive for `'a`.
unsafe fn inner_ref<'a>(tip: *const kafka_common_TopicIdPartition_t) -> &'a TopicIdPartitionInner {
    // SAFETY: Per this function's `# Safety`, `tip` is a valid topic-id-partition handle alive for
    // `'a`, i.e. the `Box::into_raw` of a `TopicIdPartitionInner` in `boxed`, the only function
    // that makes one, not yet destroyed. No call mutates a `TopicIdPartitionInner`, so the shared
    // reference is sound for `'a`.
    unsafe { &*(tip as *const TopicIdPartitionInner) }
}

fn boxed(tip: TopicIdPartition) -> *mut kafka_common_TopicIdPartition_t {
    Box::into_raw(Box::new(TopicIdPartitionInner::new(tip))) as *mut kafka_common_TopicIdPartition_t
}

/// Create an instance with the provided parameters.
///
/// # Parameters
///
/// - `topic_id` - the topic id
/// - `topic_partition` - the topic partition
///
/// # Safety
///
/// `topic_id` must be a valid uuid handle and `topic_partition` a valid
/// topic-partition handle; both are copied. The returned handle is owned, freed with
/// `kafka_common_TopicIdPartition_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_new(
    topic_id: *const kafka_common_Uuid_t,
    topic_partition: *const kafka_common_TopicPartition_t,
) -> *mut kafka_common_TopicIdPartition_t {
    boxed(TopicIdPartition::new(
        // SAFETY: Per this function's `# Safety`, `topic_id` is a valid uuid handle, as `uuid_of`
        // requires; the `Uuid` is copied out.
        unsafe { uuid_of(topic_id) },
        // SAFETY: Per this function's `# Safety`, `topic_partition` is a valid topic-partition
        // handle, as `topic_partition_ref` requires; the value is cloned, so the borrow lasts for
        // this call only.
        unsafe { topic_partition_ref(topic_partition) }.clone(),
    ))
}

/// Create an instance with the provided parameters.
///
/// # Parameters
///
/// - `topic_id` - the topic id
/// - `partition` - the partition id
/// - `topic` - the topic name, empty or `NULL` if unknown
///
/// # Safety
///
/// `topic_id` must be a valid uuid handle and `topic` `NULL` or a valid NUL-terminated
/// string; both are copied. The returned handle is owned, freed with
/// `kafka_common_TopicIdPartition_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_with_partition_topic(
    topic_id: *const kafka_common_Uuid_t,
    partition: i32,
    topic: *const c_char,
) -> *mut kafka_common_TopicIdPartition_t {
    boxed(TopicIdPartition::with_partition_topic(
        // SAFETY: Per this function's `# Safety`, `topic_id` is a valid uuid handle, as `uuid_of`
        // requires; the `Uuid` is copied out.
        unsafe { uuid_of(topic_id) },
        partition,
        // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
        // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it
        // after this call.
        unsafe { c_str_to_string(topic) },
    ))
}

/// # Returns
///
/// Universally unique id representing this topic partition.
///
/// # Safety
///
/// `self_` must be a valid topic-id-partition handle. The returned handle is owned, freed with
/// `kafka_common_Uuid_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_topic_id(
    self_: *const kafka_common_TopicIdPartition_t,
) -> *mut kafka_common_Uuid_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-id-partition handle, as
    // `inner_ref` requires; the uuid is copied into a new owned handle.
    box_uuid(unsafe { inner_ref(self_) }.tip.topic_id())
}

/// # Returns
///
/// the topic name, or an empty string if it is unknown.
///
/// # Safety
///
/// `self_` must be a valid topic-id-partition handle. The string is borrowed from the handle,
/// valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_topic(
    self_: *const kafka_common_TopicIdPartition_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-id-partition handle, as
    // `inner_ref` requires. The string returned is the embedded `TopicPartitionInner`'s own topic,
    // so it lives as long as the handle; that is the rustdoc's "borrowed from the handle, valid
    // until it is destroyed".
    unsafe { inner_ref(self_) }.topic_partition.topic_ptr()
}

/// # Returns
///
/// the partition id.
///
/// # Safety
///
/// `self_` must be a valid topic-id-partition handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_partition(self_: *const kafka_common_TopicIdPartition_t) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-id-partition handle, as
    // `inner_ref` requires; the reference is used only within this call.
    unsafe { inner_ref(self_) }.tip.partition()
}

/// # Returns
///
/// Topic partition representing this instance.
///
/// # Safety
///
/// `self_` must be a valid topic-id-partition handle. The returned handle is borrowed from it,
/// valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_topic_partition(
    self_: *const kafka_common_TopicIdPartition_t,
) -> *const kafka_common_TopicPartition_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-id-partition handle, as
    // `inner_ref` requires. The handle returned points at the `TopicPartitionInner` the inner
    // embeds, so it lives as long as the handle; that is the rustdoc's "borrowed from the handle,
    // valid until it is destroyed".
    unsafe { inner_ref(self_) }.topic_partition.as_ptr()
}

/// Returns `topicId:topic-partition`.
///
/// # Safety
///
/// `self_` must be a valid topic-id-partition handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_to_string(
    self_: *const kafka_common_TopicIdPartition_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-id-partition handle, as
    // `inner_ref` requires; the reference is used only within this call, and the text is copied
    // into an owned string.
    into_c_string(&unsafe { inner_ref(self_) }.tip.to_string())
}

/// Frees an owned topic-id-partition handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicIdPartition_destroy(self_: *mut kafka_common_TopicIdPartition_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of `boxed`, so `Box::from_raw`
        // reclaims it exactly once, together with the topic-partition view it embeds.
        drop(unsafe { Box::from_raw(self_ as *mut TopicIdPartitionInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::ptr;

    use super::*;
    use crate::common::Uuid;
    use crate::ffi::common::topic_partition::{
        kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_partition,
    };
    use crate::ffi::common::uuid::kafka_common_Uuid_destroy;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn both_constructors_expose_the_same_parts() {
        let id = Uuid::new(5, 6);
        let topic = CString::new("t").unwrap();
        // SAFETY: Test code: `uuid` comes from `box_uuid` and `tp` from `box_topic_partition`, and
        // each is destroyed once, after both constructors copy it; `topic` outlives the constructor
        // reading it; each topic-id-partition is destroyed once, after the views it lends (the
        // topic, the topic-partition) are read; the uuid from `topic_id` and the string from
        // `to_string` are each destroyed once, after they are read.
        unsafe {
            let uuid = box_uuid(id);
            let tp =
                crate::ffi::common::topic_partition::box_topic_partition(crate::common::TopicPartition::new("t", 2));
            let a = kafka_common_TopicIdPartition_new(uuid, tp);
            let b = kafka_common_TopicIdPartition_with_partition_topic(uuid, 2, topic.as_ptr());
            kafka_common_Uuid_destroy(uuid);
            kafka_common_TopicPartition_destroy(tp);

            for tip in [a, b] {
                let topic_id = kafka_common_TopicIdPartition_topic_id(tip);
                assert_eq!(uuid_of(topic_id), id);
                kafka_common_Uuid_destroy(topic_id);
                assert_eq!(CStr::from_ptr(kafka_common_TopicIdPartition_topic(tip)).to_str().unwrap(), "t");
                assert_eq!(kafka_common_TopicIdPartition_partition(tip), 2);
                assert_eq!(
                    kafka_common_TopicPartition_partition(kafka_common_TopicIdPartition_topic_partition(tip)),
                    2
                );
                let s = kafka_common_TopicIdPartition_to_string(tip);
                assert_eq!(CStr::from_ptr(s).to_str().unwrap(), format!("{id}:t-2"));
                kafka_string_destroy(s);
                kafka_common_TopicIdPartition_destroy(tip);
            }
            kafka_common_TopicIdPartition_destroy(ptr::null_mut());
        }
    }
}

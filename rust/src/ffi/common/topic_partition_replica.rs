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

//! `kafka_common_TopicPartitionReplica_t`:
//! `org.apache.kafka.common.TopicPartitionReplica` (CLAUDE.md §4).

use std::ffi::{CString, c_char};

use crate::common::TopicPartitionReplica;
use crate::ffi::ffi_guard;
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`TopicPartitionReplica`].
#[repr(C)]
pub struct kafka_common_TopicPartitionReplica_t {
    _private: [u8; 0],
}

/// What a `kafka_common_TopicPartitionReplica_t` points at: the value plus
/// the NUL-terminated topic the string getter borrows out.
struct TopicPartitionReplicaInner {
    replica: TopicPartitionReplica,
    topic_c: CString,
}

/// What a handle points at.
///
/// # Safety
///
/// `replica` must be a valid topic-partition-replica handle, alive for `'a`.
unsafe fn inner_ref<'a>(replica: *const kafka_common_TopicPartitionReplica_t) -> &'a TopicPartitionReplicaInner {
    // SAFETY: Per this function's `# Safety`, `replica` is a valid topic-partition-replica handle
    // alive for `'a`, i.e. the `Box::into_raw` of a `TopicPartitionReplicaInner` in
    // `kafka_common_TopicPartitionReplica_new`, the only function that makes one, not yet
    // destroyed. No call mutates a `TopicPartitionReplicaInner`, so the shared reference is sound
    // for `'a`.
    unsafe { &*(replica as *const TopicPartitionReplicaInner) }
}

/// The replica behind a handle.
///
/// # Safety
///
/// `replica` must be a valid topic-partition-replica handle, alive for `'a`.
pub(crate) unsafe fn topic_partition_replica_ref<'a>(
    replica: *const kafka_common_TopicPartitionReplica_t,
) -> &'a TopicPartitionReplica {
    // SAFETY: Per this function's `# Safety`, `replica` is a valid topic-partition-replica handle
    // alive for `'a`, as `inner_ref` requires.
    &unsafe { inner_ref(replica) }.replica
}

/// Creates a topic partition replica.
///
/// # Parameters
///
/// - `topic` - the topic name
/// - `partition` - the partition number
/// - `broker_id` - the id of the broker holding the replica
///
/// # Safety
///
/// `topic` must be a valid NUL-terminated string. The returned handle is owned, freed with
/// `kafka_common_TopicPartitionReplica_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_new(
    topic: *const c_char,
    partition: i32,
    broker_id: i32,
) -> *mut kafka_common_TopicPartitionReplica_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`, so nothing borrows it after
    // this call.
    let replica = TopicPartitionReplica::new(unsafe { c_str_to_string(topic) }, partition, broker_id);
    let topic_c = owned_c_string(replica.topic());
    Box::into_raw(Box::new(TopicPartitionReplicaInner { replica, topic_c }))
        as *mut kafka_common_TopicPartitionReplica_t
}

/// Returns the topic name.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle. The string is borrowed from the
/// handle, valid until it is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_topic(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-replica handle, as
    // `inner_ref` requires. The string returned is the inner's own `topic_c`, so it lives as long
    // as the handle; that is the rustdoc's "borrowed from the handle, valid until it is destroyed".
    unsafe { inner_ref(self_) }.topic_c.as_ptr()
}

/// Returns the partition number.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_partition(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-replica handle, as
    // `inner_ref` requires; the reference is used only within this call.
    unsafe { inner_ref(self_) }.replica.partition()
}

/// Returns the id of the broker holding the replica.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_broker_id(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-replica handle, as
    // `inner_ref` requires; the reference is used only within this call.
    unsafe { inner_ref(self_) }.replica.broker_id()
}

/// Returns `topic-partition-brokerId`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
/// The returned string is owned, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_to_string(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid topic-partition-replica handle, as
    // `inner_ref` requires; the reference is used only within this call, and the text is copied
    // into an owned string.
    into_c_string(&unsafe { inner_ref(self_) }.replica.to_string())
}

/// Frees an owned topic-partition-replica handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_destroy(self_: *mut kafka_common_TopicPartitionReplica_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, i.e. the `Box::into_raw` of
        // `kafka_common_TopicPartitionReplica_new`, so `Box::from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut TopicPartitionReplicaInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn handle_exposes_all_three_parts() {
        let topic = CString::new("t").unwrap();
        // SAFETY: Test code: `topic` outlives the constructor, which copies it; `replica` is
        // destroyed once, after the topic it lends and the string from `to_string`, itself
        // destroyed once, are read.
        unsafe {
            let replica = kafka_common_TopicPartitionReplica_new(topic.as_ptr(), 1, 42);
            assert_eq!(
                CStr::from_ptr(kafka_common_TopicPartitionReplica_topic(replica))
                    .to_str()
                    .unwrap(),
                "t"
            );
            assert_eq!(kafka_common_TopicPartitionReplica_partition(replica), 1);
            assert_eq!(kafka_common_TopicPartitionReplica_broker_id(replica), 42);
            let s = kafka_common_TopicPartitionReplica_to_string(replica);
            assert_eq!(
                CStr::from_ptr(s).to_str().unwrap(),
                TopicPartitionReplica::new("t", 1, 42).to_string()
            );
            kafka_string_destroy(s);
            kafka_common_TopicPartitionReplica_destroy(replica);
            kafka_common_TopicPartitionReplica_destroy(ptr::null_mut());
        }
    }
}

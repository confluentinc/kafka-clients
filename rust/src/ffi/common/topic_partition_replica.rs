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
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`TopicPartitionReplica`].
#[repr(C)]
pub struct kafka_common_TopicPartitionReplica_t {
    _private: [u8; 0],
}

/// What a [`kafka_common_TopicPartitionReplica_t`] points at: the value plus
/// the NUL-terminated topic the string getter borrows out.
struct TopicPartitionReplicaInner {
    replica: TopicPartitionReplica,
    topic_c: CString,
}

unsafe fn inner_ref<'a>(replica: *const kafka_common_TopicPartitionReplica_t) -> &'a TopicPartitionReplicaInner {
    unsafe { &*(replica as *const TopicPartitionReplicaInner) }
}

/// The replica behind a handle.
///
/// # Safety
///
/// `replica` must be a live handle.
pub(crate) unsafe fn topic_partition_replica_ref<'a>(
    replica: *const kafka_common_TopicPartitionReplica_t,
) -> &'a TopicPartitionReplica {
    &unsafe { inner_ref(replica) }.replica
}

/// `new TopicPartitionReplica(String topic, int partition, int brokerId)`,
/// as an owned handle freed with
/// [`kafka_common_TopicPartitionReplica_destroy`].
///
/// # Safety
///
/// `topic` must be a valid NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_new(
    topic: *const c_char,
    partition: i32,
    broker_id: i32,
) -> *mut kafka_common_TopicPartitionReplica_t {
    let replica = TopicPartitionReplica::new(unsafe { c_str_to_string(topic) }, partition, broker_id);
    let topic_c = owned_c_string(replica.topic());
    Box::into_raw(Box::new(TopicPartitionReplicaInner { replica, topic_c }))
        as *mut kafka_common_TopicPartitionReplica_t
}

/// `topic()`: borrowed from the handle, valid until it is destroyed.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_topic(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.topic_c.as_ptr()
}

/// `partition()`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_partition(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> i32 {
    unsafe { inner_ref(self_) }.replica.partition()
}

/// `brokerId()`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_broker_id(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> i32 {
    unsafe { inner_ref(self_) }.replica.broker_id()
}

/// `toString()`: `topic-partition-brokerId`, as an owned string freed with
/// `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-partition-replica handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_to_string(
    self_: *const kafka_common_TopicPartitionReplica_t,
) -> *mut c_char {
    into_c_string(&unsafe { inner_ref(self_) }.replica.to_string())
}

/// Frees an owned topic-partition-replica handle. Null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_TopicPartitionReplica_destroy(self_: *mut kafka_common_TopicPartitionReplica_t) {
    if !self_.is_null() {
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

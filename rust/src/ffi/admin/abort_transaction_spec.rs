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

//! `kafka_admin_AbortTransactionSpec_t`:
//! `org.apache.kafka.clients.admin.AbortTransactionSpec` (CLAUDE.md §4).
//! The topic-partition getter returns a borrowed handle valid as long as the
//! spec.

use std::ffi::c_char;

use crate::admin::AbortTransactionSpec;
use crate::ffi::common::topic_partition::{TopicPartitionInner, kafka_common_TopicPartition_t, topic_partition_ref};
use crate::ffi::util::into_c_string;

/// Opaque handle to an [`AbortTransactionSpec`].
#[repr(C)]
pub struct kafka_admin_AbortTransactionSpec_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_AbortTransactionSpec_t`] points at: the spec plus the
/// handle of its topic partition, which the getter borrows out.
pub(crate) struct AbortTransactionSpecInner {
    spec: AbortTransactionSpec,
    topic_partition: TopicPartitionInner,
}

impl AbortTransactionSpecInner {
    pub(crate) fn new(spec: AbortTransactionSpec) -> Self {
        let topic_partition = TopicPartitionInner::new(spec.topic_partition().clone());
        Self { spec, topic_partition }
    }
}

unsafe fn inner_ref<'a>(spec: *const kafka_admin_AbortTransactionSpec_t) -> &'a AbortTransactionSpecInner {
    unsafe { &*(spec as *const AbortTransactionSpecInner) }
}

/// The spec behind a handle.
///
/// # Safety
///
/// `spec` must be a valid abort-transaction-spec handle.
pub(crate) unsafe fn abort_transaction_spec_ref<'a>(
    spec: *const kafka_admin_AbortTransactionSpec_t,
) -> &'a AbortTransactionSpec {
    &unsafe { inner_ref(spec) }.spec
}

/// Hands `spec` to C as an owned handle, freed with
/// [`kafka_admin_AbortTransactionSpec_destroy`].
pub(crate) fn box_abort_transaction_spec(spec: AbortTransactionSpec) -> *mut kafka_admin_AbortTransactionSpec_t {
    Box::into_raw(Box::new(AbortTransactionSpecInner::new(spec))) as *mut kafka_admin_AbortTransactionSpec_t
}

/// `new AbortTransactionSpec(TopicPartition topicPartition, long producerId,
/// short producerEpoch, int coordinatorEpoch)`: the partition is copied, the
/// caller keeps its handle. Owned, freed with
/// [`kafka_admin_AbortTransactionSpec_destroy`].
///
/// # Safety
///
/// `topic_partition` must be a valid topic-partition handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_new(
    topic_partition: *const kafka_common_TopicPartition_t,
    producer_id: i64,
    producer_epoch: i16,
    coordinator_epoch: i32,
) -> *mut kafka_admin_AbortTransactionSpec_t {
    box_abort_transaction_spec(AbortTransactionSpec::new(
        unsafe { topic_partition_ref(topic_partition) }.clone(),
        producer_id,
        producer_epoch,
        coordinator_epoch,
    ))
}

/// `topicPartition()`: a borrowed handle valid as long as the spec, never
/// passed to `kafka_common_TopicPartition_destroy`.
///
/// # Safety
///
/// `self_` must be a valid abort-transaction-spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_topic_partition(
    self_: *const kafka_admin_AbortTransactionSpec_t,
) -> *const kafka_common_TopicPartition_t {
    unsafe { inner_ref(self_) }.topic_partition.as_ptr()
}

/// `producerId()`.
///
/// # Safety
///
/// `self_` must be a valid abort-transaction-spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_producer_id(
    self_: *const kafka_admin_AbortTransactionSpec_t,
) -> i64 {
    unsafe { abort_transaction_spec_ref(self_) }.producer_id()
}

/// `producerEpoch()`.
///
/// # Safety
///
/// `self_` must be a valid abort-transaction-spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_producer_epoch(
    self_: *const kafka_admin_AbortTransactionSpec_t,
) -> i16 {
    unsafe { abort_transaction_spec_ref(self_) }.producer_epoch()
}

/// `coordinatorEpoch()`.
///
/// # Safety
///
/// `self_` must be a valid abort-transaction-spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_coordinator_epoch(
    self_: *const kafka_admin_AbortTransactionSpec_t,
) -> i32 {
    unsafe { abort_transaction_spec_ref(self_) }.coordinator_epoch()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid abort-transaction-spec handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_to_string(
    self_: *const kafka_admin_AbortTransactionSpec_t,
) -> *mut c_char {
    into_c_string(&unsafe { abort_transaction_spec_ref(self_) }.to_string())
}

/// Frees an owned spec handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned spec handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AbortTransactionSpec_destroy(self_: *mut kafka_admin_AbortTransactionSpec_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut AbortTransactionSpecInner) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;
    use std::ptr;

    use super::*;
    use crate::common::TopicPartition;
    use crate::ffi::common::topic_partition::{
        box_topic_partition, kafka_common_TopicPartition_destroy, kafka_common_TopicPartition_partition,
    };
    use crate::ffi::util::kafka_string_destroy;

    #[test]
    fn spec_copies_its_partition_and_borrows_it_out() {
        let tp = TopicPartition::new("orders", 3);
        let tp_handle = box_topic_partition(tp.clone());
        unsafe {
            let spec = kafka_admin_AbortTransactionSpec_new(tp_handle, 100, 2, 7);
            kafka_common_TopicPartition_destroy(tp_handle);
            let expected = AbortTransactionSpec::new(tp, 100, 2, 7);
            assert_eq!(*abort_transaction_spec_ref(spec), expected);
            assert_eq!(
                kafka_common_TopicPartition_partition(kafka_admin_AbortTransactionSpec_topic_partition(spec)),
                3
            );
            assert_eq!(kafka_admin_AbortTransactionSpec_producer_id(spec), 100);
            assert_eq!(kafka_admin_AbortTransactionSpec_producer_epoch(spec), 2);
            assert_eq!(kafka_admin_AbortTransactionSpec_coordinator_epoch(spec), 7);
            let s = kafka_admin_AbortTransactionSpec_to_string(spec);
            assert_eq!(CStr::from_ptr(s).to_str().unwrap(), expected.to_string());
            kafka_string_destroy(s);
            kafka_admin_AbortTransactionSpec_destroy(spec);
            kafka_admin_AbortTransactionSpec_destroy(ptr::null_mut());
        }
    }
}

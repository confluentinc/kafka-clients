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

//! C bindings for `org.apache.kafka.clients.admin.AlterReplicaLogDirsResult`.

use std::ffi::{CStr, c_void};

use crate::admin::AlterReplicaLogDirsResult;
use crate::common::{KafkaFuture, TopicPartitionReplica};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_result, result_ref};
use crate::ffi::common::topic_partition_replica::{
    kafka_common_TopicPartitionReplica_broker_id, kafka_common_TopicPartitionReplica_destroy,
    kafka_common_TopicPartitionReplica_new, kafka_common_TopicPartitionReplica_partition,
    kafka_common_TopicPartitionReplica_t, kafka_common_TopicPartitionReplica_topic,
};
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{kafka_Map_t, owned_c_string};

/// Opaque handle to an [`AlterReplicaLogDirsResult`], owned by the caller
/// and freed with [`kafka_admin_AlterReplicaLogDirsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_AlterReplicaLogDirsResult_t {
    _private: [u8; 0],
}

/// Hands `result` to C, binding the futures it exposes to `ctx`.
pub(crate) fn box_alter_replica_log_dirs_result(
    result: AlterReplicaLogDirsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_AlterReplicaLogDirsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle.
unsafe fn handle<'a>(
    self_: *const kafka_admin_AlterReplicaLogDirsResult_t,
) -> &'a ResultHandle<AlterReplicaLogDirsResult> {
    unsafe { result_ref(self_) }
}

/// An owned `kafka_common_TopicPartitionReplica_t *` key for `replica`,
/// built through the public constructor since the replica handle keeps its
/// own NUL-terminated topic copy.
fn box_replica_key(replica: &TopicPartitionReplica) -> *mut c_void {
    let topic = owned_c_string(replica.topic());
    // SAFETY: `topic` is a valid NUL-terminated string for the whole call.
    let key =
        unsafe { kafka_common_TopicPartitionReplica_new(topic.as_ptr(), replica.partition(), replica.broker_id()) };
    key as *mut c_void
}

/// Frees a `kafka_common_TopicPartitionReplica_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned `kafka_common_TopicPartitionReplica_t *`.
unsafe fn destroy_replica_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartitionReplica_destroy(element as *mut kafka_common_TopicPartitionReplica_t) }
}

/// Compares two `kafka_common_TopicPartitionReplica_t *` keys by value,
/// through the handle's getters.
///
/// # Safety
///
/// `a` and `b` must be valid `kafka_common_TopicPartitionReplica_t *`.
unsafe fn replica_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    let a = a as *const kafka_common_TopicPartitionReplica_t;
    let b = b as *const kafka_common_TopicPartitionReplica_t;
    unsafe {
        kafka_common_TopicPartitionReplica_partition(a) == kafka_common_TopicPartitionReplica_partition(b)
            && kafka_common_TopicPartitionReplica_broker_id(a) == kafka_common_TopicPartitionReplica_broker_id(b)
            && CStr::from_ptr(kafka_common_TopicPartitionReplica_topic(a))
                == CStr::from_ptr(kafka_common_TopicPartitionReplica_topic(b))
    }
}

/// `AlterReplicaLogDirsResult.values()`: an owned map, sorted by topic,
/// partition then broker id, of owned `kafka_common_TopicPartitionReplica_t *`
/// keys (compared by value in `kafka_Map_get`) to owned
/// `kafka_common_KafkaFuture_t *` (`KafkaFuture<Void>`: `get` delivers
/// `NULL`). Freed with `kafka_Map_destroy`, which frees the keys and the
/// futures.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_values(
    self_: *const kafka_admin_AlterReplicaLogDirsResult_t,
) -> *mut kafka_Map_t {
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&TopicPartitionReplica, &KafkaFuture<()>)> = h.result.values().iter().collect();
    entries.sort_by(|a, b| {
        (a.0.topic(), a.0.partition(), a.0.broker_id()).cmp(&(b.0.topic(), b.0.partition(), b.0.broker_id()))
    });
    h.ctx.keyed_future_map(
        entries,
        box_replica_key,
        destroy_replica_element,
        replica_key_eq,
        FutureCtx::void_future,
    )
}

/// `AlterReplicaLogDirsResult.all()`: an owned `KafkaFuture<Void>` (its
/// `get` delivers `NULL`) that succeeds once every replica move was
/// accepted, freed with `kafka_common_KafkaFuture_destroy`.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_all(
    self_: *const kafka_admin_AlterReplicaLogDirsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    let h = unsafe { handle(self_) };
    h.ctx.void_future(&h.result.all())
}

/// Frees a result handle; null is a no-op. Maps and futures taken from the
/// result stay valid until they are destroyed themselves.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_AlterReplicaLogDirsResult_destroy(
    self_: *mut kafka_admin_AlterReplicaLogDirsResult_t,
) {
    unsafe { destroy_result::<AlterReplicaLogDirsResult, _>(self_) }
}

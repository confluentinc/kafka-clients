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

//! `kafka_admin_DescribeReplicaLogDirsResult_t` and its nested
//! `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t`:
//! `org.apache.kafka.clients.admin.DescribeReplicaLogDirsResult` (CLAUDE.md
//! §4). Replicas cross as `kafka_common_TopicPartitionReplica_t *` map keys,
//! built and compared through that type's own C functions.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::ffi::{CStr, CString, c_char, c_void};

use crate::admin::{DescribeReplicaLogDirsResult, ReplicaLogDirInfo};
use crate::common::{KafkaFuture, TopicPartitionReplica};
use crate::ffi::admin::{FutureCtx, ResultHandle, box_result, destroy_map_element, destroy_result, result_ref};
use crate::ffi::common::topic_partition_replica::{
    kafka_common_TopicPartitionReplica_broker_id, kafka_common_TopicPartitionReplica_destroy,
    kafka_common_TopicPartitionReplica_new, kafka_common_TopicPartitionReplica_partition,
    kafka_common_TopicPartitionReplica_t, kafka_common_TopicPartitionReplica_topic,
};
use crate::ffi::ffi_guard;
use crate::ffi::kafka_future::kafka_common_KafkaFuture_t;
use crate::ffi::util::{box_map, into_c_string, kafka_Map_t, owned_c_string};

// ---------------------------------------------------------------------------
// DescribeReplicaLogDirsResult.ReplicaLogDirInfo
// ---------------------------------------------------------------------------

/// Opaque handle to a [`ReplicaLogDirInfo`], owned and freed with
/// [`kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t`]
/// points at: the value plus the NUL-terminated copies of its two nullable
/// log dirs that the string getters borrow out.
pub(crate) struct ReplicaLogDirInfoInner {
    info: ReplicaLogDirInfo,
    current_replica_log_dir_c: Option<CString>,
    future_replica_log_dir_c: Option<CString>,
}

impl ReplicaLogDirInfoInner {
    fn new(info: ReplicaLogDirInfo) -> Self {
        let current_replica_log_dir_c = info.current_replica_log_dir().map(owned_c_string);
        let future_replica_log_dir_c = info.future_replica_log_dir().map(owned_c_string);
        Self { info, current_replica_log_dir_c, future_replica_log_dir_c }
    }
}

/// What a replica-log-dir-info handle points at.
///
/// # Safety
///
/// `info` must be a live replica-log-dir-info handle, kept alive for `'a`.
unsafe fn info_inner_ref<'a>(
    info: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> &'a ReplicaLogDirInfoInner {
    // SAFETY: Per this function's `# Safety`, `info` is a live replica-log-dir-info handle kept
    // alive for `'a`. Every `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t` is the
    // `Box::into_raw` of a `Box<ReplicaLogDirInfoInner>` (`box_replica_log_dir_info`, the only
    // place one is made, for an owned handle or a value a map or future owns), so it points at a
    // live `ReplicaLogDirInfoInner`. Nothing takes a `&mut` to it: only the destroy takes the box
    // back, and its `# Safety` excludes any other use at the same time or afterwards.
    unsafe { &*(info as *const ReplicaLogDirInfoInner) }
}

/// A borrowed `char *` for an optional NUL-terminated copy, `NULL` for none.
fn optional_c_str(value: &Option<CString>) -> *const c_char {
    value.as_ref().map_or(std::ptr::null(), |c| c.as_ptr())
}

/// Hands `info` to C as an owned handle.
pub(crate) fn box_replica_log_dir_info(
    info: ReplicaLogDirInfo,
) -> *mut kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t {
    Box::into_raw(Box::new(ReplicaLogDirInfoInner::new(info)))
        as *mut kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `info` must be a live replica-log-dir-info handle, kept alive for `'a`.
pub(crate) unsafe fn replica_log_dir_info_ref<'a>(
    info: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> &'a ReplicaLogDirInfo {
    // SAFETY: Per this function's `# Safety`, `info` is a live replica-log-dir-info handle kept
    // alive for `'a`, as `info_inner_ref` requires.
    &unsafe { info_inner_ref(info) }.info
}

/// Frees a `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *`
/// element of an owned container or future.
///
/// # Safety
///
/// `element` must be an owned replica-log-dir-info handle not yet
/// destroyed, that no other call uses at the same time; it is not used
/// afterwards.
pub(crate) unsafe fn destroy_replica_log_dir_info_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned replica-log-dir-info handle not
    // yet destroyed, that no other call uses at the same time and that is not used afterwards, as
    // `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_destroy` requires.
    unsafe {
        kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_destroy(
            element as *mut kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
        )
    }
}

/// `getCurrentReplicaLogDir()`: borrowed from the handle, valid until it is
/// destroyed; `NULL` when the replica has no current log dir.
///
/// # Safety
///
/// `self_` must be a live replica-log-dir-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_log_dir(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live replica-log-dir-info handle, kept
    // alive for this call, as `info_inner_ref` requires. The pointer handed out borrows the
    // handle's own copy, which lives as long as the handle.
    optional_c_str(&unsafe { info_inner_ref(self_) }.current_replica_log_dir_c)
}

/// `getCurrentReplicaOffsetLag()`.
///
/// # Safety
///
/// `self_` must be a live replica-log-dir-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_current_replica_offset_lag(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live replica-log-dir-info handle, kept
    // alive for this call, as `replica_log_dir_info_ref` requires; the borrow ends with the getter.
    unsafe { replica_log_dir_info_ref(self_) }.current_replica_offset_lag()
}

/// `getFutureReplicaLogDir()`: borrowed from the handle, valid until it is
/// destroyed; `NULL` when the replica is not being moved.
///
/// # Safety
///
/// `self_` must be a live replica-log-dir-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_log_dir(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live replica-log-dir-info handle, kept
    // alive for this call, as `info_inner_ref` requires. The pointer handed out borrows the
    // handle's own copy, which lives as long as the handle.
    optional_c_str(&unsafe { info_inner_ref(self_) }.future_replica_log_dir_c)
}

/// `getFutureReplicaOffsetLag()`.
///
/// # Safety
///
/// `self_` must be a live replica-log-dir-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_future_replica_offset_lag(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live replica-log-dir-info handle, kept
    // alive for this call, as `replica_log_dir_info_ref` requires; the borrow ends with the getter.
    unsafe { replica_log_dir_info_ref(self_) }.future_replica_offset_lag()
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a live replica-log-dir-info handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_to_string(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a live replica-log-dir-info handle, kept
    // alive for this call, as `replica_log_dir_info_ref` requires; the borrow ends with
    // `to_string`.
    into_c_string(&unsafe { replica_log_dir_info_ref(self_) }.to_string())
}

/// Frees an owned replica-log-dir-info handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_destroy(
    self_: *mut kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t,
) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, an owned
        // handle not yet destroyed, that no other call uses at the same time and that is not used
        // afterwards. Every such handle is the `Box::into_raw` of a `Box<ReplicaLogDirInfoInner>`
        // (`box_replica_log_dir_info`), so `from_raw` reclaims that allocation, once.
        drop(unsafe { Box::from_raw(self_ as *mut ReplicaLogDirInfoInner) });
    }
}

// ---------------------------------------------------------------------------
// DescribeReplicaLogDirsResult
// ---------------------------------------------------------------------------

/// Opaque handle to a [`DescribeReplicaLogDirsResult`], owned and freed with
/// [`kafka_admin_DescribeReplicaLogDirsResult_destroy`].
#[repr(C)]
pub struct kafka_admin_DescribeReplicaLogDirsResult_t {
    _private: [u8; 0],
}

/// Hands `replica` to C as an owned `kafka_common_TopicPartitionReplica_t *`
/// map key, through that type's own constructor.
fn box_topic_partition_replica(replica: &TopicPartitionReplica) -> *mut c_void {
    let topic = owned_c_string(replica.topic());
    // SAFETY: `topic` is the `CString` made just above, so `topic.as_ptr()` is a valid
    // NUL-terminated string, alive for this call, as `kafka_common_TopicPartitionReplica_new`
    // requires.
    let key =
        unsafe { kafka_common_TopicPartitionReplica_new(topic.as_ptr(), replica.partition(), replica.broker_id()) };
    key as *mut c_void
}

/// Frees a `kafka_common_TopicPartitionReplica_t *` key of an owned map.
///
/// # Safety
///
/// `element` must be an owned topic-partition-replica handle not yet
/// destroyed.
unsafe fn destroy_topic_partition_replica_element(element: *mut c_void) {
    // SAFETY: Per this function's `# Safety`, `element` is an owned topic-partition-replica handle
    // not yet destroyed, as `kafka_common_TopicPartitionReplica_destroy` requires.
    unsafe { kafka_common_TopicPartitionReplica_destroy(element as *mut kafka_common_TopicPartitionReplica_t) }
}

/// Compares two `kafka_common_TopicPartitionReplica_t *` keys by value, for
/// `kafka_Map_get`.
///
/// # Safety
///
/// `a` and `b` must each be null or a valid topic-partition-replica
/// handle, alive for this call.
unsafe fn topic_partition_replica_key_eq(a: *mut c_void, b: *mut c_void) -> bool {
    if a.is_null() || b.is_null() {
        return a == b;
    }
    let a = a as *const kafka_common_TopicPartitionReplica_t;
    let b = b as *const kafka_common_TopicPartitionReplica_t;
    // SAFETY: `a` and `b` are non-null (checked above) and, per this function's `# Safety`, valid
    // topic-partition-replica handles, alive for this call, as the three getters require.
    // `kafka_common_TopicPartitionReplica_topic` returns the handle's NUL-terminated topic, valid
    // until the handle is destroyed, so both `CStr` borrows, which end with the comparison, are
    // valid.
    unsafe {
        kafka_common_TopicPartitionReplica_partition(a) == kafka_common_TopicPartitionReplica_partition(b)
            && kafka_common_TopicPartitionReplica_broker_id(a) == kafka_common_TopicPartitionReplica_broker_id(b)
            && CStr::from_ptr(kafka_common_TopicPartitionReplica_topic(a))
                == CStr::from_ptr(kafka_common_TopicPartitionReplica_topic(b))
    }
}

/// The deterministic order of replica-keyed entries: topic, partition,
/// broker id.
fn replica_order(a: &TopicPartitionReplica, b: &TopicPartitionReplica) -> Ordering {
    a.topic()
        .cmp(b.topic())
        .then(a.partition().cmp(&b.partition()))
        .then(a.broker_id().cmp(&b.broker_id()))
}

/// Java's `Map<TopicPartitionReplica, ReplicaLogDirInfo>` as an owned map of
/// owned `kafka_common_TopicPartitionReplica_t *` keys (compared by value) to
/// owned `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *`,
/// ordered by topic, partition, then broker id.
fn infos_map(map: HashMap<TopicPartitionReplica, ReplicaLogDirInfo>) -> *mut c_void {
    let mut entries: Vec<(TopicPartitionReplica, ReplicaLogDirInfo)> = map.into_iter().collect();
    entries.sort_by(|(a, _), (b, _)| replica_order(a, b));
    let entries = entries
        .into_iter()
        .map(|(replica, info)| {
            (
                box_topic_partition_replica(&replica),
                box_replica_log_dir_info(info) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_topic_partition_replica_element),
        Some(destroy_replica_log_dir_info_element),
        Some(topic_partition_replica_key_eq),
    ) as *mut c_void
}

/// Hands `result` to C (used by the RPC functions in `rpc.rs`).
pub(crate) fn box_describe_replica_log_dirs_result(
    result: DescribeReplicaLogDirsResult,
    ctx: &FutureCtx,
) -> *mut kafka_admin_DescribeReplicaLogDirsResult_t {
    box_result(result, ctx)
}

/// The handle `self_` points at.
///
/// # Safety
///
/// `self_` must be a live result handle, kept alive for `'a`.
unsafe fn handle<'a>(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_t,
) -> &'a ResultHandle<DescribeReplicaLogDirsResult> {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle kept alive for `'a`,
    // and every `kafka_admin_DescribeReplicaLogDirsResult_t` is built by
    // `box_result::<DescribeReplicaLogDirsResult, _>` (in `box_describe_replica_log_dirs_result`),
    // as `result_ref` requires.
    unsafe { result_ref(self_) }
}

/// `values()`: an owned `kafka_Map_t` (freed with `kafka_Map_destroy`, which
/// frees its keys and values) of owned `kafka_common_TopicPartitionReplica_t *`
/// keys, ordered by topic, partition, then broker id and compared by value
/// in `kafka_Map_get`, to owned `kafka_common_KafkaFuture_t *` whose `get`
/// delivers a `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *`
/// owned by the future.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_values(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    let mut entries: Vec<(&TopicPartitionReplica, &KafkaFuture<ReplicaLogDirInfo>)> =
        h.result.values().iter().collect();
    entries.sort_by(|(a, _), (b, _)| replica_order(a, b));
    h.ctx.keyed_future_map(
        entries,
        box_topic_partition_replica,
        destroy_topic_partition_replica_element,
        topic_partition_replica_key_eq,
        |ctx, f| {
            // SAFETY: `destroy_replica_log_dir_info_element` frees exactly the handle
            // `box_replica_log_dir_info` makes, and the handle holds only owned data and no
            // thread-bound state, so it may be freed from any thread and read from several at once,
            // as `handle_future` requires.
            unsafe {
                ctx.handle_future(
                    f,
                    |i| box_replica_log_dir_info(i) as *mut c_void,
                    destroy_replica_log_dir_info_element,
                )
            }
        },
    )
}

/// `all()`: an owned future freed with `kafka_common_KafkaFuture_destroy`;
/// its `get` delivers a `kafka_Map_t *` (owned by the future) of
/// `kafka_common_TopicPartitionReplica_t *` keys, ordered by topic,
/// partition, then broker id and compared by value in `kafka_Map_get`, to
/// `kafka_admin_DescribeReplicaLogDirsResult_ReplicaLogDirInfo_t *` values.
///
/// # Safety
///
/// `self_` must be a live result handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_all(
    self_: *const kafka_admin_DescribeReplicaLogDirsResult_t,
) -> *mut kafka_common_KafkaFuture_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live result handle, kept alive for this
    // call, as `handle` requires.
    let h = unsafe { handle(self_) };
    // SAFETY: `destroy_map_element` frees exactly the map `infos_map` makes through `box_map`, and
    // the handle holds only owned data and no thread-bound state, so it may be freed from any
    // thread and read from several at once, as `handle_future` requires.
    unsafe { h.ctx.handle_future(&h.result.all(), infos_map, destroy_map_element) }
}

/// Frees a result handle; null is a no-op. Futures already taken from it
/// stay valid.
///
/// # Safety
///
/// `self_` must be null or an owned result handle not yet destroyed, that
/// no other call uses at the same time; it is not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_DescribeReplicaLogDirsResult_destroy(
    self_: *mut kafka_admin_DescribeReplicaLogDirsResult_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is null or an owned result handle not yet
    // destroyed, that no other call uses at the same time and that is not used afterwards; every
    // `kafka_admin_DescribeReplicaLogDirsResult_t` is built by
    // `box_result::<DescribeReplicaLogDirsResult, _>`, as `destroy_result` requires.
    unsafe { destroy_result::<DescribeReplicaLogDirsResult, _>(self_) }
}

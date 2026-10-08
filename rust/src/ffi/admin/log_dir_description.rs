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

//! `kafka_admin_LogDirDescription_t`:
//! `org.apache.kafka.clients.admin.LogDirDescription` (CLAUDE.md §4).
//!
//! Java's `error()` is a nullable `ApiException`: the C getter returns a
//! borrowed `kafka_common_Error_t` valid as long as the description, or null.
//! `OptionalLong totalBytes()` / `usableBytes()` cross as `-1` when empty,
//! which is also `DescribeLogDirsResponse.UNKNOWN_VOLUME_BYTES`, the value the
//! constructors read as "unknown".

use std::collections::HashMap;
use std::ffi::{c_char, c_void};
use std::ptr;

use crate::admin::{LogDirDescription, ReplicaInfo};
use crate::common::TopicPartition;
use crate::ffi::admin::replica_info::{
    box_replica_info, destroy_replica_info_element, kafka_admin_ReplicaInfo_t, replica_info_ref,
};
use crate::ffi::common::topic_partition::{
    TopicPartitionInner, box_topic_partition, kafka_common_TopicPartition_t, topic_partition_key_eq,
    topic_partition_ref,
};
use crate::ffi::common::{ErrorInner, kafka_common_Error_t, take_error};
use crate::ffi::util::{box_map, destroy_boxed, into_c_string, kafka_Map_t, map_entries};

/// Opaque handle to a [`LogDirDescription`].
#[repr(C)]
pub struct kafka_admin_LogDirDescription_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_LogDirDescription_t`] points at: the value plus the
/// error handle its getter borrows out.
pub(crate) struct LogDirDescriptionInner {
    description: LogDirDescription,
    error: Option<ErrorInner>,
}

impl LogDirDescriptionInner {
    fn new(description: LogDirDescription) -> Self {
        let error = description.error().cloned().map(ErrorInner::new);
        Self { description, error }
    }
}

unsafe fn inner_ref<'a>(description: *const kafka_admin_LogDirDescription_t) -> &'a LogDirDescriptionInner {
    unsafe { &*(description as *const LogDirDescriptionInner) }
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_LogDirDescription_destroy`].
pub(crate) fn box_log_dir_description(description: LogDirDescription) -> *mut kafka_admin_LogDirDescription_t {
    Box::into_raw(Box::new(LogDirDescriptionInner::new(description))) as *mut kafka_admin_LogDirDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live log-dir-description handle.
pub(crate) unsafe fn log_dir_description_ref<'a>(
    description: *const kafka_admin_LogDirDescription_t,
) -> &'a LogDirDescription {
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_LogDirDescription_t *` element of an owned
/// container.
///
/// # Safety
///
/// `element` must be an owned log-dir-description handle.
pub(crate) unsafe fn destroy_log_dir_description_element(element: *mut c_void) {
    unsafe { kafka_admin_LogDirDescription_destroy(element as *mut kafka_admin_LogDirDescription_t) }
}

/// Reads a map of `const kafka_common_TopicPartition_t *` to
/// `const kafka_admin_ReplicaInfo_t *` into owned copies; null reads as
/// empty.
unsafe fn map_replica_infos(map: *const kafka_Map_t) -> HashMap<TopicPartition, ReplicaInfo> {
    unsafe { map_entries(map) }
        .iter()
        .map(|&(k, v)| {
            (
                unsafe { topic_partition_ref(k as *const kafka_common_TopicPartition_t) }.clone(),
                unsafe { replica_info_ref(v as *const kafka_admin_ReplicaInfo_t) }.clone(),
            )
        })
        .collect()
}

/// `new LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo>
/// replicaInfos)`. `error` is nullable and **consumed**: the description
/// takes ownership of the handle, the caller does not destroy it.
/// `replica_infos` is a borrowed map of `const kafka_common_TopicPartition_t *`
/// to `const kafka_admin_ReplicaInfo_t *`, copied (null reads as empty).
/// Owned, freed with [`kafka_admin_LogDirDescription_destroy`].
///
/// # Safety
///
/// `error` must be null or an owned error handle not yet destroyed;
/// `replica_infos` null or a valid map of the documented key and value
/// handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_new(
    error: *mut kafka_common_Error_t,
    replica_infos: *const kafka_Map_t,
) -> *mut kafka_admin_LogDirDescription_t {
    box_log_dir_description(LogDirDescription::new(unsafe { take_error(error) }, unsafe {
        map_replica_infos(replica_infos)
    }))
}

/// `new LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo>
/// replicaInfos, long totalBytes, long usableBytes)`: as
/// [`kafka_admin_LogDirDescription_new`], plus the volume sizes (`-1` =
/// `UNKNOWN_VOLUME_BYTES`, read as empty).
///
/// # Safety
///
/// As [`kafka_admin_LogDirDescription_new`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_with_total_bytes_usable_bytes(
    error: *mut kafka_common_Error_t,
    replica_infos: *const kafka_Map_t,
    total_bytes: i64,
    usable_bytes: i64,
) -> *mut kafka_admin_LogDirDescription_t {
    box_log_dir_description(LogDirDescription::with_total_bytes_usable_bytes(
        unsafe { take_error(error) },
        unsafe { map_replica_infos(replica_infos) },
        total_bytes,
        usable_bytes,
    ))
}

/// `new LogDirDescription(ApiException error, Map<TopicPartition, ReplicaInfo>
/// replicaInfos, long totalBytes, long usableBytes, boolean isCordoned)`: as
/// [`kafka_admin_LogDirDescription_with_total_bytes_usable_bytes`], plus the
/// cordoned flag.
///
/// # Safety
///
/// As [`kafka_admin_LogDirDescription_new`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_with_total_bytes_usable_bytes_is_cordoned(
    error: *mut kafka_common_Error_t,
    replica_infos: *const kafka_Map_t,
    total_bytes: i64,
    usable_bytes: i64,
    is_cordoned: i8,
) -> *mut kafka_admin_LogDirDescription_t {
    box_log_dir_description(LogDirDescription::with_total_bytes_usable_bytes_is_cordoned(
        unsafe { take_error(error) },
        unsafe { map_replica_infos(replica_infos) },
        total_bytes,
        usable_bytes,
        is_cordoned != 0,
    ))
}

/// `error()`: a borrowed error handle valid as long as the description
/// (never passed to `kafka_common_Error_destroy`), or null when the log
/// directory was described without error.
///
/// # Safety
///
/// `self_` must be a valid log-dir-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_error(
    self_: *const kafka_admin_LogDirDescription_t,
) -> *const kafka_common_Error_t {
    unsafe { inner_ref(self_) }
        .error
        .as_ref()
        .map_or(ptr::null(), |error| error as *const ErrorInner as *const kafka_common_Error_t)
}

/// `replicaInfos()`: an owned map of owned `kafka_common_TopicPartition_t *`
/// keys (compared by value in `kafka_Map_get`) to owned
/// `kafka_admin_ReplicaInfo_t *` copies, ordered by topic then partition,
/// freed with `kafka_Map_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-dir-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_replica_infos(
    self_: *const kafka_admin_LogDirDescription_t,
) -> *mut kafka_Map_t {
    let mut entries: Vec<(&TopicPartition, &ReplicaInfo)> =
        unsafe { log_dir_description_ref(self_) }.replica_infos().iter().collect();
    entries.sort_by(|(a, _), (b, _)| a.topic().cmp(b.topic()).then(a.partition().cmp(&b.partition())));
    let entries = entries
        .into_iter()
        .map(|(tp, info)| {
            (
                box_topic_partition(tp.clone()) as *mut c_void,
                box_replica_info(info.clone()) as *mut c_void,
            )
        })
        .collect();
    box_map(
        entries,
        Some(destroy_boxed::<TopicPartitionInner>),
        Some(destroy_replica_info_element),
        Some(topic_partition_key_eq),
    )
}

/// `totalBytes()`: the volume size, or `-1` for `OptionalLong.empty()`.
///
/// # Safety
///
/// `self_` must be a valid log-dir-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_total_bytes(
    self_: *const kafka_admin_LogDirDescription_t,
) -> i64 {
    unsafe { log_dir_description_ref(self_) }.total_bytes().unwrap_or(-1)
}

/// `usableBytes()`: the usable volume size, or `-1` for
/// `OptionalLong.empty()`.
///
/// # Safety
///
/// `self_` must be a valid log-dir-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_usable_bytes(
    self_: *const kafka_admin_LogDirDescription_t,
) -> i64 {
    unsafe { log_dir_description_ref(self_) }.usable_bytes().unwrap_or(-1)
}

/// `isCordoned()`.
///
/// # Safety
///
/// `self_` must be a valid log-dir-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_is_cordoned(
    self_: *const kafka_admin_LogDirDescription_t,
) -> i8 {
    i8::from(unsafe { log_dir_description_ref(self_) }.is_cordoned())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid log-dir-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_to_string(
    self_: *const kafka_admin_LogDirDescription_t,
) -> *mut c_char {
    into_c_string(&unsafe { log_dir_description_ref(self_) }.to_string())
}

/// Frees an owned log-dir-description handle, with the error it holds; null
/// is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_LogDirDescription_destroy(self_: *mut kafka_admin_LogDirDescription_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut LogDirDescriptionInner) });
    }
}

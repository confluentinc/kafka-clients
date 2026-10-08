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

//! `kafka_admin_MemberAssignment_t`:
//! `org.apache.kafka.clients.admin.MemberAssignment` (CLAUDE.md §4). The
//! handle points at the Rust value, so a `MemberAssignment` held inside a
//! `kafka_admin_MemberDescription_t` is borrowed out by address.

use std::collections::HashSet;
use std::ffi::c_char;

use crate::admin::MemberAssignment;
use crate::common::TopicPartition;
use crate::ffi::common::topic_partition::{list_topic_partitions, sorted_topic_partition_list};
use crate::ffi::util::{into_c_string, kafka_List_t};

/// Opaque handle to a [`MemberAssignment`].
#[repr(C)]
pub struct kafka_admin_MemberAssignment_t {
    _private: [u8; 0],
}

/// Hands `assignment` to C as an owned handle, freed with
/// [`kafka_admin_MemberAssignment_destroy`].
pub(crate) fn box_member_assignment(assignment: MemberAssignment) -> *mut kafka_admin_MemberAssignment_t {
    Box::into_raw(Box::new(assignment)) as *mut kafka_admin_MemberAssignment_t
}

/// A borrowed handle on `assignment`, valid as long as the value stays where
/// it is (inside a boxed parent handle).
pub(crate) fn member_assignment_ptr(assignment: &MemberAssignment) -> *const kafka_admin_MemberAssignment_t {
    assignment as *const MemberAssignment as *const kafka_admin_MemberAssignment_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `assignment` must be a live member-assignment handle.
pub(crate) unsafe fn member_assignment_ref<'a>(
    assignment: *const kafka_admin_MemberAssignment_t,
) -> &'a MemberAssignment {
    unsafe { &*(assignment as *const MemberAssignment) }
}

/// `new MemberAssignment(Set<TopicPartition> topicPartitions)`: a borrowed
/// list of `const kafka_common_TopicPartition_t *`, copied (null reads as
/// empty). Owned, freed with [`kafka_admin_MemberAssignment_destroy`].
///
/// # Safety
///
/// `topic_partitions` must be null or a valid list of topic-partition
/// handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_new(
    topic_partitions: *const kafka_List_t,
) -> *mut kafka_admin_MemberAssignment_t {
    let partitions: HashSet<TopicPartition> = unsafe { list_topic_partitions(topic_partitions) }.into_iter().collect();
    box_member_assignment(MemberAssignment::new(partitions))
}

/// `topicPartitions()`: an owned list of owned `kafka_common_TopicPartition_t *`
/// copies ordered by topic then partition, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid member-assignment handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_topic_partitions(
    self_: *const kafka_admin_MemberAssignment_t,
) -> *mut kafka_List_t {
    sorted_topic_partition_list(unsafe { member_assignment_ref(self_) }.topic_partitions())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid member-assignment handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_to_string(
    self_: *const kafka_admin_MemberAssignment_t,
) -> *mut c_char {
    into_c_string(&unsafe { member_assignment_ref(self_) }.to_string())
}

/// Frees an owned member-assignment handle. Null is a no-op; a handle
/// borrowed from a `kafka_admin_MemberDescription_t` is never passed here.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_MemberAssignment_destroy(self_: *mut kafka_admin_MemberAssignment_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut MemberAssignment) });
    }
}

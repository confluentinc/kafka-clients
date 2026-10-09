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

//! `kafka_admin_TopicDescription_t`:
//! `org.apache.kafka.clients.admin.TopicDescription` (CLAUDE.md §4).
//!
//! Java's `authorizedOperations()` is null when the broker did not report
//! them and a (possibly empty) set otherwise; the C getter keeps the
//! distinction by returning null or a list of `kafka_common_acl_AclOperation_t`
//! singletons. This file also hosts the two helpers that convert such a set
//! for the group descriptions.

use std::collections::BTreeSet;
use std::ffi::{CString, c_char, c_void};
use std::ptr;

use crate::admin::TopicDescription;
use crate::common::TopicPartitionInfo;
use crate::common::acl::AclOperation;
use crate::ffi::common::acl::acl_operation::{self, kafka_common_acl_AclOperation_t};
use crate::ffi::common::topic_partition_info::{
    box_topic_partition_info, kafka_common_TopicPartitionInfo_destroy, kafka_common_TopicPartitionInfo_t,
    topic_partition_info_ref,
};
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_t, uuid_of};
use crate::ffi::util::{box_list, c_str_to_string, into_c_string, kafka_List_t, list_elements, owned_c_string};

/// Opaque handle to a [`TopicDescription`].
#[repr(C)]
pub struct kafka_admin_TopicDescription_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_TopicDescription_t`] points at: the value plus the
/// NUL-terminated name its getter borrows out.
pub(crate) struct TopicDescriptionInner {
    description: TopicDescription,
    name_c: CString,
}

impl TopicDescriptionInner {
    fn new(description: TopicDescription) -> Self {
        let name_c = owned_c_string(description.name());
        Self { description, name_c }
    }
}

unsafe fn inner_ref<'a>(description: *const kafka_admin_TopicDescription_t) -> &'a TopicDescriptionInner {
    unsafe { &*(description as *const TopicDescriptionInner) }
}

/// Hands `description` to C as an owned handle, freed with
/// [`kafka_admin_TopicDescription_destroy`].
pub(crate) fn box_topic_description(description: TopicDescription) -> *mut kafka_admin_TopicDescription_t {
    Box::into_raw(Box::new(TopicDescriptionInner::new(description))) as *mut kafka_admin_TopicDescription_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `description` must be a live topic-description handle.
pub(crate) unsafe fn topic_description_ref<'a>(
    description: *const kafka_admin_TopicDescription_t,
) -> &'a TopicDescription {
    &unsafe { inner_ref(description) }.description
}

/// Frees a `kafka_admin_TopicDescription_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned topic-description handle.
pub(crate) unsafe fn destroy_topic_description_element(element: *mut c_void) {
    unsafe { kafka_admin_TopicDescription_destroy(element as *mut kafka_admin_TopicDescription_t) }
}

/// Frees a `kafka_common_TopicPartitionInfo_t *` element of an owned list.
unsafe fn destroy_topic_partition_info_element(element: *mut c_void) {
    unsafe { kafka_common_TopicPartitionInfo_destroy(element as *mut kafka_common_TopicPartitionInfo_t) }
}

/// A Java `Set<AclOperation>` as an owned list of borrowed
/// `const kafka_common_acl_AclOperation_t *` singletons (the list owns no
/// element), or null for Java's null set.
pub(crate) fn acl_operation_list(operations: Option<&BTreeSet<AclOperation>>) -> *mut kafka_List_t {
    match operations {
        Some(operations) => {
            let elements = operations
                .iter()
                .map(|&op| acl_operation::singleton(op) as *mut c_void)
                .collect();
            box_list(elements, None)
        },
        None => ptr::null_mut(),
    }
}

/// Reads a list of `const kafka_common_acl_AclOperation_t *` singletons into a
/// Java `Set<AclOperation>`; a null list is Java's null set.
///
/// # Safety
///
/// `list` must be null or a valid list whose elements are ACL-operation
/// singletons.
pub(crate) unsafe fn list_acl_operations(list: *const kafka_List_t) -> Option<BTreeSet<AclOperation>> {
    if list.is_null() {
        return None;
    }
    Some(
        unsafe { list_elements(list) }
            .iter()
            .map(|&element| unsafe { acl_operation::value_of(element as *const kafka_common_acl_AclOperation_t) })
            .collect(),
    )
}

/// Reads a list of `const kafka_common_TopicPartitionInfo_t *` into owned
/// copies; null reads as empty.
unsafe fn list_topic_partition_infos(list: *const kafka_List_t) -> Vec<TopicPartitionInfo> {
    unsafe { list_elements(list) }
        .iter()
        .map(|&element| {
            unsafe { topic_partition_info_ref(element as *const kafka_common_TopicPartitionInfo_t) }.clone()
        })
        .collect()
}

/// `new TopicDescription(String name, boolean internal,
/// List<TopicPartitionInfo> partitions)`: `partitions` is a borrowed list of
/// `const kafka_common_TopicPartitionInfo_t *`, copied (null reads as empty);
/// the authorized operations are the empty set and the topic id
/// `Uuid.ZERO_UUID`, as in Java. Owned, freed with
/// [`kafka_admin_TopicDescription_destroy`].
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `partitions` null or a
/// valid list of topic-partition-info handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_new(
    name: *const c_char,
    internal: i8,
    partitions: *const kafka_List_t,
) -> *mut kafka_admin_TopicDescription_t {
    box_topic_description(TopicDescription::new(unsafe { c_str_to_string(name) }, internal != 0, unsafe {
        list_topic_partition_infos(partitions)
    }))
}

/// `new TopicDescription(String name, boolean internal,
/// List<TopicPartitionInfo> partitions, Set<AclOperation>
/// authorizedOperations)`: as [`kafka_admin_TopicDescription_new`], plus a
/// borrowed list of `const kafka_common_acl_AclOperation_t *` singletons,
/// copied; a null list is Java's null set.
///
/// # Safety
///
/// As [`kafka_admin_TopicDescription_new`], plus `authorized_operations`
/// null or a valid list of ACL-operation singletons.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_with_authorized_operations(
    name: *const c_char,
    internal: i8,
    partitions: *const kafka_List_t,
    authorized_operations: *const kafka_List_t,
) -> *mut kafka_admin_TopicDescription_t {
    box_topic_description(TopicDescription::with_authorized_operations(
        unsafe { c_str_to_string(name) },
        internal != 0,
        unsafe { list_topic_partition_infos(partitions) },
        unsafe { list_acl_operations(authorized_operations) },
    ))
}

/// `new TopicDescription(String name, boolean internal,
/// List<TopicPartitionInfo> partitions, Set<AclOperation>
/// authorizedOperations, Uuid topicId)`: as
/// [`kafka_admin_TopicDescription_with_authorized_operations`], plus the
/// topic id, copied.
///
/// # Safety
///
/// As [`kafka_admin_TopicDescription_with_authorized_operations`], plus
/// `topic_id` a valid uuid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_with_authorized_operations_topic_id(
    name: *const c_char,
    internal: i8,
    partitions: *const kafka_List_t,
    authorized_operations: *const kafka_List_t,
    topic_id: *const kafka_common_Uuid_t,
) -> *mut kafka_admin_TopicDescription_t {
    box_topic_description(TopicDescription::with_authorized_operations_topic_id(
        unsafe { c_str_to_string(name) },
        internal != 0,
        unsafe { list_topic_partition_infos(partitions) },
        unsafe { list_acl_operations(authorized_operations) },
        unsafe { uuid_of(topic_id) },
    ))
}

/// `name()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid topic-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_name(
    self_: *const kafka_admin_TopicDescription_t,
) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `isInternal()`.
///
/// # Safety
///
/// `self_` must be a valid topic-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_is_internal(self_: *const kafka_admin_TopicDescription_t) -> i8 {
    i8::from(unsafe { topic_description_ref(self_) }.is_internal())
}

/// `topicId()`: an owned copy, freed with `kafka_common_Uuid_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_topic_id(
    self_: *const kafka_admin_TopicDescription_t,
) -> *mut kafka_common_Uuid_t {
    box_uuid(unsafe { topic_description_ref(self_) }.topic_id())
}

/// `partitions()`: an owned list of owned `kafka_common_TopicPartitionInfo_t *`
/// copies, in partition order, freed with `kafka_List_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_partitions(
    self_: *const kafka_admin_TopicDescription_t,
) -> *mut kafka_List_t {
    let elements = unsafe { topic_description_ref(self_) }
        .partitions()
        .iter()
        .map(|info| box_topic_partition_info(info.clone()) as *mut c_void)
        .collect();
    box_list(elements, Some(destroy_topic_partition_info_element))
}

/// `authorizedOperations()`: an owned list (freed with `kafka_List_destroy`)
/// of borrowed `const kafka_common_acl_AclOperation_t *` singletons, or null
/// when the broker did not report them (Java's null).
///
/// # Safety
///
/// `self_` must be a valid topic-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_authorized_operations(
    self_: *const kafka_admin_TopicDescription_t,
) -> *mut kafka_List_t {
    acl_operation_list(unsafe { topic_description_ref(self_) }.authorized_operations())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-description handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_to_string(
    self_: *const kafka_admin_TopicDescription_t,
) -> *mut c_char {
    into_c_string(&unsafe { topic_description_ref(self_) }.to_string())
}

/// Frees an owned topic-description handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicDescription_destroy(self_: *mut kafka_admin_TopicDescription_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TopicDescriptionInner) });
    }
}

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

//! `kafka_admin_GroupListing_t`: `org.apache.kafka.clients.admin.GroupListing`
//! (CLAUDE.md §4). Java's `Optional<GroupType>` and `Optional<GroupState>`
//! cross as a nullable singleton pointer: null is `Optional.empty()`.

use std::ffi::{CString, c_char, c_void};
use std::ptr;

use crate::admin::GroupListing;
use crate::common::{GroupState, GroupType};
use crate::ffi::common::group_state::{self, kafka_common_GroupState_t};
use crate::ffi::common::group_type::{self, kafka_common_GroupType_t};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`GroupListing`].
#[repr(C)]
pub struct kafka_admin_GroupListing_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_GroupListing_t`] points at: the value plus the
/// NUL-terminated strings its getters borrow out.
pub(crate) struct GroupListingInner {
    listing: GroupListing,
    group_id_c: CString,
    protocol_c: CString,
}

impl GroupListingInner {
    fn new(listing: GroupListing) -> Self {
        let group_id_c = owned_c_string(listing.group_id());
        let protocol_c = owned_c_string(listing.protocol());
        Self { listing, group_id_c, protocol_c }
    }
}

unsafe fn inner_ref<'a>(listing: *const kafka_admin_GroupListing_t) -> &'a GroupListingInner {
    unsafe { &*(listing as *const GroupListingInner) }
}

/// Hands `listing` to C as an owned handle, freed with
/// [`kafka_admin_GroupListing_destroy`].
pub(crate) fn box_group_listing(listing: GroupListing) -> *mut kafka_admin_GroupListing_t {
    Box::into_raw(Box::new(GroupListingInner::new(listing))) as *mut kafka_admin_GroupListing_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `listing` must be a live group-listing handle.
pub(crate) unsafe fn group_listing_ref<'a>(listing: *const kafka_admin_GroupListing_t) -> &'a GroupListing {
    &unsafe { inner_ref(listing) }.listing
}

/// Frees a `kafka_admin_GroupListing_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned group-listing handle.
pub(crate) unsafe fn destroy_group_listing_element(element: *mut c_void) {
    unsafe { kafka_admin_GroupListing_destroy(element as *mut kafka_admin_GroupListing_t) }
}

/// `new GroupListing(String groupId, Optional<GroupType> type, String
/// protocol, Optional<GroupState> groupState)`; `group_type` and
/// `group_state` are singletons or null for `Optional.empty()`. Owned, freed
/// with [`kafka_admin_GroupListing_destroy`].
///
/// # Safety
///
/// `group_id` and `protocol` must be valid NUL-terminated strings;
/// `group_type` and `group_state` null or singletons of their types.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_new(
    group_id: *const c_char,
    group_type: *const kafka_common_GroupType_t,
    protocol: *const c_char,
    group_state: *const kafka_common_GroupState_t,
) -> *mut kafka_admin_GroupListing_t {
    let group_type: Option<GroupType> = if group_type.is_null() {
        None
    } else {
        Some(unsafe { group_type::value_of(group_type) })
    };
    let group_state: Option<GroupState> = if group_state.is_null() {
        None
    } else {
        Some(unsafe { group_state::value_of(group_state) })
    };
    box_group_listing(GroupListing::new(
        unsafe { c_str_to_string(group_id) },
        group_type,
        unsafe { c_str_to_string(protocol) },
        group_state,
    ))
}

/// `groupId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid group-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_group_id(self_: *const kafka_admin_GroupListing_t) -> *const c_char {
    unsafe { inner_ref(self_) }.group_id_c.as_ptr()
}

/// `type()`: the `kafka_common_GroupType_t` singleton (never freed), or null
/// for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid group-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_type(
    self_: *const kafka_admin_GroupListing_t,
) -> *const kafka_common_GroupType_t {
    unsafe { group_listing_ref(self_) }
        .r#type()
        .map_or(ptr::null(), group_type::singleton)
}

/// `protocol()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid group-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_protocol(self_: *const kafka_admin_GroupListing_t) -> *const c_char {
    unsafe { inner_ref(self_) }.protocol_c.as_ptr()
}

/// `groupState()`: the `kafka_common_GroupState_t` singleton (never freed),
/// or null for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid group-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_group_state(
    self_: *const kafka_admin_GroupListing_t,
) -> *const kafka_common_GroupState_t {
    unsafe { group_listing_ref(self_) }
        .group_state()
        .map_or(ptr::null(), group_state::singleton)
}

/// `isSimpleConsumerGroup()`: a classic group with an empty protocol.
///
/// # Safety
///
/// `self_` must be a valid group-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_is_simple_consumer_group(
    self_: *const kafka_admin_GroupListing_t,
) -> i8 {
    i8::from(unsafe { group_listing_ref(self_) }.is_simple_consumer_group())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid group-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_to_string(self_: *const kafka_admin_GroupListing_t) -> *mut c_char {
    into_c_string(&unsafe { group_listing_ref(self_) }.to_string())
}

/// Frees an owned group-listing handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_GroupListing_destroy(self_: *mut kafka_admin_GroupListing_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut GroupListingInner) });
    }
}

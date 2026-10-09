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

//! `kafka_admin_TopicListing_t`: `org.apache.kafka.clients.admin.TopicListing`
//! (CLAUDE.md §4).

use std::ffi::{CString, c_char, c_void};

use crate::admin::TopicListing;
use crate::ffi::common::uuid::{box_uuid, kafka_common_Uuid_t, uuid_of};
use crate::ffi::util::{c_str_to_string, into_c_string, owned_c_string};

/// Opaque handle to a [`TopicListing`].
#[repr(C)]
pub struct kafka_admin_TopicListing_t {
    _private: [u8; 0],
}

/// What a [`kafka_admin_TopicListing_t`] points at: the value plus the
/// NUL-terminated name its getter borrows out.
pub(crate) struct TopicListingInner {
    listing: TopicListing,
    name_c: CString,
}

impl TopicListingInner {
    fn new(listing: TopicListing) -> Self {
        let name_c = owned_c_string(listing.name());
        Self { listing, name_c }
    }
}

unsafe fn inner_ref<'a>(listing: *const kafka_admin_TopicListing_t) -> &'a TopicListingInner {
    unsafe { &*(listing as *const TopicListingInner) }
}

/// Hands `listing` to C as an owned handle, freed with
/// [`kafka_admin_TopicListing_destroy`].
pub(crate) fn box_topic_listing(listing: TopicListing) -> *mut kafka_admin_TopicListing_t {
    Box::into_raw(Box::new(TopicListingInner::new(listing))) as *mut kafka_admin_TopicListing_t
}

/// The value behind a handle.
///
/// # Safety
///
/// `listing` must be a live topic-listing handle.
pub(crate) unsafe fn topic_listing_ref<'a>(listing: *const kafka_admin_TopicListing_t) -> &'a TopicListing {
    &unsafe { inner_ref(listing) }.listing
}

/// Frees a `kafka_admin_TopicListing_t *` element of an owned container.
///
/// # Safety
///
/// `element` must be an owned topic-listing handle.
pub(crate) unsafe fn destroy_topic_listing_element(element: *mut c_void) {
    unsafe { kafka_admin_TopicListing_destroy(element as *mut kafka_admin_TopicListing_t) }
}

/// `new TopicListing(String name, Uuid topicId, boolean internal)`; the uuid
/// is copied, the caller keeps its handle. Owned, freed with
/// [`kafka_admin_TopicListing_destroy`].
///
/// # Safety
///
/// `name` must be a valid NUL-terminated string and `topic_id` a valid uuid
/// handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_new(
    name: *const c_char,
    topic_id: *const kafka_common_Uuid_t,
    internal: i8,
) -> *mut kafka_admin_TopicListing_t {
    box_topic_listing(TopicListing::new(
        unsafe { c_str_to_string(name) },
        unsafe { uuid_of(topic_id) },
        internal != 0,
    ))
}

/// `name()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid topic-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_name(self_: *const kafka_admin_TopicListing_t) -> *const c_char {
    unsafe { inner_ref(self_) }.name_c.as_ptr()
}

/// `topicId()`: an owned copy, freed with `kafka_common_Uuid_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_topic_id(
    self_: *const kafka_admin_TopicListing_t,
) -> *mut kafka_common_Uuid_t {
    box_uuid(unsafe { topic_listing_ref(self_) }.topic_id())
}

/// `isInternal()`.
///
/// # Safety
///
/// `self_` must be a valid topic-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_is_internal(self_: *const kafka_admin_TopicListing_t) -> i8 {
    i8::from(unsafe { topic_listing_ref(self_) }.is_internal())
}

/// `toString()`, as an owned string freed with `kafka_string_destroy`.
///
/// # Safety
///
/// `self_` must be a valid topic-listing handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_to_string(self_: *const kafka_admin_TopicListing_t) -> *mut c_char {
    into_c_string(&unsafe { topic_listing_ref(self_) }.to_string())
}

/// Frees an owned topic-listing handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_admin_TopicListing_destroy(self_: *mut kafka_admin_TopicListing_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut TopicListingInner) });
    }
}

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

//! The data holders nested in
//! `org.apache.kafka.clients.consumer.ConsumerPartitionAssignor`:
//! `kafka_consumer_ConsumerPartitionAssignor_Subscription_t` and
//! `kafka_consumer_ConsumerPartitionAssignor_Assignment_t` (CLAUDE.md §4,
//! "Nested types"), with the Rust-only `SubscriptionOptions` /
//! `SubscriptionOptionsBuilder` standing for the `Subscription` constructor
//! overloads with more than three parameters (CLAUDE.md §2). The assignor
//! trait itself is out of scope (consumer-threading.md §20).
//!
//! `userData` crosses as `kafka_Bytes_t` by value: an input is copied during
//! the call, an output is borrowed from the handle; `data == NULL` is Java's
//! `null`.

use std::ffi::{CString, c_char};
use std::sync::Mutex;

use crate::common::Error;
use crate::consumer::consumer_partition_assignor::{
    Assignment, Subscription, SubscriptionOptions, SubscriptionOptionsBuilder,
};
use crate::ffi::common::topic_partition::{list_topic_partitions, topic_partition_list};
use crate::ffi::common::{box_error, kafka_common_Error_t};
use crate::ffi::util::{box_string_list, c_str_to_option, kafka_Bytes_t, kafka_List_t, list_strings, owned_c_string};

/// Opaque handle to a [`Subscription`].
#[repr(C)]
pub struct kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    _private: [u8; 0],
}

/// Opaque handle to an [`Assignment`].
#[repr(C)]
pub struct kafka_consumer_ConsumerPartitionAssignor_Assignment_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`SubscriptionOptions`].
#[repr(C)]
// the Options struct standing for the Subscription constructor overloads with more than three parameters (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct kafka_consumer_SubscriptionOptions_t {
    _private: [u8; 0],
}

/// Opaque handle to a [`SubscriptionOptionsBuilder`].
#[repr(C)]
// the builder of the Options struct standing for the Subscription constructor overloads (CLAUDE.md §2)
#[doc(alias = "rust-only")]
pub struct kafka_consumer_SubscriptionOptionsBuilder_t {
    _private: [u8; 0],
}

/// What a subscription handle points at: the value and the NUL-terminated
/// copies of its optional strings; `set_group_instance_id` replaces the
/// cached copy.
struct SubscriptionInner {
    sub: Subscription,
    rack_id_c: Option<CString>,
    group_instance_id_c: Mutex<Option<CString>>,
}

impl SubscriptionInner {
    fn boxed(sub: Subscription) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
        let rack_id_c = sub.rack_id().map(owned_c_string);
        let group_instance_id_c = Mutex::new(sub.group_instance_id().map(owned_c_string));
        Box::into_raw(Box::new(Self { sub, rack_id_c, group_instance_id_c }))
            as *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t
    }
}

unsafe fn subscription_inner<'a>(
    sub: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> &'a SubscriptionInner {
    unsafe { &*(sub as *const SubscriptionInner) }
}

/// The subscription behind a handle.
///
/// # Safety
///
/// `sub` must be a valid subscription handle.
pub(crate) unsafe fn subscription_ref<'a>(
    sub: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> &'a Subscription {
    &unsafe { subscription_inner(sub) }.sub
}

/// The assignment behind a handle.
///
/// # Safety
///
/// `assignment` must be a valid assignment handle.
pub(crate) unsafe fn assignment_ref<'a>(
    assignment: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> &'a Assignment {
    unsafe { &*(assignment as *const Assignment) }
}

/// `userData` as a Rust value, copied during the call.
///
/// # Safety
///
/// `user_data` must be null or point at `len` readable bytes.
unsafe fn user_data_of(user_data: kafka_Bytes_t) -> Option<Vec<u8>> {
    unsafe { user_data.as_slice() }.map(<[u8]>::to_vec)
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// `new Subscription(List<String> topics)`: an owned handle freed with
/// [`kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy`];
/// `topics` is a list of `char *`, copied during the call.
///
/// # Safety
///
/// `topics` must be null or a valid list of strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_new(
    topics: *const kafka_List_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    SubscriptionInner::boxed(Subscription::new(unsafe { list_strings(topics) }))
}

/// `new Subscription(List<String> topics, ByteBuffer userData)`.
///
/// # Safety
///
/// `topics` must be null or a valid list of strings; `user_data` null or
/// readable for its length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data(
    topics: *const kafka_List_t,
    user_data: kafka_Bytes_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    SubscriptionInner::boxed(Subscription::with_user_data(unsafe { list_strings(topics) }, unsafe {
        user_data_of(user_data)
    }))
}

/// `new Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions)`;
/// `owned_partitions` is a list of `kafka_common_TopicPartition_t *`,
/// copied during the call.
///
/// # Safety
///
/// `topics` and `owned_partitions` must be null or valid lists of the
/// documented element types; `user_data` null or readable for its length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data_owned_partitions(
    topics: *const kafka_List_t,
    user_data: kafka_Bytes_t,
    owned_partitions: *const kafka_List_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    SubscriptionInner::boxed(Subscription::with_user_data_owned_partitions(
        unsafe { list_strings(topics) },
        unsafe { user_data_of(user_data) },
        unsafe { list_topic_partitions(owned_partitions) },
    ))
}

/// The constructor overloads with more than three parameters, through the
/// Rust-only options (CLAUDE.md §2); the options stay the caller's.
///
/// # Safety
///
/// `options` must be a valid options handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_with_options(
    options: *const kafka_consumer_SubscriptionOptions_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    SubscriptionInner::boxed(Subscription::with_options(
        unsafe { &*(options as *const SubscriptionOptions) }.clone(),
    ))
}

/// `topics()`: an owned list of owned `char *`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_topics(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *mut kafka_List_t {
    box_string_list(unsafe { subscription_ref(self_) }.topics())
}

/// `ownedPartitions()`: an owned list of owned `kafka_common_TopicPartition_t *`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_owned_partitions(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *mut kafka_List_t {
    topic_partition_list(unsafe { subscription_ref(self_) }.owned_partitions().iter().cloned())
}

/// `userData()`: borrowed from the handle, `data == NULL` for Java `null`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_user_data(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> kafka_Bytes_t {
    kafka_Bytes_t::from_option(unsafe { subscription_ref(self_) }.user_data())
}

/// `groupInstanceId()`: borrowed from the handle until the next
/// `set_group_instance_id`, `NULL` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_group_instance_id(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *const c_char {
    unsafe { subscription_inner(self_) }
        .group_instance_id_c
        .lock()
        .unwrap()
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
}

/// `setGroupInstanceId(Optional<String>)`: `NULL` is `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle; `group_instance_id` null or a valid
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_set_group_instance_id(
    self_: *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
    group_instance_id: *const c_char,
) {
    let inner = unsafe { &mut *(self_ as *mut SubscriptionInner) };
    let id = unsafe { c_str_to_option(group_instance_id) };
    *inner.group_instance_id_c.lock().unwrap() = id.as_deref().map(owned_c_string);
    inner.sub.set_group_instance_id(id);
}

/// `rackId()`: borrowed from the handle, `NULL` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_rack_id(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *const c_char {
    unsafe { subscription_inner(self_) }
        .rack_id_c
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
}

/// `generationId()`: the generation, or `-1` for `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_generation_id(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> i32 {
    unsafe { subscription_ref(self_) }.generation_id().unwrap_or(-1)
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy(
    self_: *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut SubscriptionInner)) };
    }
}

// ---------------------------------------------------------------------------
// Assignment
// ---------------------------------------------------------------------------

/// `new Assignment(List<TopicPartition> partitions)`: an owned handle freed
/// with [`kafka_consumer_ConsumerPartitionAssignor_Assignment_destroy`];
/// `partitions` is a list of `kafka_common_TopicPartition_t *`, copied
/// during the call.
///
/// # Safety
///
/// `partitions` must be null or a valid list of topic-partition handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_new(
    partitions: *const kafka_List_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t {
    Box::into_raw(Box::new(Assignment::new(unsafe { list_topic_partitions(partitions) })))
        as *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t
}

/// `new Assignment(List<TopicPartition> partitions, ByteBuffer userData)`.
///
/// # Safety
///
/// `partitions` must be null or a valid list of topic-partition handles;
/// `user_data` null or readable for its length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_with_user_data(
    partitions: *const kafka_List_t,
    user_data: kafka_Bytes_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t {
    Box::into_raw(Box::new(Assignment::with_user_data(
        unsafe { list_topic_partitions(partitions) },
        unsafe { user_data_of(user_data) },
    ))) as *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t
}

/// `partitions()`: an owned list of owned `kafka_common_TopicPartition_t *`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_partitions(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> *mut kafka_List_t {
    topic_partition_list(unsafe { assignment_ref(self_) }.partitions().iter().cloned())
}

/// `userData()`: borrowed from the handle, `data == NULL` for Java `null`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_user_data(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> kafka_Bytes_t {
    kafka_Bytes_t::from_option(unsafe { assignment_ref(self_) }.user_data())
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_destroy(
    self_: *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut Assignment)) };
    }
}

// ---------------------------------------------------------------------------
// SubscriptionOptions / SubscriptionOptionsBuilder
// ---------------------------------------------------------------------------

/// Applies a by-value fluent setter to the builder behind a handle; after
/// `build` consumed it a setter is a no-op.
unsafe fn with_builder(
    builder: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    f: impl FnOnce(SubscriptionOptionsBuilder) -> SubscriptionOptionsBuilder,
) {
    let slot = unsafe { &mut *(builder as *mut Option<SubscriptionOptionsBuilder>) };
    if let Some(b) = slot.take() {
        *slot = Some(f(b));
    }
}

/// `SubscriptionOptionsBuilder::new()`: an owned handle freed with
/// [`kafka_consumer_SubscriptionOptionsBuilder_destroy`].
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_new() -> *mut kafka_consumer_SubscriptionOptionsBuilder_t {
    Box::into_raw(Box::new(Some(SubscriptionOptionsBuilder::new()))) as *mut kafka_consumer_SubscriptionOptionsBuilder_t
}

/// `set_topics` (mandatory): a list of `char *`, copied during the call.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `topics` null or a valid list
/// of strings.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_topics(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    topics: *const kafka_List_t,
) {
    let topics = unsafe { list_strings(topics) };
    unsafe { with_builder(self_, |b| b.set_topics(topics)) }
}

/// `set_user_data`: copied during the call, `data == NULL` for Java `null`.
///
/// # Safety
///
/// `self_` must be a valid builder handle; `user_data` null or readable for
/// its length.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_user_data(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    user_data: kafka_Bytes_t,
) {
    let user_data = unsafe { user_data_of(user_data) };
    unsafe { with_builder(self_, |b| b.set_user_data(user_data)) }
}

/// `set_owned_partitions`: a list of `kafka_common_TopicPartition_t *`,
/// copied during the call.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `owned_partitions` null or a
/// valid list of topic-partition handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_owned_partitions(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    owned_partitions: *const kafka_List_t,
) {
    let owned_partitions = unsafe { list_topic_partitions(owned_partitions) };
    unsafe { with_builder(self_, |b| b.set_owned_partitions(owned_partitions)) }
}

/// `set_generation_id`.
///
/// # Safety
///
/// `self_` must be a valid builder handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_generation_id(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    generation_id: i32,
) {
    unsafe { with_builder(self_, |b| b.set_generation_id(generation_id)) }
}

/// `set_rack_id`: `NULL` is `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid builder handle; `rack_id` null or a valid
/// NUL-terminated string.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_rack_id(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    rack_id: *const c_char,
) {
    let rack_id = unsafe { c_str_to_option(rack_id) };
    unsafe { with_builder(self_, |b| b.set_rack_id(rack_id)) }
}

/// `build()`: validates the mandatory fields and delivers the options,
/// owned by the caller ([`kafka_consumer_SubscriptionOptions_destroy`]), or
/// returns the `IllegalArgumentError`. The builder is consumed either way.
///
/// # Safety
///
/// `self_` must be a valid builder handle and `out_build` a valid pointer.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_build(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    out_build: *mut *mut kafka_consumer_SubscriptionOptions_t,
) -> *mut kafka_common_Error_t {
    let slot = unsafe { &mut *(self_ as *mut Option<SubscriptionOptionsBuilder>) };
    let Some(builder) = slot.take() else {
        return box_error(Error::local_illegal_state("SubscriptionOptionsBuilder already built"));
    };
    match builder.build() {
        Ok(options) => {
            unsafe { *out_build = Box::into_raw(Box::new(options)) as *mut kafka_consumer_SubscriptionOptions_t };
            std::ptr::null_mut()
        },
        Err(error) => box_error(error),
    }
}

/// Frees a builder handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid builder handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_destroy(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut Option<SubscriptionOptionsBuilder>)) };
    }
}

/// Frees an options handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a valid options handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptions_destroy(self_: *mut kafka_consumer_SubscriptionOptions_t) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut SubscriptionOptions)) };
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;
    use crate::ffi::util::{kafka_List_destroy, kafka_List_size};

    fn topics() -> *mut kafka_List_t {
        box_string_list(["a", "b"])
    }

    #[test]
    fn subscription_round_trip() {
        let topics = topics();
        let sub = unsafe {
            kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data(
                topics,
                kafka_Bytes_t::from_slice(b"ud"),
            )
        };
        unsafe {
            kafka_List_destroy(topics);
            let got = kafka_consumer_ConsumerPartitionAssignor_Subscription_topics(sub);
            assert_eq!(kafka_List_size(got), 2);
            kafka_List_destroy(got);
            assert_eq!(
                kafka_consumer_ConsumerPartitionAssignor_Subscription_user_data(sub).as_slice(),
                Some(&b"ud"[..])
            );
            assert!(kafka_consumer_ConsumerPartitionAssignor_Subscription_group_instance_id(sub).is_null());
            kafka_consumer_ConsumerPartitionAssignor_Subscription_set_group_instance_id(sub, c"gi".as_ptr());
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerPartitionAssignor_Subscription_group_instance_id(sub))
                    .to_str()
                    .unwrap(),
                "gi"
            );
            assert_eq!(subscription_ref(sub).group_instance_id(), Some("gi"));
            assert_eq!(kafka_consumer_ConsumerPartitionAssignor_Subscription_generation_id(sub), -1);
            assert!(kafka_consumer_ConsumerPartitionAssignor_Subscription_rack_id(sub).is_null());
            kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy(sub);
        }
    }

    #[test]
    fn builder_and_assignment() {
        let builder = kafka_consumer_SubscriptionOptionsBuilder_new();
        let topics = topics();
        let mut options = std::ptr::null_mut();
        unsafe {
            kafka_consumer_SubscriptionOptionsBuilder_set_topics(builder, topics);
            kafka_consumer_SubscriptionOptionsBuilder_set_generation_id(builder, 9);
            kafka_consumer_SubscriptionOptionsBuilder_set_rack_id(builder, c"r1".as_ptr());
            kafka_consumer_SubscriptionOptionsBuilder_set_user_data(builder, kafka_Bytes_t::NULL);
            assert!(kafka_consumer_SubscriptionOptionsBuilder_build(builder, &mut options).is_null());
            kafka_consumer_SubscriptionOptionsBuilder_destroy(builder);
            kafka_List_destroy(topics);
            let sub = kafka_consumer_ConsumerPartitionAssignor_Subscription_with_options(options);
            kafka_consumer_SubscriptionOptions_destroy(options);
            assert_eq!(kafka_consumer_ConsumerPartitionAssignor_Subscription_generation_id(sub), 9);
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerPartitionAssignor_Subscription_rack_id(sub))
                    .to_str()
                    .unwrap(),
                "r1"
            );
            assert!(
                kafka_consumer_ConsumerPartitionAssignor_Subscription_user_data(sub)
                    .data
                    .is_null()
            );
            kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy(sub);

            let assignment = kafka_consumer_ConsumerPartitionAssignor_Assignment_new(std::ptr::null());
            let parts = kafka_consumer_ConsumerPartitionAssignor_Assignment_partitions(assignment);
            assert_eq!(kafka_List_size(parts), 0);
            kafka_List_destroy(parts);
            assert!(
                kafka_consumer_ConsumerPartitionAssignor_Assignment_user_data(assignment)
                    .data
                    .is_null()
            );
            kafka_consumer_ConsumerPartitionAssignor_Assignment_destroy(assignment);
        }
    }
}

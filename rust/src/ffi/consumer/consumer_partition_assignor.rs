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
use crate::ffi::common::fail_with;
use crate::ffi::common::kafka_common_Error_t;
use crate::ffi::common::topic_partition::{list_topic_partitions, topic_partition_list};
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    box_string_list, c_str_to_option, into_c_string, kafka_Bytes_t, kafka_List_t, list_strings, owned_c_string,
};

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

/// What a subscription handle points at.
///
/// # Safety
///
/// `sub` must be a valid subscription handle, kept alive for `'a`, during
/// which nothing takes a `&mut` to it: only
/// [`kafka_consumer_ConsumerPartitionAssignor_Subscription_set_group_instance_id`]
/// takes one, and it requires that no other call uses the handle at the same
/// time.
unsafe fn subscription_inner<'a>(
    sub: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> &'a SubscriptionInner {
    // SAFETY: Per this function's `# Safety`, `sub` is a `Box<SubscriptionInner>` from
    // `SubscriptionInner::boxed`, the only constructor, kept alive for `'a`, during which nothing
    // takes a `&mut` to it, so the shared reference is valid for `'a`.
    unsafe { &*(sub as *const SubscriptionInner) }
}

/// The subscription behind a handle.
///
/// # Safety
///
/// `sub` must be a valid subscription handle kept alive for `'a`, during
/// which nothing takes a `&mut` to it (see `subscription_inner`).
pub(crate) unsafe fn subscription_ref<'a>(
    sub: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> &'a Subscription {
    // SAFETY: Per this function's `# Safety`, `sub` is a valid subscription handle kept alive for
    // `'a`, during which nothing takes a `&mut` to it, as `subscription_inner` requires.
    &unsafe { subscription_inner(sub) }.sub
}

/// The assignment behind a handle.
///
/// # Safety
///
/// `assignment` must be a valid assignment handle kept alive for `'a`.
pub(crate) unsafe fn assignment_ref<'a>(
    assignment: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> &'a Assignment {
    // SAFETY: Per this function's `# Safety`, `assignment` is a `Box<Assignment>` from one of the
    // `Assignment` constructors here, kept alive for `'a`, and no handle to an assignment is ever
    // mutated, so the shared reference is valid for `'a`.
    unsafe { &*(assignment as *const Assignment) }
}

/// `userData` as a Rust value, copied during the call.
///
/// # Safety
///
/// `user_data`'s `data` must be null or point to `len` readable bytes not
/// written during the call.
unsafe fn user_data_of(user_data: kafka_Bytes_t) -> Option<Vec<u8>> {
    // SAFETY: Per this function's `# Safety`, `user_data`'s `data` is null or points to `len`
    // readable bytes not written during the call; the bytes are copied before return, so the
    // slice's `'a` is this call, as `as_slice` requires.
    unsafe { user_data.as_slice() }.map(<[u8]>::to_vec)
}

// ---------------------------------------------------------------------------
// Subscription
// ---------------------------------------------------------------------------

/// `new Subscription(List<String> topics)`; `topics` is a list of `char *`.
///
/// # Parameters
///
/// - `topics` - The list of topics to subscribe to
///
/// # Safety
///
/// `topics` must be null or a valid list of strings.
///
/// `topics` is copied during the call. The returned handle is owned by the caller, freed with
/// [`kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_new(
    topics: *const kafka_List_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list whose elements are
    // NUL-terminated strings, as `list_strings` requires; the strings are copied, so the borrow
    // lasts for this call only.
    SubscriptionInner::boxed(Subscription::new(unsafe { list_strings(topics) }))
}

/// `new Subscription(List<String> topics, ByteBuffer userData)`.
///
/// # Parameters
///
/// - `topics` - The list of topics to subscribe to
/// - `user_data` - Nullable user data to include in the subscription
///
/// # Safety
///
/// `topics` must be null or a valid list of strings; `user_data`'s `data`
/// null or pointing to `len` readable bytes not written during the call.
///
/// `topics` and `user_data` are copied during the call. The returned handle is owned by the caller,
/// freed with `kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data(
    topics: *const kafka_List_t,
    user_data: kafka_Bytes_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list whose elements are
    // NUL-terminated strings, as `list_strings` requires; the strings are copied, so the borrow
    // lasts for this call only. Per this function's `# Safety`, `user_data`'s `data` is null or
    // points to `len` readable bytes not written during the call, as `user_data_of` requires.
    SubscriptionInner::boxed(Subscription::with_user_data(unsafe { list_strings(topics) }, unsafe {
        user_data_of(user_data)
    }))
}

/// `new Subscription(List<String> topics, ByteBuffer userData, List<TopicPartition> ownedPartitions)`;
/// `owned_partitions` is a list of `kafka_common_TopicPartition_t *`,
/// copied during the call.
///
/// # Parameters
///
/// - `topics` - The list of topics to subscribe to
/// - `user_data` - Nullable user data to include in the subscription
/// - `owned_partitions` - The partitions currently owned by this consumer
///
/// # Safety
///
/// `topics` and `owned_partitions` must be null or valid lists of the
/// documented element types; `user_data`'s `data` null or pointing to `len`
/// readable bytes not written during the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data_owned_partitions(
    topics: *const kafka_List_t,
    user_data: kafka_Bytes_t,
    owned_partitions: *const kafka_List_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    SubscriptionInner::boxed(Subscription::with_user_data_owned_partitions(
        // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list whose elements
        // are NUL-terminated strings, as `list_strings` requires; the strings are copied, so the
        // borrow lasts for this call only.
        unsafe { list_strings(topics) },
        // SAFETY: Per this function's `# Safety`, `user_data`'s `data` is null or points to `len`
        // readable bytes not written during the call, as `user_data_of` requires.
        unsafe { user_data_of(user_data) },
        // SAFETY: Per this function's `# Safety`, `owned_partitions` is null or a valid list whose
        // elements are topic-partition handles, as `list_topic_partitions` requires; the values are
        // cloned, so the borrow lasts for this call only.
        unsafe { list_topic_partitions(owned_partitions) },
    ))
}

/// The constructor overloads with more than three parameters, through the
/// Rust-only options.
///
/// # Parameters
///
/// - `options` - the constructor's parameters, which stay the caller's
///
/// # Safety
///
/// `options` must be a valid options handle.
///
/// The returned handle is owned by the caller, freed with
/// `kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_with_options(
    options: *const kafka_consumer_SubscriptionOptions_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t {
    SubscriptionInner::boxed(Subscription::with_options(
        // SAFETY: Per this function's `# Safety`, `options` is a valid options handle, a
        // `Box<SubscriptionOptions>` from `kafka_consumer_SubscriptionOptionsBuilder_build`, which
        // nothing mutates; the value is cloned, so the borrow lasts for this call only.
        unsafe { &*(options as *const SubscriptionOptions) }.clone(),
    ))
}

/// `topics()`: a list of `char *`.
///
/// # Returns
///
/// The list of topics
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned list is owned by the caller, freed with `kafka_List_destroy`, which also frees its
/// elements.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_topics(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `subscription_ref` requires.
    box_string_list(unsafe { subscription_ref(self_) }.topics())
}

/// `ownedPartitions()`: a list of `kafka_common_TopicPartition_t *`.
///
/// # Returns
///
/// The list of owned partitions
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned list is owned by the caller, freed with `kafka_List_destroy`, which also frees its
/// elements.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_owned_partitions(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `subscription_ref` requires.
    topic_partition_list(unsafe { subscription_ref(self_) }.owned_partitions().iter().cloned())
}

/// `userData()`, `data == NULL` for Java `null`.
///
/// # Returns
///
/// The user data, or `NULL` if none was provided
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned bytes are borrowed from the handle: they stay valid until the handle is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_user_data(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `subscription_ref` requires. The returned view borrows the
    // handle's bytes, which the rustdoc tells the caller.
    kafka_Bytes_t::from_option(unsafe { subscription_ref(self_) }.user_data())
}

/// `groupInstanceId()`, `NULL` for `Optional.empty()`.
///
/// # Returns
///
/// The group instance ID, or empty if this is a dynamic member
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned pointer is borrowed from the handle: it stays valid until the handle is destroyed
/// or its next `set_group_instance_id`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_group_instance_id(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference; only `set_group_instance_id` takes a `&mut` to it, and it
    // requires that no other call uses the handle at the same time, as `subscription_inner`
    // requires.
    unsafe { subscription_inner(self_) }
        .group_instance_id_c
        .lock()
        .unwrap()
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
}

/// `setGroupInstanceId(Optional<String>)`: `NULL` is `Optional.empty()`.
///
/// # Parameters
///
/// - `group_instance_id` - The group instance ID
///
/// # Safety
///
/// `self_` must be a valid handle that no other call uses at the same time;
/// `group_instance_id` null or a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_set_group_instance_id(
    self_: *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
    group_instance_id: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, a `Box<SubscriptionInner>`
    // from `SubscriptionInner::boxed`, that no other call uses at the same time, so the exclusive
    // reference, used only within this call, aliases nothing.
    let inner = unsafe { &mut *(self_ as *mut SubscriptionInner) };
    // SAFETY: Per this function's `# Safety`, `group_instance_id` is null or a valid NUL-terminated
    // string, as `c_str_to_option` requires.
    let id = unsafe { c_str_to_option(group_instance_id) };
    *inner.group_instance_id_c.lock().unwrap() = id.as_deref().map(owned_c_string);
    inner.sub.set_group_instance_id(id);
}

/// `rackId()`, `NULL` for `Optional.empty()`.
///
/// # Returns
///
/// The rack ID, or empty if not provided
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned pointer is borrowed from the handle: it stays valid until the handle is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_rack_id(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `subscription_inner` requires.
    unsafe { subscription_inner(self_) }
        .rack_id_c
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
}

/// `generationId()`.
///
/// # Returns
///
/// The generation ID, or `-1` if not provided (Java's `Optional.empty()`).
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard(fallback = -2)]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_generation_id(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `subscription_ref` requires.
    // A caught Rust panic returns -2, which is no generation: Java's `Subscription` stores a
    // negative one as `Optional.empty()`.
    unsafe { subscription_ref(self_) }.generation_id().unwrap_or(-1)
}

/// Java `toString()`.
///
/// # Returns
///
/// The string form.
///
/// # Safety
///
/// `self_` must be a valid subscription handle, used by no setter during the call.
///
/// The returned string is owned by the caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_to_string(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid subscription handle, as `subscription_ref` requires; the string is built during the call.
    into_c_string(&unsafe { subscription_ref(self_) }.to_string())
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, not used
/// afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Subscription_destroy(
    self_: *mut kafka_consumer_ConsumerPartitionAssignor_Subscription_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. a `Box<SubscriptionInner>` from
        // `SubscriptionInner::boxed`, so taking it back and dropping it frees it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut SubscriptionInner)) };
    }
}

// ---------------------------------------------------------------------------
// Assignment
// ---------------------------------------------------------------------------

/// `new Assignment(List<TopicPartition> partitions)`; `partitions` is a list
/// of `kafka_common_TopicPartition_t *`.
///
/// # Parameters
///
/// - `partitions` - The list of partitions assigned to the consumer
///
/// # Safety
///
/// `partitions` must be null or a valid list of topic-partition handles.
///
/// `partitions` is copied during the call. The returned handle is owned by the caller, freed with
/// [`kafka_consumer_ConsumerPartitionAssignor_Assignment_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_new(
    partitions: *const kafka_List_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t {
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list whose elements
    // are topic-partition handles, as `list_topic_partitions` requires; the values are cloned, so
    // the borrow lasts for this call only.
    Box::into_raw(Box::new(Assignment::new(unsafe { list_topic_partitions(partitions) })))
        as *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t
}

/// `new Assignment(List<TopicPartition> partitions, ByteBuffer userData)`.
///
/// # Parameters
///
/// - `partitions` - The list of partitions assigned to the consumer
/// - `user_data` - Nullable user data to include in the assignment
///
/// # Safety
///
/// `partitions` must be null or a valid list of topic-partition handles;
/// `user_data`'s `data` null or pointing to `len` readable bytes not written
/// during the call.
///
/// `partitions` and `user_data` are copied during the call. The returned handle is owned by the
/// caller, freed with `kafka_consumer_ConsumerPartitionAssignor_Assignment_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_with_user_data(
    partitions: *const kafka_List_t,
    user_data: kafka_Bytes_t,
) -> *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t {
    Box::into_raw(Box::new(Assignment::with_user_data(
        // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list whose
        // elements are topic-partition handles, as `list_topic_partitions` requires; the values are
        // cloned, so the borrow lasts for this call only.
        unsafe { list_topic_partitions(partitions) },
        // SAFETY: Per this function's `# Safety`, `user_data`'s `data` is null or points to `len`
        // readable bytes not written during the call, as `user_data_of` requires.
        unsafe { user_data_of(user_data) },
    ))) as *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t
}

/// `partitions()`: a list of `kafka_common_TopicPartition_t *`.
///
/// # Returns
///
/// The list of partitions
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned list is owned by the caller, freed with `kafka_List_destroy`, which also frees its
/// elements.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_partitions(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `assignment_ref` requires.
    topic_partition_list(unsafe { assignment_ref(self_) }.partitions().iter().cloned())
}

/// `userData()`, `data == NULL` for Java `null`.
///
/// # Returns
///
/// The user data, or `NULL` if none was provided
///
/// # Safety
///
/// `self_` must be a valid handle.
///
/// The returned bytes are borrowed from the handle: they stay valid until the handle is destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_user_data(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `assignment_ref` requires. The returned view borrows the
    // handle's bytes, which the rustdoc tells the caller.
    kafka_Bytes_t::from_option(unsafe { assignment_ref(self_) }.user_data())
}

/// Java `toString()`.
///
/// # Returns
///
/// The string form.
///
/// # Safety
///
/// `self_` must be a valid assignment handle.
///
/// The returned string is owned by the caller, freed with `kafka_string_destroy`.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_to_string(
    self_: *const kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) -> *mut c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid assignment handle, as `assignment_ref` requires; the string is built during the call.
    into_c_string(&unsafe { assignment_ref(self_) }.to_string())
}

/// Frees a handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, not used
/// afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerPartitionAssignor_Assignment_destroy(
    self_: *mut kafka_consumer_ConsumerPartitionAssignor_Assignment_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. a `Box<Assignment>` from one of the `Assignment`
        // constructors here, so taking it back and dropping it frees it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut Assignment)) };
    }
}

// ---------------------------------------------------------------------------
// SubscriptionOptions / SubscriptionOptionsBuilder
// ---------------------------------------------------------------------------

/// Applies a by-value fluent setter to the builder behind a handle; after
/// `build` consumed it a setter is a no-op.
///
/// # Safety
///
/// `builder` must be a valid builder handle that no other call uses at the
/// same time.
unsafe fn with_builder(
    builder: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    f: impl FnOnce(SubscriptionOptionsBuilder) -> SubscriptionOptionsBuilder,
) {
    // SAFETY: Per this function's `# Safety`, `builder` is the `Box::into_raw` of an
    // `Option<SubscriptionOptionsBuilder>` in `kafka_consumer_SubscriptionOptionsBuilder_new`, the
    // only constructor, that no other call uses at the same time, so the exclusive reference, used
    // only within this call, aliases nothing.
    let slot = unsafe { &mut *(builder as *mut Option<SubscriptionOptionsBuilder>) };
    if let Some(b) = slot.take() {
        *slot = Some(f(b));
    }
}

/// Creates a builder of the `Subscription` constructor's parameters.
///
/// # Returns
///
/// The builder handle.
///
/// # Safety
///
/// The returned handle is owned by the caller, freed with
/// [`kafka_consumer_SubscriptionOptionsBuilder_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_new() -> *mut kafka_consumer_SubscriptionOptionsBuilder_t {
    Box::into_raw(Box::new(Some(SubscriptionOptionsBuilder::new()))) as *mut kafka_consumer_SubscriptionOptionsBuilder_t
}

/// Sets the subscribed topics.
///
/// # Parameters
///
/// - `topics` - the subscribed topics (mandatory), a list of `char *` copied during the call
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time, and `topics` null or a valid list of strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_topics(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    topics: *const kafka_List_t,
) {
    // SAFETY: Per this function's `# Safety`, `topics` is null or a valid list whose elements are
    // NUL-terminated strings, as `list_strings` requires; the strings are copied, so the borrow
    // lasts for this call only.
    let topics = unsafe { list_strings(topics) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `with_builder` requires.
    unsafe { with_builder(self_, |b| b.set_topics(topics)) }
}

/// Sets the user data.
///
/// # Parameters
///
/// - `user_data` - the user data, copied during the call; `data == NULL` for Java `null`
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time; `user_data`'s `data` null or pointing to `len` readable bytes
/// not written during the call.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_user_data(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    user_data: kafka_Bytes_t,
) {
    // SAFETY: Per this function's `# Safety`, `user_data`'s `data` is null or points to `len`
    // readable bytes not written during the call, as `user_data_of` requires.
    let user_data = unsafe { user_data_of(user_data) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `with_builder` requires.
    unsafe { with_builder(self_, |b| b.set_user_data(user_data)) }
}

/// Sets the owned partitions.
///
/// # Parameters
///
/// - `owned_partitions` - the owned partitions, a list of `kafka_common_TopicPartition_t *` copied during the call
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time, and `owned_partitions` null or a valid list of
/// topic-partition handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_owned_partitions(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    owned_partitions: *const kafka_List_t,
) {
    // SAFETY: Per this function's `# Safety`, `owned_partitions` is null or a valid list whose
    // elements are topic-partition handles, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let owned_partitions = unsafe { list_topic_partitions(owned_partitions) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `with_builder` requires.
    unsafe { with_builder(self_, |b| b.set_owned_partitions(owned_partitions)) }
}

/// Sets the generation ID.
///
/// # Parameters
///
/// - `generation_id` - the generation ID
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_generation_id(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    generation_id: i32,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `with_builder` requires.
    unsafe { with_builder(self_, |b| b.set_generation_id(generation_id)) }
}

/// Sets the rack ID.
///
/// # Parameters
///
/// - `rack_id` - the rack ID, `NULL` for `Optional.empty()`
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the
/// same time; `rack_id` null or a valid NUL-terminated string.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_set_rack_id(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    rack_id: *const c_char,
) {
    // SAFETY: Per this function's `# Safety`, `rack_id` is null or a valid NUL-terminated string,
    // as `c_str_to_option` requires.
    let rack_id = unsafe { c_str_to_option(rack_id) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid builder handle that no other call
    // uses at the same time, as `with_builder` requires.
    unsafe { with_builder(self_, |b| b.set_rack_id(rack_id)) }
}

/// Validates the mandatory fields and builds the options. The builder is consumed either way.
///
/// # Returns
///
/// The options, or `NULL` on failure.
///
/// # Errors
///
/// - `kafka_common_Error_is_local_illegal_argument_error` if a mandatory field is not set
///
/// # Safety
///
/// `self_` must be a valid builder handle that no other call uses at the same
/// time, and `out_err` a valid slot pointing at null.
///
/// The returned options are owned by the caller, freed with
/// [`kafka_consumer_SubscriptionOptions_destroy`].
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_build(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_consumer_SubscriptionOptions_t {
    // SAFETY: Per this function's `# Safety`, `self_` is the `Box::into_raw` of an
    // `Option<SubscriptionOptionsBuilder>` in `kafka_consumer_SubscriptionOptionsBuilder_new`, the
    // only constructor, that no other call uses at the same time, so the exclusive reference, used
    // only within this call, aliases nothing.
    let slot = unsafe { &mut *(self_ as *mut Option<SubscriptionOptionsBuilder>) };
    let Some(builder) = slot.take() else {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        return unsafe { fail_with(out_err, Error::local_illegal_state("SubscriptionOptionsBuilder already built")) };
    };
    match builder.build() {
        Ok(options) => Box::into_raw(Box::new(options)) as *mut kafka_consumer_SubscriptionOptions_t,
        // SAFETY: Per this function's `# Safety`, `out_err` is as `fail_with` requires.
        Err(error) => unsafe { fail_with(out_err, error) },
    }
}

/// Frees a builder handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned builder handle not yet destroyed, not
/// used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptionsBuilder_destroy(
    self_: *mut kafka_consumer_SubscriptionOptionsBuilder_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // builder handle not yet destroyed, i.e. a `Box<Option<SubscriptionOptionsBuilder>>` from
        // `kafka_consumer_SubscriptionOptionsBuilder_new`, so taking it back and dropping it frees
        // it exactly once.
        unsafe { drop(Box::from_raw(self_ as *mut Option<SubscriptionOptionsBuilder>)) };
    }
}

/// Frees an options handle; a null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or an owned options handle not yet destroyed, not
/// used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_SubscriptionOptions_destroy(self_: *mut kafka_consumer_SubscriptionOptions_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // options handle not yet destroyed, i.e. a `Box<SubscriptionOptions>` from
        // `kafka_consumer_SubscriptionOptionsBuilder_build`, so taking it back and dropping it
        // frees it exactly once.
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
        // SAFETY: Test code: `topics` is a live list of strings and the bytes are a static literal.
        let sub = unsafe {
            kafka_consumer_ConsumerPartitionAssignor_Subscription_with_user_data(
                topics,
                kafka_Bytes_t::from_slice(b"ud"),
            )
        };
        // SAFETY: Test code: every handle is live and owned by this test, used on this thread only,
        // and each is destroyed once, after its last use.
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
        let mut err = std::ptr::null_mut();
        // SAFETY: Test code: every handle is live and owned by this test, used on this thread only,
        // `err` is a valid slot pointing at null, and each handle is destroyed once, after its last
        // use.
        unsafe {
            kafka_consumer_SubscriptionOptionsBuilder_set_topics(builder, topics);
            kafka_consumer_SubscriptionOptionsBuilder_set_generation_id(builder, 9);
            kafka_consumer_SubscriptionOptionsBuilder_set_rack_id(builder, c"r1".as_ptr());
            kafka_consumer_SubscriptionOptionsBuilder_set_user_data(builder, kafka_Bytes_t::NULL);
            let options = kafka_consumer_SubscriptionOptionsBuilder_build(builder, &mut err);
            assert!(err.is_null());
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

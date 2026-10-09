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

//! `kafka_consumer_ConsumerGroupMetadata_t`:
//! `org.apache.kafka.clients.consumer.ConsumerGroupMetadata`.
//!
//! Java deprecated the class's constructors in 4.2 and turns it into an
//! interface in 5.0; Rust already has the trait, so C sees a rule 3
//! interface (CLAUDE.md §4): a handle usually comes from
//! `kafka_consumer_Consumer_group_metadata`, and
//! [`kafka_consumer_ConsumerGroupMetadata_new`] lets C implement it (a test
//! double for `sendOffsetsToTransaction`, say). The four getters are
//! invokers: on a consumer's handle they read the consumer's metadata, on a
//! C implementation they call its function pointers.
//!
//! The string getters return borrowed pointers. For a consumer's metadata
//! the handle caches NUL-terminated copies; for a C implementation they are
//! whatever the C function returned, so a C implementation keeps its
//! strings alive for as long as the Java getter's return value would be
//! referenced — until the handle is destroyed is the safe rule.

#![expect(non_camel_case_types)]

use std::ffi::{CStr, CString, c_char, c_void};
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::consumer::ConsumerGroupMetadata;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::util::owned_c_string;

/// Opaque handle to a [`ConsumerGroupMetadata`].
#[repr(C)]
pub struct kafka_consumer_ConsumerGroupMetadata_t {
    _private: [u8; 0],
}

/// `String groupId()` of a C implementation: a borrowed string.
pub type kafka_consumer_ConsumerGroupMetadata_group_id_fn_t = unsafe extern "C" fn(self_: *mut c_void) -> *const c_char;

/// `int generationId()` of a C implementation.
pub type kafka_consumer_ConsumerGroupMetadata_generation_id_fn_t = unsafe extern "C" fn(self_: *mut c_void) -> i32;

/// `String memberId()` of a C implementation: a borrowed string.
pub type kafka_consumer_ConsumerGroupMetadata_member_id_fn_t =
    unsafe extern "C" fn(self_: *mut c_void) -> *const c_char;

/// `Optional<String> groupInstanceId()` of a C implementation: a borrowed
/// string, or `NULL` for `Optional.empty()`.
pub type kafka_consumer_ConsumerGroupMetadata_group_instance_id_fn_t =
    unsafe extern "C" fn(self_: *mut c_void) -> *const c_char;

/// What the handle points at: the metadata and lazily built NUL-terminated
/// copies of its strings for the borrowed getters.
struct ConsumerGroupMetadataInner {
    meta: Arc<dyn ConsumerGroupMetadata>,
    group_id_c: OnceLock<CString>,
    member_id_c: OnceLock<CString>,
    group_instance_id_c: OnceLock<Option<CString>>,
}

/// Hands `meta` to C as an owned handle, freed with
/// [`kafka_consumer_ConsumerGroupMetadata_destroy`].
pub(crate) fn box_group_metadata(meta: Arc<dyn ConsumerGroupMetadata>) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    Box::into_raw(Box::new(ConsumerGroupMetadataInner {
        meta,
        group_id_c: OnceLock::new(),
        member_id_c: OnceLock::new(),
        group_instance_id_c: OnceLock::new(),
    })) as *mut kafka_consumer_ConsumerGroupMetadata_t
}

/// Borrows the [`ConsumerGroupMetadata`] inside a handle.
///
/// Shared with the producer FFI so
/// `kafka_producer_Producer_send_offsets_to_transaction` can take the very
/// handle a consumer produced, mirroring Java's
/// `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())`.
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle.
pub(crate) unsafe fn group_metadata_ref<'a>(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> &'a Arc<dyn ConsumerGroupMetadata> {
    &unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.meta
}

unsafe fn inner_ref<'a>(meta: *const kafka_consumer_ConsumerGroupMetadata_t) -> &'a ConsumerGroupMetadataInner {
    unsafe { &*(meta as *const ConsumerGroupMetadataInner) }
}

/// A C implementation of [`ConsumerGroupMetadata`] registered through
/// [`kafka_consumer_ConsumerGroupMetadata_new`].
struct CGroupMetadata {
    self_: SendPtr,
    group_id: kafka_consumer_ConsumerGroupMetadata_group_id_fn_t,
    generation_id: kafka_consumer_ConsumerGroupMetadata_generation_id_fn_t,
    member_id: kafka_consumer_ConsumerGroupMetadata_member_id_fn_t,
    group_instance_id: kafka_consumer_ConsumerGroupMetadata_group_instance_id_fn_t,
}

impl CGroupMetadata {
    /// The string a C getter returned, borrowed for as long as the
    /// implementation keeps it (see the module docs); a null or non-UTF-8
    /// result reads as `None`.
    unsafe fn string<'a>(ptr: *const c_char) -> Option<&'a str> {
        if ptr.is_null() {
            return None;
        }
        unsafe { CStr::from_ptr(ptr) }.to_str().ok()
    }
}

impl fmt::Debug for CGroupMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CGroupMetadata")
            .field("self", &self.self_.0)
            .finish_non_exhaustive()
    }
}

impl ConsumerGroupMetadata for CGroupMetadata {
    fn group_id(&self) -> &str {
        unsafe { Self::string((self.group_id)(self.self_.get())) }.unwrap_or("")
    }

    fn generation_id(&self) -> i32 {
        unsafe { (self.generation_id)(self.self_.get()) }
    }

    fn member_id(&self) -> &str {
        unsafe { Self::string((self.member_id)(self.self_.get())) }.unwrap_or("")
    }

    fn group_instance_id(&self) -> Option<&str> {
        unsafe { Self::string((self.group_instance_id)(self.self_.get())) }
    }
}

/// Registers a C implementation (CLAUDE.md §4 rule 3): the caller owns
/// `self_` and keeps it alive until the handle is destroyed. All four
/// methods are abstract in Java, so none of the pointers may be `NULL`.
///
/// # Safety
///
/// The function pointers must be valid for the lifetime of the handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_new(
    self_: *mut c_void,
    group_id: kafka_consumer_ConsumerGroupMetadata_group_id_fn_t,
    generation_id: kafka_consumer_ConsumerGroupMetadata_generation_id_fn_t,
    member_id: kafka_consumer_ConsumerGroupMetadata_member_id_fn_t,
    group_instance_id: kafka_consumer_ConsumerGroupMetadata_group_instance_id_fn_t,
) -> *mut kafka_consumer_ConsumerGroupMetadata_t {
    box_group_metadata(Arc::new(CGroupMetadata {
        self_: SendPtr(self_),
        group_id,
        generation_id,
        member_id,
        group_instance_id,
    }))
}

/// `groupId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    let inner = unsafe { inner_ref(self_) };
    inner.group_id_c.get_or_init(|| owned_c_string(inner.meta.group_id())).as_ptr()
}

/// `generationId()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_generation_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> i32 {
    unsafe { inner_ref(self_) }.meta.generation_id()
}

/// `memberId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_member_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    let inner = unsafe { inner_ref(self_) };
    inner
        .member_id_c
        .get_or_init(|| owned_c_string(inner.meta.member_id()))
        .as_ptr()
}

/// `groupInstanceId()`: borrowed from the handle, or `NULL` for
/// `Optional.empty()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_instance_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    let inner = unsafe { inner_ref(self_) };
    inner
        .group_instance_id_c
        .get_or_init(|| inner.meta.group_instance_id().map(owned_c_string))
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
}

/// Frees a handle; a null pointer is a no-op. A C implementation's `self_`
/// stays the caller's.
///
/// # Safety
///
/// `self_` must be null or a valid handle not used afterwards.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_destroy(
    self_: *mut kafka_consumer_ConsumerGroupMetadata_t,
) {
    if !self_.is_null() {
        unsafe { drop(Box::from_raw(self_ as *mut ConsumerGroupMetadataInner)) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        group: CString,
        member: CString,
        instance: Option<CString>,
        generation: i32,
    }

    unsafe extern "C" fn group_id(self_: *mut c_void) -> *const c_char {
        unsafe { &*(self_ as *const Fixture) }.group.as_ptr()
    }
    unsafe extern "C" fn generation_id(self_: *mut c_void) -> i32 {
        unsafe { &*(self_ as *const Fixture) }.generation
    }
    unsafe extern "C" fn member_id(self_: *mut c_void) -> *const c_char {
        unsafe { &*(self_ as *const Fixture) }.member.as_ptr()
    }
    unsafe extern "C" fn group_instance_id(self_: *mut c_void) -> *const c_char {
        unsafe { &*(self_ as *const Fixture) }
            .instance
            .as_ref()
            .map_or(std::ptr::null(), |c| c.as_ptr())
    }

    #[test]
    fn c_implementation_is_read_through_the_trait_and_the_getters() {
        let mut fixture = Fixture { group: c"g".to_owned(), member: c"m".to_owned(), instance: None, generation: 7 };
        let handle = unsafe {
            kafka_consumer_ConsumerGroupMetadata_new(
                &mut fixture as *mut Fixture as *mut c_void,
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            )
        };
        unsafe {
            let meta = group_metadata_ref(handle);
            assert_eq!(meta.group_id(), "g");
            assert_eq!(meta.member_id(), "m");
            assert_eq!(meta.generation_id(), 7);
            assert_eq!(meta.group_instance_id(), None);
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerGroupMetadata_group_id(handle))
                    .to_str()
                    .unwrap(),
                "g"
            );
            assert_eq!(kafka_consumer_ConsumerGroupMetadata_generation_id(handle), 7);
            assert!(kafka_consumer_ConsumerGroupMetadata_group_instance_id(handle).is_null());
            kafka_consumer_ConsumerGroupMetadata_destroy(handle);
        }

        fixture.instance = Some(c"i".to_owned());
        let handle = unsafe {
            kafka_consumer_ConsumerGroupMetadata_new(
                &mut fixture as *mut Fixture as *mut c_void,
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            )
        };
        unsafe {
            assert_eq!(
                CStr::from_ptr(kafka_consumer_ConsumerGroupMetadata_group_instance_id(handle))
                    .to_str()
                    .unwrap(),
                "i"
            );
            kafka_consumer_ConsumerGroupMetadata_destroy(handle);
        }
    }
}

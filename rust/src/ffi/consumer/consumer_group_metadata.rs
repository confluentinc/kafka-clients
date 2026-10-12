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
//! referenced. The safe rule is the one
//! [`kafka_consumer_ConsumerGroupMetadata_new`] states: until the handle is
//! destroyed and every operation it was passed to has completed.

#![expect(non_camel_case_types)]

use std::ffi::{CStr, CString, c_char, c_void};
use std::fmt;
use std::sync::{Arc, OnceLock};

use crate::consumer::ConsumerGroupMetadata;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::ffi_guard;
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
/// `meta` must be a valid group-metadata handle, kept alive for `'a`.
pub(crate) unsafe fn group_metadata_ref<'a>(
    meta: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> &'a Arc<dyn ConsumerGroupMetadata> {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid handle, i.e. a
    // `ConsumerGroupMetadataInner` from `box_group_metadata`, kept alive for `'a`; nothing takes a
    // `&mut` to it before `kafka_consumer_ConsumerGroupMetadata_destroy`, so the shared reborrow is
    // valid for `'a`.
    &unsafe { &*(meta as *const ConsumerGroupMetadataInner) }.meta
}

/// The handle's contents, for the getters.
///
/// # Safety
///
/// `meta` must be a valid group-metadata handle, kept alive for `'a`.
unsafe fn inner_ref<'a>(meta: *const kafka_consumer_ConsumerGroupMetadata_t) -> &'a ConsumerGroupMetadataInner {
    // SAFETY: Per this function's `# Safety`, `meta` is a valid handle, i.e. a
    // `ConsumerGroupMetadataInner` from `box_group_metadata`, kept alive for `'a`; nothing takes a
    // `&mut` to it before `kafka_consumer_ConsumerGroupMetadata_destroy`, so the shared reborrow is
    // valid for `'a`.
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
    ///
    /// # Safety
    ///
    /// `ptr` must be null or a NUL-terminated string, alive and unmodified
    /// for `'a`.
    unsafe fn string<'a>(ptr: *const c_char) -> Option<&'a str> {
        if ptr.is_null() {
            return None;
        }
        // SAFETY: `ptr` is non-null (checked above) and, per this function's `# Safety`, a
        // NUL-terminated string alive and unmodified for `'a`, as `CStr::from_ptr` requires.
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
        // SAFETY: `self.group_id` and `self.self_` are the pair the C caller registered together
        // through `kafka_consumer_ConsumerGroupMetadata_new`, whose `# Safety` keeps both valid,
        // and each string the function returns alive and unmodified, until the handle is destroyed
        // and every operation it was passed to has completed, and allows the call from any thread,
        // concurrently with others. `self` is reached only through the handle or such an operation,
        // so that outlasts this borrow of `self`, which the result is tied to: `string`'s
        // requirement holds.
        unsafe { Self::string((self.group_id)(self.self_.get())) }.unwrap_or("")
    }

    fn generation_id(&self) -> i32 {
        // SAFETY: `self.generation_id` and `self.self_` are the pair the C caller registered
        // together through `kafka_consumer_ConsumerGroupMetadata_new`, whose `# Safety` keeps both
        // valid until the handle is destroyed and every operation it was passed to has completed,
        // and allows the call from any thread, concurrently with others. `self` is reached only
        // through the handle or such an operation, so this `&self` call comes before that.
        unsafe { (self.generation_id)(self.self_.get()) }
    }

    fn member_id(&self) -> &str {
        // SAFETY: `self.member_id` and `self.self_` are the pair the C caller registered together
        // through `kafka_consumer_ConsumerGroupMetadata_new`, whose `# Safety` keeps both valid,
        // and each string the function returns alive and unmodified, until the handle is destroyed
        // and every operation it was passed to has completed, and allows the call from any thread,
        // concurrently with others. `self` is reached only through the handle or such an operation,
        // so that outlasts this borrow of `self`, which the result is tied to: `string`'s
        // requirement holds.
        unsafe { Self::string((self.member_id)(self.self_.get())) }.unwrap_or("")
    }

    fn group_instance_id(&self) -> Option<&str> {
        // SAFETY: `self.group_instance_id` and `self.self_` are the pair the C caller registered
        // together through `kafka_consumer_ConsumerGroupMetadata_new`, whose `# Safety` keeps both
        // valid, and each string the function returns alive and unmodified, until the handle is
        // destroyed and every operation it was passed to has completed, and allows the call from
        // any thread, concurrently with others. `self` is reached only through the handle or such
        // an operation, so that outlasts this borrow of `self`, which the result is tied to:
        // `string`'s requirement holds.
        unsafe { Self::string((self.group_instance_id)(self.self_.get())) }
    }
}

/// Registers a C implementation (CLAUDE.md §4 rule 3): the caller owns
/// `self_` and keeps it alive until the handle is destroyed and every
/// operation the handle was passed to has completed: an operation may keep
/// the implementation after it returns, as a `_cb` one does until its
/// callback has fired. All four methods are abstract in Java, so none of the
/// pointers may be `NULL`. They may be called from any thread, concurrently.
///
/// # Safety
///
/// The function pointers and `self_` must stay valid, and each string a
/// function returns alive and unmodified, until the handle is destroyed and
/// every operation it was passed to has completed; the functions must be
/// safe to call from any thread, concurrently.
#[ffi_guard]
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this call, as
    // `inner_ref` requires; the reference does not outlive the call, and the returned pointer
    // borrows a `CString` the handle keeps until it is destroyed.
    let inner = unsafe { inner_ref(self_) };
    inner.group_id_c.get_or_init(|| owned_c_string(inner.meta.group_id())).as_ptr()
}

/// `generationId()`.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_generation_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> i32 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this expression,
    // the only use of the reference, as `inner_ref` requires.
    unsafe { inner_ref(self_) }.meta.generation_id()
}

/// `memberId()`: borrowed from the handle.
///
/// # Safety
///
/// `self_` must be a valid handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_member_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this call, as
    // `inner_ref` requires; the reference does not outlive the call, and the returned pointer
    // borrows a `CString` the handle keeps until it is destroyed.
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
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_group_instance_id(
    self_: *const kafka_consumer_ConsumerGroupMetadata_t,
) -> *const c_char {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid handle, alive for this call, as
    // `inner_ref` requires; the reference does not outlive the call, and the returned pointer
    // borrows a `CString` the handle keeps until it is destroyed.
    let inner = unsafe { inner_ref(self_) };
    inner
        .group_instance_id_c
        .get_or_init(|| inner.meta.group_instance_id().map(owned_c_string))
        .as_ref()
        .map_or(std::ptr::null(), |c| c.as_ptr())
}

/// Frees an owned handle; a null pointer is a no-op. A C implementation's
/// `self_` stays the caller's.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerGroupMetadata_destroy(
    self_: *mut kafka_consumer_ConsumerGroupMetadata_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. a `Box<ConsumerGroupMetadataInner>` from
        // `box_group_metadata`, so taking it back and dropping it frees it exactly once.
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
        // SAFETY: Test code: `self_` is the `&mut fixture` the test registered, alive and not
        // modified while a handle built on it exists.
        unsafe { &*(self_ as *const Fixture) }.group.as_ptr()
    }
    unsafe extern "C" fn generation_id(self_: *mut c_void) -> i32 {
        // SAFETY: Test code: `self_` is the `&mut fixture` the test registered, alive and not
        // modified while a handle built on it exists.
        unsafe { &*(self_ as *const Fixture) }.generation
    }
    unsafe extern "C" fn member_id(self_: *mut c_void) -> *const c_char {
        // SAFETY: Test code: `self_` is the `&mut fixture` the test registered, alive and not
        // modified while a handle built on it exists.
        unsafe { &*(self_ as *const Fixture) }.member.as_ptr()
    }
    unsafe extern "C" fn group_instance_id(self_: *mut c_void) -> *const c_char {
        // SAFETY: Test code: `self_` is the `&mut fixture` the test registered, alive and not
        // modified while a handle built on it exists.
        unsafe { &*(self_ as *const Fixture) }
            .instance
            .as_ref()
            .map_or(std::ptr::null(), |c| c.as_ptr())
    }

    #[test]
    fn c_implementation_is_read_through_the_trait_and_the_getters() {
        let mut fixture = Fixture { group: c"g".to_owned(), member: c"m".to_owned(), instance: None, generation: 7 };
        // SAFETY: Test code: `fixture` outlives the handle, which is destroyed before `fixture` is
        // next modified, and its strings stay alive with it; the four functions match the pointer
        // types.
        let handle = unsafe {
            kafka_consumer_ConsumerGroupMetadata_new(
                &mut fixture as *mut Fixture as *mut c_void,
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            )
        };
        // SAFETY: Test code: `handle` is an owned handle from `_new`, used here and destroyed once,
        // last; the returned strings are read before that.
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
        // SAFETY: Test code: `fixture` outlives the handle, which is destroyed before `fixture`
        // goes out of scope, and its strings stay alive with it; the four functions match the
        // pointer types.
        let handle = unsafe {
            kafka_consumer_ConsumerGroupMetadata_new(
                &mut fixture as *mut Fixture as *mut c_void,
                group_id,
                generation_id,
                member_id,
                group_instance_id,
            )
        };
        // SAFETY: Test code: `handle` is an owned handle from `_new`, used here and destroyed once,
        // last; the returned string is read before that.
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

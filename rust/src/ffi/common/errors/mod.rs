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

//! The `Error` variants carrying state beyond `message` / `source`
//! (CLAUDE.md §4, "Exceptions having additional fields in Java").
//!
//! Each such Java class gets an opaque `kafka_common_<Class>_t`. A handle is
//! either a **view** borrowed from a `kafka_common_Error_t` through
//! `kafka_common_Error_<snake(class)>` (null when the error is not that
//! class), valid until the error is destroyed and never passed to
//! `_destroy`, or an **owned** value built by the class's own constructors
//! and freed with `kafka_common_<Class>_destroy`. Both point at the same
//! [`Payload<T>`], so the getters do not care which they were given.
//!
//! When the view's natural name is already taken by the `Error` static
//! factory of the same class, the view keeps the `_error` suffix:
//! `kafka_common_Error_topic_authorization` builds the error and
//! `kafka_common_Error_topic_authorization_error` reads its payload back.
//! `check-ffi-translation` derives both names.
//!
//! Collection-valued getters build an owned `kafka_List_t` / `kafka_Map_t`
//! on each call, ordered by topic then partition (or sorted strings) so a C
//! caller sees a deterministic order; the caller frees it with the
//! container's `_destroy`, independently of the payload's lifetime.

pub(crate) mod consumer_commit_failed_error;
pub(crate) mod consumer_log_truncation_error;
pub(crate) mod consumer_no_offset_for_partition_error;
pub(crate) mod consumer_offset_out_of_range_error;
pub(crate) mod consumer_retriable_commit_failed_error;
pub(crate) mod duplicate_resource_error;
pub(crate) mod group_authorization_error;
pub(crate) mod invalid_topic_error;
pub(crate) mod local_callback_error;
pub(crate) mod quota_violation_error;
pub(crate) mod record_deserialization_error;
pub(crate) mod record_too_large_error;
pub(crate) mod resource_not_found_error;
pub(crate) mod throttling_quota_exceeded_error;
pub(crate) mod topic_authorization_error;

use std::any::Any;
use std::ffi::{CString, c_char};
use std::fmt;
use std::ptr;
use std::sync::OnceLock;

use crate::common::Error;
use crate::ffi::common::{ErrorInner, kafka_common_Error_t, take_error};
use crate::ffi::util::{into_c_string, owned_c_string};

/// What every payload class offers [`Payload<T>`]: Java's `getMessage()` /
/// `getCause()` pair and, for a class whose Java getter returns a `String`
/// the handle must keep alive (`resource()`, `groupId()`), that one string.
///
/// Rust-only FFI plumbing (DoD #7): one generic wrapper serves the twelve
/// classes instead of twelve hand-written caches of the same three fields.
pub(crate) trait PayloadClass: Clone + fmt::Display + Send + Sync + 'static {
    fn message(&self) -> &str;
    fn source(&self) -> Option<&Error>;
    fn text(&self) -> Option<&str> {
        None
    }
}

/// What a `kafka_common_<Class>_t` points at: the value plus the
/// NUL-terminated strings its borrowed getters return and the lazily built
/// cause handle, owned here so the chain of causes lives as long as the
/// payload (the same shape as [`ErrorInner`]).
pub(crate) struct Payload<T> {
    value: T,
    message_c: CString,
    text_c: Option<CString>,
    source: OnceLock<Option<Box<ErrorInner>>>,
    /// The borrowed handles a class's structured getters return (a
    /// `TopicPartition`, a `MetricName`, the record headers), built on first
    /// request by [`Payload::views`] and owned here so they live as long as
    /// the payload. One slot: a class has one set of views.
    views: OnceLock<Box<dyn Any + Send + Sync>>,
}

impl<T: PayloadClass> Payload<T> {
    pub(crate) fn new(value: T) -> Self {
        let message_c = owned_c_string(value.message());
        let text_c = value.text().map(owned_c_string);
        Self { value, message_c, text_c, source: OnceLock::new(), views: OnceLock::new() }
    }

    /// The class's cached views, built from the value by `build` on the first
    /// call. Every caller of one payload class must ask for the same `V`.
    pub(crate) fn views<V: Send + Sync + 'static>(&self, build: impl FnOnce(&T) -> V) -> &V {
        let views = self.views.get_or_init(|| Box::new(build(&self.value)));
        views.downcast_ref::<V>().expect("one payload class caches one kind of views")
    }

    /// Hands `value` to C as an owned handle of opaque type `P`.
    pub(crate) fn boxed<P>(value: T) -> *mut P {
        Box::into_raw(Box::new(Self::new(value))) as *mut P
    }

    /// The payload behind a handle of opaque type `P`.
    ///
    /// # Safety
    ///
    /// `handle` must be a view or owned handle of this payload class.
    pub(crate) unsafe fn from_ptr<'a, P>(handle: *const P) -> &'a Self {
        unsafe { &*(handle as *const Self) }
    }

    /// The payload behind an owned handle of opaque type `P`, mutably.
    ///
    /// # Safety
    ///
    /// `handle` must be an owned handle not yet destroyed, never a view
    /// borrowed from a `kafka_common_Error_t`, and no reference obtained
    /// through [`Payload::from_ptr`] on it may be live.
    pub(crate) unsafe fn from_ptr_mut<'a, P>(handle: *mut P) -> &'a mut Self {
        unsafe { &mut *(handle as *mut Self) }
    }

    /// Replaces the value in place, as a Rust `with_*` builder step does on
    /// an owned handle: the message and text strings are rebuilt and the
    /// cached cause dropped, so strings and cause previously borrowed from
    /// the handle are invalidated. The structured views are kept, so the
    /// replacement must leave the fields they derive from unchanged (the one
    /// caller, `with_source`, changes only the cause).
    pub(crate) fn replace_value(&mut self, replace: impl FnOnce(T) -> T) {
        let value = replace(self.value.clone());
        self.message_c = owned_c_string(value.message());
        self.text_c = value.text().map(owned_c_string);
        self.source = OnceLock::new();
        self.value = value;
    }

    /// Frees an owned handle of opaque type `P`; null is a no-op.
    ///
    /// # Safety
    ///
    /// `handle` must be null or an owned handle not yet destroyed, never a
    /// view borrowed from a `kafka_common_Error_t`.
    pub(crate) unsafe fn destroy<P>(handle: *mut P) {
        if !handle.is_null() {
            drop(unsafe { Box::from_raw(handle as *mut Self) });
        }
    }

    pub(crate) fn value(&self) -> &T {
        &self.value
    }

    /// `getMessage()`, borrowed from the handle.
    pub(crate) fn message_ptr(&self) -> *const c_char {
        self.message_c.as_ptr()
    }

    /// The class's cached string, borrowed from the handle; null for Java's
    /// null.
    pub(crate) fn text_ptr(&self) -> *const c_char {
        self.text_c.as_ref().map_or(ptr::null(), |text| text.as_ptr())
    }

    /// `getCause()`: a borrowed error handle, or null when there is none.
    pub(crate) fn source_ptr(&self) -> *const kafka_common_Error_t {
        match self
            .source
            .get_or_init(|| self.value.source().cloned().map(|e| Box::new(ErrorInner::new(e))))
        {
            Some(inner) => &**inner as *const ErrorInner as *const kafka_common_Error_t,
            None => ptr::null(),
        }
    }

    /// `toString()`, as an owned string freed with `kafka_string_destroy`.
    pub(crate) fn to_c_string(&self) -> *mut c_char {
        into_c_string(&self.value.to_string())
    }
}

/// Takes ownership of a cause passed to a constructor as a consumed
/// `kafka_common_Error_t *`; the handle is invalid afterwards. A null handle
/// violates the constructor's precondition and reads as a bare
/// `KafkaException`.
///
/// # Safety
///
/// `source` must be null or an owned error handle not yet destroyed.
pub(crate) unsafe fn take_source(source: *mut kafka_common_Error_t) -> Error {
    unsafe { take_error(source) }.unwrap_or_else(|| Error::kafka_message(""))
}

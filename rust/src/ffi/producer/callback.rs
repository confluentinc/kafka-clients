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

//! `kafka_producer_Callback_t`: `org.apache.kafka.clients.producer.Callback`,
//! the delivery callback of `Producer.send(record, callback)`.
//!
//! Rust translates the interface to a closure (`producer::Callback`), which
//! C cannot write; so C implements it as a rule 3 interface (CLAUDE.md §4):
//! `_new(void *self, on_completion)` builds a registration that
//! `kafka_producer_Producer_send_with_callback` copies into the Rust closure.
//! The registration handle may be destroyed as soon as `send_with_callback`
//! returned; `self` must outlive the callback, which fires once per record
//! when the producer completes, fails or aborts it.
//!
//! The callback is fired by the producer's background task, so it is always
//! queued (§4 rule 5): `kafka_producer_Producer__execute_callbacks` invokes
//! `on_completion(self, metadata, error)` on the pumping thread, exactly one
//! of the two arguments being non-`NULL` the way Java hands exactly one
//! non-`null` (a pre-accumulator rejection delivers the placeholder metadata
//! Java delivers, beside the error). Both are borrowed for the call.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::Error;
use crate::ffi::callback_queue::{CallbackQueue, SendPtr};
use crate::ffi::common::{box_error, kafka_common_Error_destroy, kafka_common_Error_t};
use crate::ffi::producer::record_metadata::{
    box_record_metadata, kafka_producer_RecordMetadata_destroy, kafka_producer_RecordMetadata_t,
};
use crate::producer::{Callback, RecordMetadata};

/// Opaque handle to a C `Callback` registration.
#[repr(C)]
pub struct kafka_producer_Callback_t {
    _private: [u8; 0],
}

/// `void onCompletion(RecordMetadata metadata, Exception exception)` of a C
/// implementation: `metadata` and `error` are borrowed for the call, exactly
/// one of them is `NULL` except for the pre-accumulator rejection described
/// in the module documentation.
pub type kafka_producer_Callback_on_completion_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    metadata: *const kafka_producer_RecordMetadata_t,
    error: *const kafka_common_Error_t,
);

/// The registration a handle holds; copied into every Rust closure built
/// from it, so the handle itself is not needed after `send_with_callback`.
#[derive(Clone, Copy)]
pub(crate) struct CallbackRegistration {
    self_: SendPtr,
    on_completion: kafka_producer_Callback_on_completion_fn_t,
}

impl CallbackRegistration {
    /// Invokes the C implementation.
    ///
    /// # Safety
    ///
    /// `metadata` and `error` must be null or live handles.
    pub(crate) unsafe fn fire(
        self,
        metadata: *const kafka_producer_RecordMetadata_t,
        error: *const kafka_common_Error_t,
    ) {
        unsafe { (self.on_completion)(self.self_.0, metadata, error) };
    }

    /// The Rust delivery callback standing for this registration: it boxes
    /// what the producer delivers and queues the C invocation on `queue`,
    /// freeing both handles after it ran.
    pub(crate) fn into_callback(self, queue: Arc<CallbackQueue>) -> Callback {
        Box::new(move |metadata: Option<&RecordMetadata>, error: Option<&Error>| {
            let metadata =
                SendPtr(metadata.map_or(std::ptr::null_mut(), |m| box_record_metadata(m.clone()) as *mut c_void));
            let error = SendPtr(error.map_or(std::ptr::null_mut(), |e| box_error(e.clone()) as *mut c_void));
            queue.push(Box::new(move || unsafe {
                let metadata = metadata.get() as *mut kafka_producer_RecordMetadata_t;
                let error = error.get() as *mut kafka_common_Error_t;
                self.fire(metadata, error);
                kafka_producer_RecordMetadata_destroy(metadata);
                kafka_common_Error_destroy(error);
            }));
        })
    }
}

/// The registration behind a handle.
///
/// # Safety
///
/// `callback` must be a live handle.
pub(crate) unsafe fn callback_registration(callback: *const kafka_producer_Callback_t) -> CallbackRegistration {
    *unsafe { &*(callback as *const CallbackRegistration) }
}

/// Registers a C implementation of `Callback`. The caller owns `self_` and
/// keeps it alive until the callback fired; the handle is freed with
/// [`kafka_producer_Callback_destroy`], at any time after the `send` that
/// took it returned.
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_Callback_new(
    self_: *mut c_void,
    on_completion: kafka_producer_Callback_on_completion_fn_t,
) -> *mut kafka_producer_Callback_t {
    Box::into_raw(Box::new(CallbackRegistration { self_: SendPtr(self_), on_completion }))
        as *mut kafka_producer_Callback_t
}

/// `Callback.onCompletion(metadata, exception)`: invokes the C implementation
/// on the calling thread with the borrowed arguments.
///
/// # Safety
///
/// `self_` must be a live handle; `metadata` and `error` null or live
/// handles.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Callback_on_completion(
    self_: *const kafka_producer_Callback_t,
    metadata: *const kafka_producer_RecordMetadata_t,
    error: *const kafka_common_Error_t,
) {
    unsafe { callback_registration(self_).fire(metadata, error) };
}

/// Frees the handle; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle not yet destroyed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Callback_destroy(self_: *mut kafka_producer_Callback_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut CallbackRegistration) });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::common::TopicPartition;
    use crate::ffi::common::{error_ref, kafka_common_Error_message};
    use crate::ffi::producer::record_metadata::kafka_producer_RecordMetadata_offset;
    use crate::ffi::util::c_str_to_string;

    #[derive(Default)]
    struct Log(Mutex<Vec<(i64, Option<String>)>>);

    unsafe extern "C" fn on_completion(
        self_: *mut c_void,
        metadata: *const kafka_producer_RecordMetadata_t,
        error: *const kafka_common_Error_t,
    ) {
        let log = unsafe { &*(self_ as *const Log) };
        let offset = if metadata.is_null() {
            -1
        } else {
            unsafe { kafka_producer_RecordMetadata_offset(metadata) }
        };
        let error = (!error.is_null()).then(|| unsafe { c_str_to_string(kafka_common_Error_message(error)) });
        log.0.lock().unwrap().push((offset, error));
    }

    #[test]
    fn invoker_and_queued_delivery_reach_the_implementation_once_each() {
        let log = Log::default();
        let handle = kafka_producer_Callback_new(&log as *const Log as *mut c_void, on_completion);
        let metadata = box_record_metadata(RecordMetadata::new(TopicPartition::new("t", 0), 5, 0, -1, -1, -1));
        let error = box_error(Error::timeout("late"));
        unsafe {
            kafka_producer_Callback_on_completion(handle, metadata, std::ptr::null());
            kafka_producer_Callback_on_completion(handle, std::ptr::null(), error);
        }
        assert_eq!(*log.0.lock().unwrap(), vec![(5, None), (-1, Some("late".to_string()))]);
        assert_eq!(
            unsafe { error_ref(error) }.error.message(),
            "late",
            "the invoker borrows its arguments"
        );

        // Through the Rust closure the invocation is queued, not run inline.
        let queue = Arc::new(CallbackQueue::new());
        let callback = unsafe { callback_registration(handle) }.into_callback(Arc::clone(&queue));
        unsafe { kafka_producer_Callback_destroy(handle) };
        callback(Some(&RecordMetadata::new(TopicPartition::new("t", 0), 9, 1, -1, -1, -1)), None);
        assert_eq!(log.0.lock().unwrap().len(), 2, "queued, not fired inline");
        assert_eq!(queue.execute(), 1);
        assert_eq!(log.0.lock().unwrap()[2], (10, None));

        unsafe {
            kafka_producer_RecordMetadata_destroy(metadata);
            kafka_common_Error_destroy(error);
            kafka_producer_Callback_destroy(std::ptr::null_mut());
        }
    }
}

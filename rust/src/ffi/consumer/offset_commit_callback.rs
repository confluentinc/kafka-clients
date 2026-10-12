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

//! C interface for `org.apache.kafka.clients.consumer.OffsetCommitCallback`
//! (CLAUDE.md §4 rule 3).
//!
//! `onComplete` is an `async fn` in Rust, so the C method returns `void`,
//! takes a trailing `int64_t callback_id` and reports through
//! `kafka_consumer_Consumer__set_callback_result`. Java's `onComplete` is
//! `void`: a reported error is ignored (freed), the report only tells the
//! consumer the callback finished. The consumer invokes it from the
//! application thread during the next blocking-style call (`poll`,
//! `commit_*`, ...; consumer-threading.md §31), on the calling thread for a
//! blocking entry point and through `kafka_consumer_Consumer__execute_callbacks`
//! for a `_cb` one.

#![expect(non_camel_case_types)]

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::Arc;

use async_trait::async_trait;

use crate::common::{Error, TopicPartition};
use crate::consumer::{OffsetAndMetadata, OffsetCommitCallback};
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::common::{
    box_error, deliver_panic_opaque, error_ref, kafka_common_Error_destroy, kafka_common_Error_t, run_inline,
};
use crate::ffi::consumer::offset_and_metadata::{map_offset_and_metadata, offset_and_metadata_map};
use crate::ffi::consumer::{Delivery, register_callback_result};
use crate::ffi::ffi_guard;
use crate::ffi::util::{kafka_Map_destroy, kafka_Map_t};

/// Opaque handle to an `OffsetCommitCallback` implementation registered
/// with [`kafka_consumer_OffsetCommitCallback_new`].
#[repr(C)]
pub struct kafka_consumer_OffsetCommitCallback_t {
    _private: [u8; 0],
}

/// `onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception)`
/// of a C implementation: `offsets` maps `kafka_common_TopicPartition_t *`
/// to `kafka_consumer_OffsetAndMetadata_t *`, sorted and borrowed for the
/// call; `error` is `NULL` on success, otherwise borrowed for the call. The
/// method is `async` in Rust (CLAUDE.md §4 rule 3): it reports completion
/// with `kafka_consumer_Consumer__set_callback_result(consumer, callback_id, NULL)`
/// (an error passed there is ignored, `onComplete` being `void` in Java).
pub type kafka_consumer_OffsetCommitCallback_on_complete_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    offsets: *const kafka_Map_t,
    error: *const kafka_common_Error_t,
    callback_id: i64,
);

/// What a callback handle points at (see `ListenerRegistration`).
#[derive(Clone, Copy)]
pub(crate) struct CommitCallbackRegistration {
    self_: SendPtr,
    on_complete: kafka_consumer_OffsetCommitCallback_on_complete_fn_t,
}

impl CommitCallbackRegistration {
    /// Invokes the C implementation on the calling thread with owned copies
    /// of `offsets` and `error`, freed after it returned, and a fresh
    /// callback id whose report reaches `sink`.
    fn fire(
        self,
        offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
        error: Option<&Error>,
        sink: Box<dyn FnOnce(Result<(), Error>) + Send>,
    ) {
        // The id is registered last, right before the call, so a `_cb` entry
        // point's guard that reports a panic before then is the only
        // completion.
        let offsets = offset_and_metadata_map(offsets);
        let error = error.map_or(std::ptr::null_mut(), |e| box_error(e.clone()));
        let callback_id = register_callback_result(sink);
        // SAFETY: `self.on_complete` and `self.self_` are the pair the C caller registered together
        // through `kafka_consumer_OffsetCommitCallback_new`, whose rustdoc has the caller keep
        // `self_` alive until the handle is destroyed and, once passed to a `commit_async_*`, until
        // `onComplete` fired or the consumer was destroyed. A registration is copied only out of a
        // live handle: by an invoker, which fires it within its own call, or into the adapter of
        // one `commit_async_*`, which fires it as that commit's `onComplete`; so this call falls
        // within that window. `offsets` is the owned map `offset_and_metadata_map` just built and
        // `error` null or the fresh `box_error` handle above, both borrowed by the C method for the
        // call, as the `_fn_t` rustdoc states, then freed here exactly once;
        // `kafka_common_Error_destroy` accepts null.
        unsafe {
            (self.on_complete)(self.self_.get(), offsets, error, callback_id);
            kafka_Map_destroy(offsets);
            kafka_common_Error_destroy(error);
        }
    }
}

/// The `OffsetCommitCallback` a consumer holds for a C registration.
struct CCommitCallback {
    registration: CommitCallbackRegistration,
    delivery: Arc<Delivery>,
}

#[async_trait]
impl OffsetCommitCallback for CCommitCallback {
    async fn on_complete(&self, offsets: &HashMap<TopicPartition, OffsetAndMetadata>, error: Option<&Error>) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let registration = self.registration;
        let offsets = offsets.clone();
        let error = error.cloned();
        self.delivery.invoke(Box::new(move || {
            registration.fire(
                &offsets,
                error.as_ref(),
                Box::new(move |result| {
                    let _ = tx.send(result);
                }),
            );
        }));
        // `onComplete` is void in Java: the report only marks completion.
        let _ = rx.await;
    }
}

/// The registration behind a handle.
///
/// # Safety
///
/// `callback` must be a live callback handle.
pub(crate) unsafe fn commit_callback_registration(
    callback: *const kafka_consumer_OffsetCommitCallback_t,
) -> CommitCallbackRegistration {
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, i.e. the
    // `Box<CommitCallbackRegistration>` of `kafka_consumer_OffsetCommitCallback_new`, not yet
    // destroyed; the reference is used only to copy the `Copy` registration out, within this
    // expression.
    *unsafe { &*(callback as *const CommitCallbackRegistration) }
}

/// The Rust callback a consumer registers for the C implementation behind
/// `callback`, delivering through `delivery`.
///
/// # Safety
///
/// `callback` must be a live callback handle.
pub(crate) unsafe fn commit_callback_adapter(
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    delivery: Arc<Delivery>,
) -> Arc<dyn OffsetCommitCallback> {
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_registration` requires.
    Arc::new(CCommitCallback { registration: unsafe { commit_callback_registration(callback) }, delivery })
}

/// Registers a C implementation of `OffsetCommitCallback`. The caller owns
/// `self_` and keeps it alive until the handle is destroyed and, once passed
/// to a `commit_async_*`, until `onComplete` fired (or the consumer was
/// destroyed).
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_consumer_OffsetCommitCallback_new(
    self_: *mut c_void,
    on_complete: kafka_consumer_OffsetCommitCallback_on_complete_fn_t,
) -> *mut kafka_consumer_OffsetCommitCallback_t {
    Box::into_raw(Box::new(CommitCallbackRegistration { self_: SendPtr(self_), on_complete }))
        as *mut kafka_consumer_OffsetCommitCallback_t
}

/// Invokes the implementation's `onComplete` on the calling thread with
/// copies of `offsets` (a map of `kafka_common_TopicPartition_t *` to
/// `kafka_consumer_OffsetAndMetadata_t *`) and `error` (`NULL` for
/// success), and waits for its `__set_callback_result` report.
///
/// # Safety
///
/// `self_` must be a live callback handle, `offsets` a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one, and `error` null or a live
/// error.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetCommitCallback_on_complete(
    self_: *const kafka_consumer_OffsetCommitCallback_t,
    offsets: *const kafka_Map_t,
    error: *const kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live callback handle, as
    // `commit_callback_registration` requires.
    let registration = unsafe { commit_callback_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of topic-partition to
    // offset-and-metadata handles that no other call mutates during this one, as
    // `map_offset_and_metadata` requires.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: `error` is non-null (checked in this expression) and, per this function's `# Safety`,
    // a live error handle, as `error_ref` requires; the reference is used only to clone the error,
    // within this expression.
    let error = (!error.is_null()).then(|| unsafe { error_ref(error) }.error.clone());
    let (tx, rx) = std::sync::mpsc::channel();
    registration.fire(
        &offsets,
        error.as_ref(),
        Box::new(move |result| {
            let _ = tx.send(result);
        }),
    );
    let _ = rx.recv();
}

/// Completion of [`kafka_consumer_OffsetCommitCallback_on_complete_cb`]:
/// `onComplete` is void, so only `opaque` is passed.
pub type kafka_consumer_OffsetCommitCallback_on_complete_cb_t = unsafe extern "C" fn(opaque: *mut c_void);

/// Non-blocking `on_complete`: the method runs on the calling thread, `cb`
/// fires when its completion is reported (a standalone callback has no
/// callbacks vector to queue on, so `cb` runs on the reporting thread).
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as the last step, to the sink `fire` registers right before calling the C method; nothing
    // after that can panic, as the C method cannot unwind into Rust and the two destroys are
    // guarded. So `cb` has not fired for this `opaque`, as `deliver_panic_opaque` requires;
    // `run_inline` fires it on this thread.
    unsafe { deliver_panic_opaque(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetCommitCallback_on_complete_cb(
    self_: *const kafka_consumer_OffsetCommitCallback_t,
    offsets: *const kafka_Map_t,
    error: *const kafka_common_Error_t,
    cb: kafka_consumer_OffsetCommitCallback_on_complete_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live callback handle, as
    // `commit_callback_registration` requires.
    let registration = unsafe { commit_callback_registration(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is a valid map of topic-partition to
    // offset-and-metadata handles that no other call mutates during this one, as
    // `map_offset_and_metadata` requires.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: `error` is non-null (checked in this expression) and, per this function's `# Safety`,
    // a live error handle, as `error_ref` requires; the reference is used only to clone the error,
    // within this expression.
    let error = (!error.is_null()).then(|| unsafe { error_ref(error) }.error.clone());
    let opaque = SendPtr(opaque);
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    // SAFETY: `cb` and `opaque` are the pair the C caller passed to this function, per its
    // `# Safety` a valid completion; `cb` fires only from this sink, which
    // `register_callback_result` holds under a fresh id and runs at most once, as a `FnOnce`, when
    // the report for that id arrives.
    registration.fire(&offsets, error.as_ref(), Box::new(move |_| unsafe { cb(opaque.get()) }));
}

/// Frees the handle; a consumer it was passed to keeps its own copy of the
/// registration. A null pointer is a no-op.
///
/// # Safety
///
/// `self_` must be null or a live handle not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_OffsetCommitCallback_destroy(
    self_: *mut kafka_consumer_OffsetCommitCallback_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is a live
        // handle not used afterwards, i.e. the `Box<CommitCallbackRegistration>` of
        // `kafka_consumer_OffsetCommitCallback_new`, so taking it back and dropping it frees it
        // exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut CommitCallbackRegistration) });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::ffi::common::fail_point;
    use crate::ffi::common::kafka_common_Error_message;
    use crate::ffi::common::panic_probe::{Probe, on_opaque};
    use crate::ffi::consumer::complete_callback_result;
    use crate::ffi::consumer::offset_and_metadata::kafka_consumer_OffsetAndMetadata_offset;
    use crate::ffi::util::{c_str_to_string, kafka_Map_size, kafka_Map_value};

    #[derive(Default)]
    struct Log(Mutex<Vec<(Vec<i64>, Option<String>)>>);

    unsafe extern "C" fn on_complete(
        self_: *mut c_void,
        offsets: *const kafka_Map_t,
        error: *const kafka_common_Error_t,
        callback_id: i64,
    ) {
        // SAFETY: Test code: `self_` is the `&log` the test registered, alive for the whole test.
        let log = unsafe { &*(self_ as *const Log) };
        // SAFETY: Test code: `offsets` is the map `fire` passes, live for the call.
        let n = unsafe { kafka_Map_size(offsets) };
        let values = (0..n)
            // SAFETY: Test code: `offsets` is the map `fire` passes, live for the call, and `i` is
            // in range, so its value is an offset-and-metadata handle.
            .map(|i| unsafe { kafka_consumer_OffsetAndMetadata_offset(kafka_Map_value(offsets, i) as *const _) })
            .collect();
        // SAFETY: Test code: `error` is non-null (checked in this expression) and borrowed for the
        // call, and its message is a NUL-terminated string owned by it.
        let error = (!error.is_null()).then(|| unsafe { c_str_to_string(kafka_common_Error_message(error)) });
        log.0.lock().unwrap().push((values, error));
        // Reporting an error is allowed and ignored: `onComplete` is void.
        complete_callback_result(callback_id, Err(Error::local_illegal_state("ignored")));
    }

    #[test]
    fn invoker_copies_its_arguments_and_waits_for_the_report() {
        let log = Log::default();
        let handle = kafka_consumer_OffsetCommitCallback_new(&log as *const Log as *mut c_void, on_complete);
        let offsets = HashMap::from([(TopicPartition::new("t", 0), OffsetAndMetadata::new(7).unwrap())]);
        let c_offsets = offset_and_metadata_map(&offsets);
        let error = box_error(Error::timeout("late"));
        // SAFETY: Test code: `handle`, `c_offsets` and `error` are live handles this test made,
        // each destroyed once, after its last use.
        unsafe {
            kafka_consumer_OffsetCommitCallback_on_complete(handle, c_offsets, std::ptr::null());
            kafka_consumer_OffsetCommitCallback_on_complete(handle, c_offsets, error);
            assert_eq!(error_ref(error).error.message(), "late", "borrowed, not consumed");
            kafka_common_Error_destroy(error);
            kafka_Map_destroy(c_offsets);
        }
        assert_eq!(
            *log.0.lock().unwrap(),
            vec![(vec![7], None), (vec![7], Some("late".to_string()))]
        );

        let fired = Mutex::new(0);
        unsafe extern "C" fn cb(opaque: *mut c_void) {
            // SAFETY: Test code: `opaque` is `&fired`, alive until the test ends.
            *unsafe { &*(opaque as *const Mutex<i32>) }.lock().unwrap() += 1;
        }
        let c_offsets = offset_and_metadata_map(&offsets);
        // SAFETY: Test code: `handle` and `c_offsets` are live handles this test made, each
        // destroyed once, after its last use; `cb` matches the completion type and `&fired`
        // outlives the call.
        unsafe {
            kafka_consumer_OffsetCommitCallback_on_complete_cb(
                handle,
                c_offsets,
                std::ptr::null(),
                cb,
                &fired as *const _ as *mut c_void,
            );
            kafka_Map_destroy(c_offsets);
            kafka_consumer_OffsetCommitCallback_destroy(handle);
        }
        assert_eq!(*fired.lock().unwrap(), 1);
        assert_eq!(log.0.lock().unwrap().len(), 3);
    }

    /// A standalone callback has no callbacks vector, so a panic before
    /// `onComplete` fires `cb` once on the calling thread, before the call
    /// returns, and the C method never runs.
    #[test]
    fn a_panicking_on_complete_cb_fires_cb_once_inline() {
        let log = Log::default();
        let handle = kafka_consumer_OffsetCommitCallback_new(&log as *const Log as *mut c_void, on_complete);
        let c_offsets = offset_and_metadata_map(&HashMap::new());
        let probe = Probe::default();
        fail_point::arm();
        // SAFETY: Test code: `handle` and `c_offsets` are live handles this test made, each
        // destroyed once, after its last use; the probe outlives the call.
        unsafe {
            kafka_consumer_OffsetCommitCallback_on_complete_cb(
                handle,
                c_offsets,
                std::ptr::null(),
                on_opaque,
                probe.as_opaque(),
            );
            kafka_Map_destroy(c_offsets);
            kafka_consumer_OffsetCommitCallback_destroy(handle);
        }
        assert_eq!(probe.fired(), ["<no error>"]);
        assert!(log.0.lock().unwrap().is_empty(), "onComplete never ran");
    }
}

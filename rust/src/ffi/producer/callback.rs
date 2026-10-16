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
//! Rust translates the interface to the `producer::Callback` trait, which C
//! cannot implement; so C implements it as a rule 3 interface (CLAUDE.md §4):
//! `_new(void *self, on_completion)` builds a registration that
//! `kafka_producer_Producer_send_with_callback` consumes into a Rust
//! `Callback`, as the Rust `send_with_callback` takes its `Box<dyn Callback>`;
//! `self` must outlive the callback, which fires once per record when the
//! producer completes, fails or aborts it.
//!
//! The callback is fired by the producer's background task, so it is always
//! queued (§4 rule 5): `kafka_producer_Producer__execute_callbacks` invokes
//! `on_completion(self, metadata, error, callback_id)` on the pumping thread.
//! `metadata` is always the record's metadata (the placeholder Java delivers
//! for a failed record, whose offset is -1) and `error` is `NULL` on success,
//! the record's failure otherwise; both are borrowed for the call.
//!
//! Java's `onCompletion` runs on the producer's I/O thread, and the record's
//! future completes only once it returned (`ProducerBatch.completeFutureAndFireCallbacks`
//! fires the callbacks before `produceFuture.done()`), so `flush()` and
//! `commitTransaction()` return only after it. C's callback runs later, on
//! whichever thread pumps the queue, so it reports that it finished: it
//! calls `kafka_producer_Producer__set_callback_result(producer, callback_id,
//! NULL, error)`, inside `on_completion` or later, from any thread. Until
//! then the record's future does not complete and `flush` and the
//! transaction commit and abort wait for it. A reported error is Java's
//! `onCompletion` throwing: logged at error level, as Java logs it, and
//! freed; it fails nothing, and the record's future completes with the
//! record's own outcome.

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::sync::Arc;

use crate::common::Error;
use crate::common::internals::KafkaFutureImpl;
use crate::ffi::callback_queue::{CallbackQueue, SendPtr};
use crate::ffi::callback_result::{CallbackResults, INVOKER_RESULTS, ResultSink, void_sink};
use crate::ffi::common::{
    box_error, deliver_panic_opaque, kafka_common_Error_destroy, kafka_common_Error_t, run_inline,
};
use crate::ffi::ffi_guard;
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
/// implementation: `metadata` and `error` are borrowed for the call, as the
/// module documentation describes. The method reports that it finished with
/// `kafka_producer_Producer__set_callback_result(producer, callback_id, NULL, error)`,
/// inside the call or later, from any thread; `error` is `NULL`, or an owned
/// `kafka_common_Error_t *` standing for Java's `onCompletion` throwing,
/// which the producer logs and frees. Until the report the record's future
/// does not complete, and `flush`, `commit_transaction` and
/// `abort_transaction` wait for it.
pub type kafka_producer_Callback_on_completion_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    metadata: *const kafka_producer_RecordMetadata_t,
    error: *const kafka_common_Error_t,
    callback_id: i64,
);

/// The registration a handle holds, moved into the Rust callback built from
/// it when a function consumes the handle.
#[derive(Clone, Copy)]
pub(crate) struct CallbackRegistration {
    self_: SendPtr,
    on_completion: kafka_producer_Callback_on_completion_fn_t,
}

impl CallbackRegistration {
    /// Invokes the C implementation with `callback_id`, which its report
    /// carries.
    ///
    /// # Safety
    ///
    /// `metadata` and `error` must be null or live handles.
    pub(crate) unsafe fn fire(
        self,
        metadata: *const kafka_producer_RecordMetadata_t,
        error: *const kafka_common_Error_t,
        callback_id: i64,
    ) {
        // SAFETY: `on_completion` and `self_` are the pair the C implementation registered with
        // `kafka_producer_Callback_new`, whose contract keeps `self_` alive until the callback
        // fired, so calling `on_completion` with them meets its contract; per this function's
        // `# Safety`, `metadata` and `error` are null or live handles, borrowed for the call as
        // `kafka_producer_Callback_on_completion_fn_t` documents.
        unsafe { (self.on_completion)(self.self_.0, metadata, error, callback_id) };
    }

    /// The Rust delivery callback standing for this registration. When the
    /// record completes it registers, in `results`, the sink of the C
    /// callback's report, which completes `completion` with the record's
    /// outcome, and then queues the C invocation on `queue` with the id it
    /// got, boxing what the producer delivers and freeing both handles after
    /// the C call returned. A closure dropped without being called (a record
    /// rejected before the accumulator) registers nothing.
    ///
    /// Per record this allocates the sink, the queued job and the metadata
    /// handle, and the error handle for a failed record; no task is spawned
    /// (CLAUDE.md §13).
    pub(crate) fn into_callback(
        self,
        queue: Arc<CallbackQueue>,
        results: Arc<CallbackResults>,
        completion: KafkaFutureImpl<RecordMetadata>,
    ) -> Box<dyn Callback> {
        Box::new(move |metadata: Option<&RecordMetadata>, error: Option<&Error>| {
            let metadata = metadata.cloned();
            let error = error.cloned();
            let callback_id = results.register(report_sink(metadata.clone(), error.clone(), completion));
            // SAFETY: `metadata` and `error` are the `box_record_metadata` and `box_error` handles
            // built inside the job, or null, owned by the job alone: they are live for `fire`, which
            // borrows them, and the two `_destroy` calls (each a no-op on null) free each exactly
            // once after it returned. The job is a `FnOnce`, so the queue runs it once.
            queue.push(Box::new(move || unsafe {
                let metadata = metadata.map_or(std::ptr::null_mut(), box_record_metadata);
                let error = error.map_or(std::ptr::null_mut(), box_error);
                self.fire(metadata, error, callback_id);
                kafka_producer_RecordMetadata_destroy(metadata);
                kafka_common_Error_destroy(error);
            }));
        })
    }
}

/// The sink of a delivery callback's report: a reported error is Java's
/// `onCompletion` throwing, logged as `ProducerBatch.completeFutureAndFireCallbacks`
/// logs it (`ProducerBatch.java:319`) and then dropped; either way
/// `completion` then completes with the record's own outcome, its `error`
/// when it failed. A sink drained by the producer's `_destroy` completes it
/// the same way.
fn report_sink(
    metadata: Option<RecordMetadata>,
    error: Option<Error>,
    completion: KafkaFutureImpl<RecordMetadata>,
) -> ResultSink {
    void_sink(move |reported| {
        if let Err(thrown) = reported {
            match &metadata {
                Some(metadata) => log::error!(
                    "Error executing user-provided callback on message for topic-partition '{}-{}': {}",
                    metadata.topic(),
                    metadata.partition(),
                    thrown
                ),
                None => log::error!("Error executing user-provided callback on message: {thrown}"),
            }
        }
        match (metadata, error) {
            (_, Some(error)) => completion.complete_with_error(error),
            (Some(metadata), None) => completion.complete(metadata),
            // The producer always delivers one of the two.
            (None, None) => completion.complete_with_error(Error::local_illegal_state(
                "the record completed with neither metadata nor error",
            )),
        };
    })
}

/// Takes the registration a consumed handle holds, freeing the handle.
///
/// # Safety
///
/// `callback` must be a live handle, not used again by the caller.
pub(crate) unsafe fn take_callback(callback: *mut kafka_producer_Callback_t) -> CallbackRegistration {
    // SAFETY: Per this function's `# Safety`, `callback` is a live handle, i.e. the `Box::into_raw`
    // of a `CallbackRegistration` in `kafka_producer_Callback_new`, the only constructor, which
    // the caller does not use again, so `Box::from_raw` reclaims it exactly once.
    *unsafe { Box::from_raw(callback as *mut CallbackRegistration) }
}

/// Registers a C implementation of `Callback`. The caller owns `self_` and
/// keeps it alive until the callback fired. The handle is owned until a
/// function consumes it — a `send` taking a callback, or
/// [`kafka_producer_Callback_on_completion`] and its `_cb` twin, whatever
/// their outcome — and is freed with [`kafka_producer_Callback_destroy`] only
/// when none did.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_producer_Callback_new(
    self_: *mut c_void,
    on_completion: kafka_producer_Callback_on_completion_fn_t,
) -> *mut kafka_producer_Callback_t {
    Box::into_raw(Box::new(CallbackRegistration { self_: SendPtr(self_), on_completion }))
        as *mut kafka_producer_Callback_t
}

/// `Callback.onCompletion(metadata, exception)`: invokes the C implementation
/// on the calling thread with the borrowed arguments and waits for its
/// `kafka_producer_Producer__set_callback_result` report. No producer is
/// involved: the report's `producer` argument is not read, so it may be any
/// producer or `NULL`, and a reported error is freed. The callback fires once,
/// so `self_` is consumed, as the Rust `on_completion` takes `self: Box<Self>`.
///
/// # Safety
///
/// `self_` must be a live handle, not used again; `metadata` and `error` null
/// or live handles.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Callback_on_completion(
    self_: *mut kafka_producer_Callback_t,
    metadata: *const kafka_producer_RecordMetadata_t,
    error: *const kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle not used again, as
    // `take_callback` requires.
    let registration = unsafe { take_callback(self_) };
    let (tx, rx) = std::sync::mpsc::channel();
    let callback_id = INVOKER_RESULTS.register(void_sink(move |result| {
        let _ = tx.send(result);
    }));
    // SAFETY: Per this function's `# Safety`, `metadata` and `error` are null or live handles, as
    // `fire` requires.
    unsafe { registration.fire(metadata, error, callback_id) };
    // `onCompletion` is void in Java: the report only marks completion.
    let _ = rx.recv();
}

/// Completion of [`kafka_producer_Callback_on_completion_cb`]:
/// `onCompletion` is void, so only `opaque` is passed.
pub type kafka_producer_Callback_on_completion_cb_t = unsafe extern "C" fn(opaque: *mut c_void);

/// Non-blocking `on_completion`: the method runs on the calling thread and
/// `cb` fires when its completion is reported (a standalone callback has no
/// callbacks vector to queue on, so `cb` runs on the reporting thread). The
/// report is made as for [`kafka_producer_Callback_on_completion`].
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as its last step, to the sink it registers right before calling the C method; nothing after
    // that can panic, as the C method cannot unwind into Rust. So `cb` has not fired for this
    // `opaque`, as `deliver_panic_opaque` requires; `run_inline` fires it on this thread.
    unsafe { deliver_panic_opaque(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Callback_on_completion_cb(
    self_: *mut kafka_producer_Callback_t,
    metadata: *const kafka_producer_RecordMetadata_t,
    error: *const kafka_common_Error_t,
    cb: kafka_producer_Callback_on_completion_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle not used again, as
    // `take_callback` requires.
    let registration = unsafe { take_callback(self_) };
    let opaque = SendPtr(opaque);
    #[cfg(test)]
    crate::ffi::common::fail_point::hit();
    // SAFETY: `cb` and `opaque` are the pair the C caller passed to this function, per its
    // `# Safety` a valid completion; `cb` fires only from this sink, which `INVOKER_RESULTS` holds
    // under a fresh id and runs at most once, as a `FnOnce`, when the report for that id arrives.
    let callback_id = INVOKER_RESULTS.register(void_sink(move |_| unsafe { cb(opaque.get()) }));
    // SAFETY: Per this function's `# Safety`, `metadata` and `error` are null or live handles, as
    // `fire` requires.
    unsafe { registration.fire(metadata, error, callback_id) };
}

/// Frees a handle no function consumed; null is a no-op.
///
/// # Safety
///
/// `self_` must be null or a handle neither destroyed nor consumed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_producer_Callback_destroy(self_: *mut kafka_producer_Callback_t) {
    if !self_.is_null() {
        // SAFETY: `self_` is non-null (checked above) and, per this function's `# Safety`, a handle
        // neither destroyed nor consumed, i.e. the `Box::into_raw` of `kafka_producer_Callback_new`
        // no `take_callback` reclaimed, so `Box::from_raw` reclaims it exactly once.
        drop(unsafe { Box::from_raw(self_ as *mut CallbackRegistration) });
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;
    use crate::common::KafkaFuture;
    use crate::common::TopicPartition;
    use crate::ffi::common::fail_point;
    use crate::ffi::common::panic_probe::{Probe, on_opaque};
    use crate::ffi::common::{error_ref, kafka_common_Error_message};
    use crate::ffi::producer::kafka_producer_Producer__set_callback_result;
    use crate::ffi::producer::record_metadata::kafka_producer_RecordMetadata_offset;
    use crate::ffi::util::c_str_to_string;

    /// Each invocation's offset (-1 without metadata), error message and id.
    #[derive(Default)]
    struct Log(Mutex<Vec<(i64, Option<String>, i64)>>);

    impl Log {
        fn entries(&self) -> Vec<(i64, Option<String>)> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .map(|(offset, error, _)| (*offset, error.clone()))
                .collect()
        }

        fn last_id(&self) -> i64 {
            self.0.lock().unwrap().last().unwrap().2
        }
    }

    /// Logs the invocation; an invoker's (tagged) id is reported at once,
    /// with an error, which is allowed and ignored; a producer's is left to
    /// the test.
    unsafe extern "C" fn on_completion(
        self_: *mut c_void,
        metadata: *const kafka_producer_RecordMetadata_t,
        error: *const kafka_common_Error_t,
        callback_id: i64,
    ) {
        // SAFETY: Test code: `self_` is the test's `Log`, which outlives every invocation.
        let log = unsafe { &*(self_ as *const Log) };
        let offset = if metadata.is_null() {
            -1
        } else {
            // SAFETY: Test code: a non-null `metadata` is a live handle, borrowed for this call.
            unsafe { kafka_producer_RecordMetadata_offset(metadata) }
        };
        // SAFETY: Test code: a non-null `error` is a live handle, borrowed for this call; its
        // message is copied out.
        let error = (!error.is_null()).then(|| unsafe { c_str_to_string(kafka_common_Error_message(error)) });
        log.0.lock().unwrap().push((offset, error, callback_id));
        if CallbackResults::is_tagged(callback_id) {
            // SAFETY: Test code: `callback_id` is the pending id an invoker issued, so the function
            // does not read its producer view and null is fine; the error is an owned handle,
            // handed over.
            unsafe {
                kafka_producer_Producer__set_callback_result(
                    std::ptr::null(),
                    callback_id,
                    std::ptr::null_mut(),
                    box_error(Error::local_illegal_state("ignored")),
                )
            };
        }
    }

    /// The value of a completed future.
    fn resolved(future: KafkaFuture<RecordMetadata>) -> Result<RecordMetadata, Error> {
        assert!(future.is_done());
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future.get())
    }

    fn record_metadata(offset: i64) -> RecordMetadata {
        RecordMetadata::new(TopicPartition::new("t", 0), offset, 0, -1, -1, -1)
    }

    #[test]
    fn invoker_runs_the_implementation_inline_and_waits_for_the_report() {
        let log = Log::default();
        // Each invocation consumes its handle, as `on_completion` takes `self: Box<Self>`.
        let handle = || kafka_producer_Callback_new(&log as *const Log as *mut c_void, on_completion);
        let metadata = box_record_metadata(record_metadata(5));
        let error = box_error(Error::timeout("late"));
        // SAFETY: Test code: each handle comes from `kafka_producer_Callback_new` over the test's
        // `log`, which outlives every invocation, and is consumed by the one invocation it is
        // passed to; `metadata` and `error` are live handles until they are destroyed once at the
        // end.
        unsafe {
            kafka_producer_Callback_on_completion(handle(), metadata, std::ptr::null());
            kafka_producer_Callback_on_completion(handle(), std::ptr::null(), error);
        }
        assert_eq!(log.entries(), vec![(5, None), (-1, Some("late".to_string()))]);
        assert!(CallbackResults::is_tagged(log.last_id()), "an invoker's id is tagged");
        assert_eq!(
            // SAFETY: Test code: `error` is the live `box_error` handle above, destroyed only at
            // the end.
            unsafe { error_ref(error) }.error.message(),
            "late",
            "the invoker borrows its arguments"
        );

        let fired = Mutex::new(0);
        unsafe extern "C" fn cb(opaque: *mut c_void) {
            // SAFETY: Test code: `opaque` is `&fired`, alive until the test ends.
            *unsafe { &*(opaque as *const Mutex<i32>) }.lock().unwrap() += 1;
        }
        // SAFETY: Test code: the fresh handle is consumed by the call and `metadata` is still live;
        // `cb` matches the completion type and `&fired` outlives the call, which reports inline, so
        // `cb` has fired on return.
        unsafe {
            kafka_producer_Callback_on_completion_cb(
                handle(),
                metadata,
                std::ptr::null(),
                cb,
                &fired as *const _ as *mut c_void,
            );
        }
        assert_eq!(*fired.lock().unwrap(), 1);
        assert_eq!(log.entries().len(), 3);

        // SAFETY: Test code: each handle is destroyed once, the callback handle never consumed, and
        // destroying a null callback handle is a no-op.
        unsafe {
            kafka_producer_RecordMetadata_destroy(metadata);
            kafka_common_Error_destroy(error);
            kafka_producer_Callback_destroy(handle());
            kafka_producer_Callback_destroy(std::ptr::null_mut());
        }
    }

    /// A standalone callback has no callbacks vector, so a panic before
    /// `onCompletion` fires `cb` once on the calling thread, before the call
    /// returns, and the C method never runs.
    #[test]
    fn a_panicking_on_completion_cb_fires_cb_once_inline() {
        let log = Log::default();
        let handle = kafka_producer_Callback_new(&log as *const Log as *mut c_void, on_completion);
        let probe = Probe::default();
        fail_point::arm();
        // SAFETY: Test code: `handle` is a live handle this test made, consumed by the call before
        // the fail point; null metadata and error are allowed, and the probe outlives the call.
        unsafe {
            kafka_producer_Callback_on_completion_cb(
                handle,
                std::ptr::null(),
                std::ptr::null(),
                on_opaque,
                probe.as_opaque(),
            );
        }
        assert_eq!(probe.fired(), ["<no error>"]);
        assert!(log.entries().is_empty(), "onCompletion never ran");
    }

    #[test]
    fn a_queued_delivery_completes_its_future_only_once_reported() {
        let log = Log::default();
        let handle = kafka_producer_Callback_new(&log as *const Log as *mut c_void, on_completion);
        let queue = Arc::new(CallbackQueue::new());
        let results = Arc::new(CallbackResults::new());
        // SAFETY: Test code: `handle` is live and not used again; the closures hold copies of the
        // registration, and `log` outlives the queued invocations.
        let registration = unsafe { take_callback(handle) };

        // A record that succeeds: queued, not fired inline, and its future waits for the report,
        // whose error is only logged.
        let completion = KafkaFutureImpl::new();
        let callback = registration.into_callback(Arc::clone(&queue), Arc::clone(&results), completion.clone());
        callback.on_completion(Some(&record_metadata(9)), None);
        assert!(log.entries().is_empty(), "queued, not fired inline");
        assert_eq!(queue.execute(), 1);
        assert_eq!(log.entries(), vec![(9, None)]);
        let id = log.last_id();
        assert!(!CallbackResults::is_tagged(id), "a producer's id is its own");
        assert!(!completion.is_done(), "not done before the report");
        assert!(results.complete(id, std::ptr::null_mut(), Some(Error::local_illegal_state("thrown"))));
        assert_eq!(resolved(completion.future()).unwrap().offset(), 9);
        assert!(!results.complete(id, std::ptr::null_mut(), None), "a second report is ignored");

        // A record that fails: the future fails with the record's error, whatever the report.
        let completion = KafkaFutureImpl::new();
        let callback = registration.into_callback(Arc::clone(&queue), Arc::clone(&results), completion.clone());
        callback.on_completion(Some(&record_metadata(-1)), Some(&Error::timeout("expired")));
        assert_eq!(queue.execute(), 1);
        assert_eq!(log.entries()[1], (-1, Some("expired".to_string())));
        assert!(!completion.is_done());
        assert!(results.complete(log.last_id(), std::ptr::null_mut(), None));
        assert_eq!(resolved(completion.future()).unwrap_err().message(), "expired");

        // A closure dropped uncalled (a record rejected before the accumulator) registers nothing.
        drop(registration.into_callback(Arc::clone(&queue), Arc::clone(&results), KafkaFutureImpl::new()));
        assert!(results.drain().is_empty());
        assert_eq!(queue.execute(), 0);
    }

    /// The ids a round of deliveries is invoked with, stored without
    /// allocating.
    struct Ids {
        next: std::sync::atomic::AtomicUsize,
        ids: [std::sync::atomic::AtomicI64; 8],
    }

    unsafe extern "C" fn store_id(
        self_: *mut c_void,
        _metadata: *const kafka_producer_RecordMetadata_t,
        _error: *const kafka_common_Error_t,
        callback_id: i64,
    ) {
        // SAFETY: Test code: `self_` is the test's `Ids`, which outlives every invocation.
        let ids = unsafe { &*(self_ as *const Ids) };
        let slot = ids.next.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ids.ids[slot].store(callback_id, std::sync::atomic::Ordering::SeqCst);
    }

    /// The per-record cost of the C delivery path, from the record's
    /// completion to its report: the sink, the queued job, and the metadata
    /// handle with its topic `CString`, which the C callback borrows. Nothing
    /// else: no task, no copy of the record (CLAUDE.md §13). A pump costs one
    /// more, its snapshot of the queue, whatever the number of records. The
    /// record's future, which the report completes, adds what completing a
    /// fresh future costs on the platform: nothing where std's locks are
    /// futexes (Linux), one allocation per lock at its first use where they
    /// are pthread-backed (macOS: the future's mutex and its `Notify`'s).
    #[test]
    fn a_delivery_allocates_its_sink_job_and_metadata_handle_only() {
        const RECORDS: usize = 8;
        let ids = Ids { next: Default::default(), ids: Default::default() };
        let handle = kafka_producer_Callback_new(&ids as *const Ids as *mut c_void, store_id);
        // SAFETY: Test code: `handle` is live and not used again; `ids` outlives every invocation.
        let registration = unsafe { take_callback(handle) };
        let queue = Arc::new(CallbackQueue::new());
        let results = Arc::new(CallbackResults::new());
        let metadata = record_metadata(3);
        let complete_cost = {
            let fresh = KafkaFutureImpl::new();
            let value = metadata.clone();
            let _guard = crate::AllocTrackingGuard::new();
            fresh.complete(value);
            crate::AllocTrackingGuard::count()
        };
        let mut counts = Vec::new();
        for _ in 0..16 {
            ids.next.store(0, std::sync::atomic::Ordering::SeqCst);
            let completions: Vec<KafkaFutureImpl<RecordMetadata>> =
                (0..RECORDS).map(|_| KafkaFutureImpl::new()).collect();
            let mut callbacks: Vec<Box<dyn Callback>> = completions
                .iter()
                .map(|completion| {
                    registration.into_callback(Arc::clone(&queue), Arc::clone(&results), completion.clone())
                })
                .collect();
            let count = {
                let _guard = crate::AllocTrackingGuard::new();
                for callback in callbacks.drain(..) {
                    callback.on_completion(Some(&metadata), None);
                }
                assert_eq!(queue.execute(), RECORDS as i32);
                for id in &ids.ids {
                    let id = id.load(std::sync::atomic::Ordering::SeqCst);
                    assert!(results.complete(id, std::ptr::null_mut(), None));
                }
                crate::AllocTrackingGuard::count()
            };
            assert!(completions.iter().all(KafkaFutureImpl::is_done));
            counts.push(count);
        }
        // The first round grows the registry's slab and the queue's buffer.
        assert!(
            counts[1..].iter().all(|&count| count == (4 + complete_cost) * RECORDS + 1),
            "{counts:?}, completing a future costs {complete_cost}"
        );
    }
}

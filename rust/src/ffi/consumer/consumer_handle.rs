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

//! C bindings for the consumer's reentrancy handle
//! ([`ConsumerHandle`], Rust-only; consumer-threading.md §31, §41).
//!
//! A Java listener calls the consumer back by capturing the `consumer`
//! variable; the Rust listener trait takes only `&self`, so a listener
//! captures a `ConsumerHandle` instead. Its C view is obtained with
//! `kafka_consumer_Consumer_handle`, owned by the caller and independent of
//! the consumer's single-owner flag: it is usable while an operation is in
//! flight, which is exactly when a listener runs.
//!
//! # Threads
//!
//! A blocking handle operation may be called from inside a listener that a
//! blocking consumer operation invoked on the calling thread, i.e. from a
//! thread already inside the runtime's `block_on`; a nested `block_on`
//! there is forbidden by tokio. So every blocking handle operation spawns
//! its future as a task of the consumer, which destroying the class handle
//! joins, and waits for the result on a channel, whichever thread it is
//! called from; a panic on that task is returned as the operation's error,
//! as the header's panic contract states. A `_cb` handle operation
//! queues its completion on the consumer's callbacks vector like every
//! other `_cb`. The handle holds the consumer weakly: once the consumer is
//! destroyed its operations fail with `LocalIllegalState`, and a `_cb`
//! called then fires `cb` inline with that error (there is no vector left
//! to queue on).

#![expect(non_camel_case_types)]

use std::ffi::c_void;
use std::future::Future;
use std::sync::{Arc, Weak};

use crate::common::Error;
use crate::consumer::ConsumerHandle;
use crate::ffi::callback_queue::{CallbackJob, SendPtr};
use crate::ffi::common::topic_partition::{
    kafka_common_TopicPartition_t, list_topic_partitions, map_topic_partition_i64, sorted_topic_partition_list,
    topic_partition_i64_map, topic_partition_ref,
};
use crate::ffi::common::{
    box_error, catch_task_panic, deliver_panic, deliver_panic_i64, deliver_panic_value, kafka_common_Error_t,
};
use crate::ffi::common::{deliver_unit, deliver_value};
use crate::ffi::consumer::offset_and_metadata::{
    kafka_consumer_OffsetAndMetadata_t, map_offset_and_metadata, offset_and_metadata_map, offset_and_metadata_ref,
};
use crate::ffi::consumer::offset_and_timestamp::offset_and_timestamp_map;
use crate::ffi::consumer::offset_commit_callback::{commit_callback_adapter, kafka_consumer_OffsetCommitCallback_t};
use crate::ffi::consumer::{Client, INVOKE_ROUTE, InvokeRoute, deliver_i64, deliver_ptr, deliver_void, ms};
use crate::ffi::ffi_guard;
use crate::ffi::util::{kafka_List_t, kafka_Map_t, sorted_string_list};

/// Opaque handle to a [`ConsumerHandle`].
// Rust-only: a listener reenters the consumer through it (consumer-threading.md §31, §41), as a Java listener captures the consumer
#[doc(alias = "rust-only")]
#[repr(C)]
pub struct kafka_consumer_ConsumerHandle_t {
    _private: [u8; 0],
}

/// What a handle points at.
struct HandleInner {
    handle: ConsumerHandle,
    /// Weak, so a leftover handle never keeps the consumer alive past the
    /// class handle's destroy.
    client: Weak<Client>,
}

/// A new handle on `client`'s consumer, owned by the caller.
pub(crate) fn box_consumer_handle(client: &Arc<Client>) -> *mut kafka_consumer_ConsumerHandle_t {
    Box::into_raw(Box::new(HandleInner {
        handle: client.handle().clone(),
        client: Arc::downgrade(client),
    })) as *mut kafka_consumer_ConsumerHandle_t
}

/// What a handle points at.
///
/// # Safety
///
/// `self_` must be a live handle, i.e. one from `box_consumer_handle` not yet
/// destroyed, kept alive for `'a`.
unsafe fn inner<'a>(self_: *const kafka_consumer_ConsumerHandle_t) -> &'a HandleInner {
    // SAFETY: Per this function's `# Safety`, `self_` is a `Box<HandleInner>` from
    // `box_consumer_handle`, the only constructor, kept alive for `'a`; nothing takes a `&mut` to
    // it but `kafka_consumer_ConsumerHandle_destroy`, which no other call may overlap, so the
    // shared reference is valid for `'a`.
    unsafe { &*(self_ as *const HandleInner) }
}

fn destroyed() -> Error {
    Error::local_illegal_state("consumer destroyed")
}

impl HandleInner {
    /// Drives `future` as a tracked task of the consumer and waits for it
    /// (see the module docs for why not `block_on`). A panic inside
    /// `future` is returned as the
    /// [`panic_error`](crate::ffi::common::panic_error) of `fn_name`, the
    /// entry point; the consumer being destroyed before the task finishes
    /// is reported as `LocalIllegalState`.
    fn run_blocking<T, F>(&self, fn_name: &'static str, future: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: Future<Output = Result<T, Error>> + Send + 'static,
    {
        let Some(client) = self.client.upgrade() else {
            return Err(destroyed());
        };
        // The fail point is taken on the calling thread and fired inside the
        // task, so a test can make the task itself panic.
        #[cfg(test)]
        let future = {
            let armed = crate::ffi::common::fail_point::take();
            async move {
                crate::ffi::common::fail_point::hit_if(armed);
                future.await
            }
        };
        let (tx, rx) = std::sync::mpsc::channel::<Wait<T>>();
        // The interface methods the operation triggers (the commit callbacks
        // a handle commit drains) come back here and run on this thread, as
        // Java runs them nested inside the call (see `InvokeRoute`).
        let route = {
            let tx = tx.clone();
            InvokeRoute::Caller(Arc::new(move |job: CallbackJob| {
                // The waiter leaves only after `Done`, which this task sends
                // after its last job; run the job here should it be gone.
                if let Err(std::sync::mpsc::SendError(Wait::Job(job))) = tx.send(Wait::Job(job)) {
                    job();
                }
            }))
        };
        client.spawn(async move {
            let result = INVOKE_ROUTE.scope(route, catch_task_panic(fn_name, future)).await;
            let _ = tx.send(Wait::Done(result));
        });
        // Not kept alive while waiting: the handle holds the consumer weakly.
        drop(client);
        loop {
            match rx.recv() {
                Ok(Wait::Job(job)) => job(),
                Ok(Wait::Done(result)) => return result,
                Err(_) => return Err(destroyed()),
            }
        }
    }

    /// Drives `future` as a tracked task of the consumer and hands its
    /// result to `deliver`, which queues the completion; `fallback` fires
    /// the completion inline when the consumer is gone. A panic inside
    /// `future` is delivered as the
    /// [`panic_error`](crate::ffi::common::panic_error) of `fn_name`, the
    /// entry point.
    fn run_cb<T, F, D, B>(&self, fn_name: &'static str, future: F, deliver: D, fallback: B)
    where
        T: Send + 'static,
        F: Future<Output = Result<T, Error>> + Send + 'static,
        D: FnOnce(&Client, Result<T, Error>) + Send + 'static,
        B: FnOnce(Error),
    {
        #[cfg(test)]
        crate::ffi::common::fail_point::hit();
        match self.client.upgrade() {
            Some(client) => {
                let client2 = Arc::clone(&client);
                client.spawn(async move {
                    // The interface methods the operation triggers are queued
                    // with its completion (see `InvokeRoute`).
                    let result = INVOKE_ROUTE.scope(InvokeRoute::Queue, catch_task_panic(fn_name, future)).await;
                    deliver(&client2, result);
                });
            },
            None => fallback(destroyed()),
        }
    }
}

/// What the task of a blocking handle operation sends the waiting thread.
enum Wait<T> {
    /// An interface method the operation triggered, to run on that thread.
    Job(CallbackJob),
    /// The operation's result.
    Done(Result<T, Error>),
}

/// Where the guard of a `_cb` entry point delivers a caught panic: the
/// consumer's callbacks vector, or the calling thread once the consumer is
/// destroyed, as [`HandleInner::run_cb`] delivers every completion (see
/// [`deliver_panic`]).
///
/// # Safety
///
/// `self_` must be a live handle.
unsafe fn completion_sink(self_: *const kafka_consumer_ConsumerHandle_t) -> impl FnOnce(CallbackJob) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // the only use of the reference, as `inner` requires; the sink keeps the upgraded `Arc`, if
    // any, not the reference.
    let client = unsafe { inner(self_) }.client.upgrade();
    move |job| match client {
        Some(client) => client.delivery.queue.push(job),
        None => job(),
    }
}

/// `assign(Collection<TopicPartition>)` from inside a listener: a list
/// of `kafka_common_TopicPartition_t *`, copied. An empty list is
/// rejected (it would leave the group, which only the consumer itself
/// may do).
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assign(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking(
                "kafka_consumer_ConsumerHandle_assign",
                async move { h.assign(partitions).await },
            ),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_assign_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assign_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_assign_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_assign_cb",
        async move { h.assign(partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `seek(TopicPartition partition, long offset)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_seek_with_offset", async move {
                h.seek_with_offset(partition, offset).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_seek_with_offset_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    cb: kafka_consumer_ConsumerHandle_seek_with_offset_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_with_offset_cb",
        async move { h.seek_with_offset(partition, offset).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `seek(TopicPartition partition, OffsetAndMetadata offsetAndMetadata)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `offset_and_metadata` is a valid offset-and-metadata
    // handle, alive for this expression, as `offset_and_metadata_ref` requires; the value is
    // cloned.
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata", async move {
                h.seek_with_offset_and_metadata(partition, offset_and_metadata).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
    cb: kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    // SAFETY: Per this function's `# Safety`, `offset_and_metadata` is a valid offset-and-metadata
    // handle, alive for this expression, as `offset_and_metadata_ref` requires; the value is
    // cloned.
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb",
        async move { h.seek_with_offset_and_metadata(partition, offset_and_metadata).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `seekToBeginning(Collection<TopicPartition>)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_beginning(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_seek_to_beginning", async move {
                h.seek_to_beginning(&partitions).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_seek_to_beginning_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_beginning_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_seek_to_beginning_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_to_beginning_cb",
        async move { h.seek_to_beginning(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `seekToEnd(Collection<TopicPartition>)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_end(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_seek_to_end", async move {
                h.seek_to_end(&partitions).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_seek_to_end_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_end_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_seek_to_end_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_to_end_cb",
        async move { h.seek_to_end(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `pause(Collection<TopicPartition>)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_pause(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_pause", async move { h.pause(&partitions).await }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_pause_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_pause_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_pause_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_pause_cb",
        async move { h.pause(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `resume(Collection<TopicPartition>)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_resume(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking(
                "kafka_consumer_ConsumerHandle_resume",
                async move { h.resume(&partitions).await },
            ),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_resume_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_resume_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_resume_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_resume_cb",
        async move { h.resume(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `position(TopicPartition partition)`.
///
/// Blocking; callable from inside a listener. The value is returned; on failure
/// the error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let result = inner.run_blocking(
        "kafka_consumer_ConsumerHandle_position",
        async move { h.position(&partition).await },
    );
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the result
    // to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |v| v) }
}

/// The completion of the `_cb` twin: `value` on success, `-1` beside
/// an owned `error` otherwise.
pub type kafka_consumer_ConsumerHandle_position_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic_i64` requires.
    unsafe { deliver_panic_i64(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    cb: kafka_consumer_ConsumerHandle_position_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_position_cb",
        async move { h.position(&partition).await },
        move |client, result| deliver_i64(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(-1, box_error(error), opaque.get()) },
    )
}

/// `position(TopicPartition partition, Duration timeout)`: `timeout` in
/// milliseconds (negative fails as Java's `IllegalArgumentException`).
///
/// Blocking; callable from inside a listener. The value is returned; on failure
/// the error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_with_timeout(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    out_err: *mut *mut kafka_common_Error_t,
) -> i64 {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_position_with_timeout", async move {
        h.position_with_timeout(&partition, ms(timeout)?).await
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to
    // write the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |v| v) }
}

/// The completion of the `_cb` twin: `value` on success, `-1` beside
/// an owned `error` otherwise.
pub type kafka_consumer_ConsumerHandle_position_with_timeout_cb_t =
    unsafe extern "C" fn(value: i64, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic_i64` requires.
    unsafe { deliver_panic_i64(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_with_timeout_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    cb: kafka_consumer_ConsumerHandle_position_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partition` is a valid topic-partition handle, alive
    // for this expression, as `topic_partition_ref` requires; the value is cloned.
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_position_with_timeout_cb",
        async move { h.position_with_timeout(&partition, ms(timeout)?).await },
        move |client, result| deliver_i64(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(-1, box_error(error), opaque.get()) },
    )
}

/// `committed(Set<TopicPartition> partitions)`: an owned map of owned
/// `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndMetadata_t *`.
///
/// Blocking; callable from inside a listener. The owned value is returned; on
/// failure the error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_committed(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_committed", async move {
        h.committed(&partitions).await
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| offset_and_metadata_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_ConsumerHandle_committed_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_committed_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_committed_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_committed_cb",
        async move { h.committed(&partitions).await },
        move |client, result| deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_metadata_map(&offsets)),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `beginningOffsets(Collection<TopicPartition> partitions)`: an owned
/// map of owned `kafka_common_TopicPartition_t *` to owned `int64_t *`.
///
/// Blocking; callable from inside a listener. The owned value is returned; on
/// failure the error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_beginning_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_beginning_offsets", async move {
        h.beginning_offsets(&partitions).await
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write
    // the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_ConsumerHandle_beginning_offsets_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_beginning_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_beginning_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_beginning_offsets_cb",
        async move { h.beginning_offsets(&partitions).await },
        move |client, result| deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets)),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `endOffsets(Collection<TopicPartition> partitions)`: an owned map of
/// owned `kafka_common_TopicPartition_t *` to owned `int64_t *`.
///
/// Blocking; callable from inside a listener. The owned value is returned; on
/// failure the error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_end_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_end_offsets", async move {
        h.end_offsets(&partitions).await
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write the
    // result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| topic_partition_i64_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_ConsumerHandle_end_offsets_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_end_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_end_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `partitions` is null or a valid list of
    // `kafka_common_TopicPartition_t *`, as `list_topic_partitions` requires; the values are
    // cloned, so the borrow lasts for this call only.
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_end_offsets_cb",
        async move { h.end_offsets(&partitions).await },
        move |client, result| deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets)),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch)`: the
/// input maps `kafka_common_TopicPartition_t *` to `int64_t *`; the owned
/// result maps owned `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndTimestamp_t *`.
///
/// Blocking; callable from inside a listener. The owned value is returned; on
/// failure the error is stored in `*out_err`.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_offsets_for_times(
    self_: *const kafka_consumer_ConsumerHandle_t,
    timestamps_to_search: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut kafka_Map_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `timestamps_to_search` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `int64_t *`, as `map_topic_partition_i64` requires; the
    // entries are copied, so the borrow lasts for this call only.
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_offsets_for_times", async move {
        h.offsets_for_times(timestamps_to_search).await
    });
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid pointer to write
    // the result to, as `deliver_value` requires.
    unsafe { deliver_value(result, out_err, |offsets| offset_and_timestamp_map(&offsets)) }
}

/// The completion of the `_cb` twin: the owned `value` on success,
/// `NULL` beside an owned `error` otherwise.
pub type kafka_consumer_ConsumerHandle_offsets_for_times_cb_t =
    unsafe extern "C" fn(value: *mut kafka_Map_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_offsets_for_times_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    timestamps_to_search: *const kafka_Map_t,
    cb: kafka_consumer_ConsumerHandle_offsets_for_times_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `timestamps_to_search` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `int64_t *`, as `map_topic_partition_i64` requires; the
    // entries are copied, so the borrow lasts for this call only.
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_offsets_for_times_cb",
        async move { h.offsets_for_times(timestamps_to_search).await },
        move |client, result| {
            deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_timestamp_map(&offsets))
        },
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `commitSync()` from inside a listener (the canonical "flush offsets in
/// `onPartitionsRevoked`" use).
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync(
    self_: *const kafka_consumer_ConsumerHandle_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking(
                "kafka_consumer_ConsumerHandle_commit_sync",
                async move { h.commit_sync().await },
            ),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_commit_sync_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    cb: kafka_consumer_ConsumerHandle_commit_sync_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_sync_cb",
        async move { h.commit_sync().await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `commitSync(Map<TopicPartition, OffsetAndMetadata> offsets)`: a map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`,
/// copied.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and `offsets` null or a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one. `out_err` must be a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_with_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_commit_sync_with_offsets", async move {
                h.commit_sync_with_offsets(offsets).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    cb: kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb",
        async move { h.commit_sync_with_offsets(offsets).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `commitAsync()`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and the other parameters valid for their
/// documented types. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async(
    self_: *const kafka_consumer_ConsumerHandle_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking(
                "kafka_consumer_ConsumerHandle_commit_async",
                async move { h.commit_async().await },
            ),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_commit_async_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    cb: kafka_consumer_ConsumerHandle_commit_async_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_async_cb",
        async move { h.commit_async().await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets)`.
///
/// Blocking; callable from inside a listener. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and `offsets` null or a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one. `out_err` must be a valid slot
/// pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_commit_async_offsets", async move {
                h.commit_async_offsets(offsets).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_commit_async_offsets_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` is queued for
/// `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments comes
    // first and hands nothing over; the body hands `cb` over only as the last step of
    // `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing can panic,
    // or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its last
    // action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    cb: kafka_consumer_ConsumerHandle_commit_async_offsets_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_async_offsets_cb",
        async move { h.commit_async_offsets(offsets).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `commitAsync(OffsetCommitCallback callback)`: the callback
/// registration is copied during the call; its `self` must stay alive
/// until `onComplete` has fired (or the consumer is destroyed).
///
/// `onComplete` runs where the consumer runs its other commit callbacks, in
/// commit order: on the thread of the next blocking consumer or handle
/// operation that drains them (a later handle commit included, as Java's
/// `commit()` runs `executeCallbacks()` first), or queued for
/// `kafka_consumer_Consumer__execute_callbacks` by a `_cb` one. On a
/// `MockConsumer` handle the mock's consumed positions are committed and
/// `onComplete` runs before this call returns, on the calling thread, as
/// Java's `MockConsumer.commitAsync(callback)` invokes it inline.
///
/// Blocking; callable from inside a listener or an `onComplete`. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle and `callback` a live callback handle.
/// `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_with_callback(
    self_: *const kafka_consumer_ConsumerHandle_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    let Some(client) = inner.client.upgrade() else {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
        return unsafe { deliver_unit(Err(destroyed()), out_err) };
    };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    // Not kept alive while the operation runs: the handle holds the consumer weakly.
    drop(client);
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_commit_async_with_callback", async move {
                h.commit_async_with_callback(callback).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_commit_async_with_callback_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` and the `onComplete` calls the operation
/// triggers are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments and
    // building the adapter come first and hand nothing over; the body hands `cb` over only as the
    // last step of `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing
    // can panic, or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its
    // last action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_with_callback_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    cb: kafka_consumer_ConsumerHandle_commit_async_with_callback_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = inner
        .client
        .upgrade()
        .map(|client| unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) });
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_async_with_callback_cb",
        async move {
            match callback {
                Some(callback) => h.commit_async_with_callback(callback).await,
                None => Err(destroyed()),
            }
        },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback)`:
/// `offsets` is copied, and the callback registration too; its `self` must
/// stay alive until `onComplete` has fired (or the consumer is destroyed).
///
/// `onComplete` runs as for
/// [`kafka_consumer_ConsumerHandle_commit_async_with_callback`]. On a
/// `MockConsumer` handle the offsets are committed into the mock and
/// `onComplete` runs before this call returns, on the calling thread, as
/// Java's `MockConsumer.commitAsync` invokes it inline.
///
/// Blocking; callable from inside a listener or an `onComplete`. Fails with
/// `LocalIllegalState` once the consumer is destroyed.
///
/// # Safety
///
/// `self_` must be a live handle, `offsets` null or a valid map of
/// `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *`
/// that no other call mutates during this one, and `callback` a live callback
/// handle. `out_err` must be a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    out_err: *mut *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let Some(client) = inner.client.upgrade() else {
        // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
        return unsafe { deliver_unit(Err(destroyed()), out_err) };
    };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) };
    // Not kept alive while the operation runs: the handle holds the consumer weakly.
    drop(client);
    let h = inner.handle.clone();
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_unit` requires.
    unsafe {
        deliver_unit(
            inner.run_blocking("kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback", async move {
                h.commit_async_with_offsets_callback(offsets, callback).await
            }),
            out_err,
        )
    }
}

/// The completion of the `_cb` twin: `error` is `NULL` on success,
/// owned otherwise.
pub type kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb_t =
    unsafe extern "C" fn(error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The non-blocking twin: `cb` and the `onComplete` calls the operation
/// triggers (on a `MockConsumer` handle, this commit's own `onComplete`
/// first) are queued for `kafka_consumer_Consumer__execute_callbacks`.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, as `completion_sink`
    // requires. The guard runs this only when the body did not return. Reading the arguments and
    // building the adapter come first and hand nothing over; the body hands `cb` over only as the
    // last step of `HandleInner::run_cb`: to the task `Client::spawn` registers, after which nothing
    // can panic, or, once the consumer is destroyed, to the inline fallback, which fires `cb` as its
    // last action. So `cb` has not fired for this `opaque`, as `deliver_panic` requires.
    unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    callback: *mut kafka_consumer_OffsetCommitCallback_t,
    cb: kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, kept alive for this call,
    // as `inner` requires.
    let inner = unsafe { inner(self_) };
    // SAFETY: Per this function's `# Safety`, `offsets` is null or a valid map of
    // `kafka_common_TopicPartition_t *` to `kafka_consumer_OffsetAndMetadata_t *` that no other
    // call mutates during this one, as `map_offset_and_metadata` requires; the entries are copied.
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    // SAFETY: Per this function's `# Safety`, `callback` is a live callback handle, as
    // `commit_callback_adapter` requires; the registration is copied, so the handle is borrowed for
    // this call only. Its `self` stays alive until `onComplete` fires, as
    // `kafka_consumer_OffsetCommitCallback_new` documents.
    let callback = inner
        .client
        .upgrade()
        .map(|client| unsafe { commit_callback_adapter(callback, Arc::clone(client.delivery())) });
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb",
        async move {
            match callback {
                Some(callback) => h.commit_async_with_offsets_callback(offsets, callback).await,
                None => Err(destroyed()),
            }
        },
        move |client, result| deliver_void(client, cb, opaque, result),
        // SAFETY: `cb` and `opaque` are this call's own arguments, and `cb` is valid per this
        // function's `# Safety`. `run_cb` calls this fallback only when the consumer is gone and
        // nothing was spawned, so `deliver` never runs and `cb` fires here exactly once, with an
        // owned error.
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `assignment()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// sorted by topic and partition.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assignment(
    self_: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // the only use of the reference, as `inner` requires.
    sorted_topic_partition_list(unsafe { inner(self_) }.handle.assignment().iter())
}

/// `subscription()`: an owned list of owned `char *`, sorted.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_subscription(
    self_: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // the only use of the reference, as `inner` requires.
    sorted_string_list(unsafe { inner(self_) }.handle.subscription().iter())
}

/// `paused()`: an owned list of owned `kafka_common_TopicPartition_t *`,
/// sorted.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_paused(
    self_: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_List_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // the only use of the reference, as `inner` requires.
    sorted_topic_partition_list(unsafe { inner(self_) }.handle.paused().iter())
}

/// `wakeup()`: as `kafka_consumer_Consumer_wakeup`, from any thread.
///
/// # Safety
///
/// `self_` must be a live handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_wakeup(self_: *const kafka_consumer_ConsumerHandle_t) {
    // SAFETY: Per this function's `# Safety`, `self_` is a live handle, alive for this expression,
    // the only use of the reference, as `inner` requires.
    unsafe { inner(self_) }.handle.wakeup();
}

/// Frees the handle; null is a no-op. The consumer is unaffected.
///
/// # Safety
///
/// `self_` must be null or an owned handle not yet destroyed, that no other
/// call uses at the same time, not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_destroy(self_: *mut kafka_consumer_ConsumerHandle_t) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` (non-null, checked above) is an owned
        // handle not yet destroyed, i.e. a `Box<HandleInner>` from `box_consumer_handle`, that no
        // other call uses at the same time, so taking it back and dropping it frees it exactly
        // once. It holds the client weakly, so the consumer is unaffected.
        drop(unsafe { Box::from_raw(self_ as *mut HandleInner) });
    }
}

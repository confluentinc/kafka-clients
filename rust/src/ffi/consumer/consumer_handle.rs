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
//! its future as a task of the consumer, which `kafka_consumer_Consumer_destroy`
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
use crate::ffi::consumer::offset_and_metadata::{
    kafka_consumer_OffsetAndMetadata_t, map_offset_and_metadata, offset_and_metadata_map, offset_and_metadata_ref,
};
use crate::ffi::consumer::offset_and_timestamp::offset_and_timestamp_map;
use crate::ffi::consumer::{Client, deliver_i64, deliver_ptr, deliver_void, error_slot, ms, out_slot};
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
    /// Weak, so a leftover handle never keeps the consumer alive past
    /// `kafka_consumer_Consumer_destroy`.
    client: Weak<Client>,
}

/// A new handle on `client`'s consumer, owned by the caller.
pub(crate) fn box_consumer_handle(client: &Arc<Client>) -> *mut kafka_consumer_ConsumerHandle_t {
    Box::into_raw(Box::new(HandleInner {
        handle: client.handle().clone(),
        client: Arc::downgrade(client),
    })) as *mut kafka_consumer_ConsumerHandle_t
}

unsafe fn inner<'a>(self_: *const kafka_consumer_ConsumerHandle_t) -> &'a HandleInner {
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
        let (tx, rx) = std::sync::mpsc::channel();
        client.spawn(async move {
            let _ = tx.send(catch_task_panic(fn_name, future).await);
        });
        // Not kept alive while waiting: the handle holds the consumer weakly.
        drop(client);
        rx.recv().unwrap_or_else(|_| Err(destroyed()))
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
                    let result = catch_task_panic(fn_name, future).await;
                    deliver(&client2, result);
                });
            },
            None => fallback(destroyed()),
        }
    }
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assign(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    error_slot(inner.run_blocking(
        "kafka_consumer_ConsumerHandle_assign",
        async move { h.assign(partitions).await },
    ))
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_assign_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_assign_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_assign_cb",
        async move { h.assign(partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    error_slot(
        inner.run_blocking("kafka_consumer_ConsumerHandle_seek_with_offset", async move {
            h.seek_with_offset(partition, offset).await
        }),
    )
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset: i64,
    cb: kafka_consumer_ConsumerHandle_seek_with_offset_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_with_offset_cb",
        async move { h.seek_with_offset(partition, offset).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    let h = inner.handle.clone();
    error_slot(
        inner.run_blocking("kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata", async move {
            h.seek_with_offset_and_metadata(partition, offset_and_metadata).await
        }),
    )
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    offset_and_metadata: *const kafka_consumer_OffsetAndMetadata_t,
    cb: kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let offset_and_metadata = unsafe { offset_and_metadata_ref(offset_and_metadata) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb",
        async move { h.seek_with_offset_and_metadata(partition, offset_and_metadata).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_beginning(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    error_slot(
        inner.run_blocking("kafka_consumer_ConsumerHandle_seek_to_beginning", async move {
            h.seek_to_beginning(&partitions).await
        }),
    )
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_beginning_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_seek_to_beginning_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_to_beginning_cb",
        async move { h.seek_to_beginning(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_end(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    error_slot(inner.run_blocking("kafka_consumer_ConsumerHandle_seek_to_end", async move {
        h.seek_to_end(&partitions).await
    }))
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_seek_to_end_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_seek_to_end_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_seek_to_end_cb",
        async move { h.seek_to_end(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_pause(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    error_slot(inner.run_blocking("kafka_consumer_ConsumerHandle_pause", async move { h.pause(&partitions).await }))
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_pause_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_pause_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_pause_cb",
        async move { h.pause(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_resume(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    error_slot(inner.run_blocking(
        "kafka_consumer_ConsumerHandle_resume",
        async move { h.resume(&partitions).await },
    ))
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_resume_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_resume_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_resume_cb",
        async move { h.resume(&partitions).await },
        move |client, result| deliver_void(client, cb, opaque, result),
        |error| unsafe { cb(box_error(error), opaque.get()) },
    )
}

/// `position(TopicPartition partition)`.
///
/// Blocking; callable from inside a listener. The value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live handle, the other parameters valid for
/// their documented types and the `out_` pointer valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    out_position: *mut i64,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let result = inner.run_blocking(
        "kafka_consumer_ConsumerHandle_position",
        async move { h.position(&partition).await },
    );
    unsafe { out_slot(result, out_position, |v| v) }
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic_i64(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    cb: kafka_consumer_ConsumerHandle_position_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_position_cb",
        async move { h.position(&partition).await },
        move |client, result| deliver_i64(client, cb, opaque, result),
        |error| unsafe { cb(-1, box_error(error), opaque.get()) },
    )
}

/// `position(TopicPartition partition, Duration timeout)`: `timeout` in
/// milliseconds (negative fails as Java's `IllegalArgumentException`).
///
/// Blocking; callable from inside a listener. The value is delivered
/// through the trailing `out_` parameter, or the error returned.
///
/// # Safety
///
/// `self_` must be a live handle, the other parameters valid for
/// their documented types and the `out_` pointer valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_with_timeout(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    out_position_with_timeout: *mut i64,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_position_with_timeout", async move {
        h.position_with_timeout(&partition, ms(timeout)?).await
    });
    unsafe { out_slot(result, out_position_with_timeout, |v| v) }
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic_i64(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_position_with_timeout_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partition: *const kafka_common_TopicPartition_t,
    timeout: i64,
    cb: kafka_consumer_ConsumerHandle_position_with_timeout_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partition = unsafe { topic_partition_ref(partition) }.clone();
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_position_with_timeout_cb",
        async move { h.position_with_timeout(&partition, ms(timeout)?).await },
        move |client, result| deliver_i64(client, cb, opaque, result),
        |error| unsafe { cb(-1, box_error(error), opaque.get()) },
    )
}

/// `committed(Set<TopicPartition> partitions)`: an owned map of owned
/// `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndMetadata_t *`.
///
/// Blocking; callable from inside a listener. The owned value is
/// delivered through the trailing `out_` parameter, or the error
/// returned.
///
/// # Safety
///
/// `self_` must be a live handle, the other parameters valid for
/// their documented types and the `out_` pointer valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_committed(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_committed: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_committed", async move {
        h.committed(&partitions).await
    });
    unsafe { out_slot(result, out_committed, |offsets| offset_and_metadata_map(&offsets)) }
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_committed_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_committed_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_committed_cb",
        async move { h.committed(&partitions).await },
        move |client, result| deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_metadata_map(&offsets)),
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `beginningOffsets(Collection<TopicPartition> partitions)`: an owned
/// map of owned `kafka_common_TopicPartition_t *` to owned `int64_t *`.
///
/// Blocking; callable from inside a listener. The owned value is
/// delivered through the trailing `out_` parameter, or the error
/// returned.
///
/// # Safety
///
/// `self_` must be a live handle, the other parameters valid for
/// their documented types and the `out_` pointer valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_beginning_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_beginning_offsets: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_beginning_offsets", async move {
        h.beginning_offsets(&partitions).await
    });
    unsafe { out_slot(result, out_beginning_offsets, |offsets| topic_partition_i64_map(&offsets)) }
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_beginning_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_beginning_offsets_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_beginning_offsets_cb",
        async move { h.beginning_offsets(&partitions).await },
        move |client, result| deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets)),
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `endOffsets(Collection<TopicPartition> partitions)`: an owned map of
/// owned `kafka_common_TopicPartition_t *` to owned `int64_t *`.
///
/// Blocking; callable from inside a listener. The owned value is
/// delivered through the trailing `out_` parameter, or the error
/// returned.
///
/// # Safety
///
/// `self_` must be a live handle, the other parameters valid for
/// their documented types and the `out_` pointer valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_end_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    out_end_offsets: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_end_offsets", async move {
        h.end_offsets(&partitions).await
    });
    unsafe { out_slot(result, out_end_offsets, |offsets| topic_partition_i64_map(&offsets)) }
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_end_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    partitions: *const kafka_List_t,
    cb: kafka_consumer_ConsumerHandle_end_offsets_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let partitions = unsafe { list_topic_partitions(partitions) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_end_offsets_cb",
        async move { h.end_offsets(&partitions).await },
        move |client, result| deliver_ptr(client, cb, opaque, result, move |offsets| topic_partition_i64_map(&offsets)),
        |error| unsafe { cb(std::ptr::null_mut(), box_error(error), opaque.get()) },
    )
}

/// `offsetsForTimes(Map<TopicPartition, Long> timestampsToSearch)`: the
/// input maps `kafka_common_TopicPartition_t *` to `int64_t *`; the owned
/// result maps owned `kafka_common_TopicPartition_t *` to owned
/// `kafka_consumer_OffsetAndTimestamp_t *`.
///
/// Blocking; callable from inside a listener. The owned value is
/// delivered through the trailing `out_` parameter, or the error
/// returned.
///
/// # Safety
///
/// `self_` must be a live handle, the other parameters valid for
/// their documented types and the `out_` pointer valid.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_offsets_for_times(
    self_: *const kafka_consumer_ConsumerHandle_t,
    timestamps_to_search: *const kafka_Map_t,
    out_offsets_for_times: *mut *mut kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let h = inner.handle.clone();
    let result = inner.run_blocking("kafka_consumer_ConsumerHandle_offsets_for_times", async move {
        h.offsets_for_times(timestamps_to_search).await
    });
    unsafe { out_slot(result, out_offsets_for_times, |offsets| offset_and_timestamp_map(&offsets)) }
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic_value(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_offsets_for_times_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    timestamps_to_search: *const kafka_Map_t,
    cb: kafka_consumer_ConsumerHandle_offsets_for_times_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let timestamps_to_search = unsafe { map_topic_partition_i64(timestamps_to_search) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_offsets_for_times_cb",
        async move { h.offsets_for_times(timestamps_to_search).await },
        move |client, result| {
            deliver_ptr(client, cb, opaque, result, move |offsets| offset_and_timestamp_map(&offsets))
        },
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync(
    self_: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    error_slot(inner.run_blocking(
        "kafka_consumer_ConsumerHandle_commit_sync",
        async move { h.commit_sync().await },
    ))
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    cb: kafka_consumer_ConsumerHandle_commit_sync_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_sync_cb",
        async move { h.commit_sync().await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_with_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    error_slot(
        inner.run_blocking("kafka_consumer_ConsumerHandle_commit_sync_with_offsets", async move {
            h.commit_sync_with_offsets(offsets).await
        }),
    )
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    cb: kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb",
        async move { h.commit_sync_with_offsets(offsets).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async(
    self_: *const kafka_consumer_ConsumerHandle_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    error_slot(inner.run_blocking(
        "kafka_consumer_ConsumerHandle_commit_async",
        async move { h.commit_async().await },
    ))
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    cb: kafka_consumer_ConsumerHandle_commit_async_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_async_cb",
        async move { h.commit_async().await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
/// `self_` must be a live handle and the other parameters valid for
/// their documented types.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_offsets(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
) -> *mut kafka_common_Error_t {
    let inner = unsafe { inner(self_) };
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    error_slot(
        inner.run_blocking("kafka_consumer_ConsumerHandle_commit_async_offsets", async move {
            h.commit_async_offsets(offsets).await
        }),
    )
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
#[ffi_guard(on_panic = |err| unsafe { deliver_panic(completion_sink(self_), cb, opaque, err) })]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_commit_async_offsets_cb(
    self_: *const kafka_consumer_ConsumerHandle_t,
    offsets: *const kafka_Map_t,
    cb: kafka_consumer_ConsumerHandle_commit_async_offsets_cb_t,
    opaque: *mut c_void,
) {
    let inner = unsafe { inner(self_) };
    let offsets = unsafe { map_offset_and_metadata(offsets) };
    let h = inner.handle.clone();
    let opaque = SendPtr(opaque);
    inner.run_cb(
        "kafka_consumer_ConsumerHandle_commit_async_offsets_cb",
        async move { h.commit_async_offsets(offsets).await },
        move |client, result| deliver_void(client, cb, opaque, result),
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
    unsafe { inner(self_) }.handle.wakeup();
}

/// Frees the handle; null is a no-op. The consumer is unaffected.
///
/// # Safety
///
/// `self_` must be null or a live handle not used afterwards.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_consumer_ConsumerHandle_destroy(self_: *mut kafka_consumer_ConsumerHandle_t) {
    if !self_.is_null() {
        drop(unsafe { Box::from_raw(self_ as *mut HandleInner) });
    }
}

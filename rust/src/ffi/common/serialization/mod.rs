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

//! `kafka_common_serialization_*`: `org.apache.kafka.common.serialization`
//! (CLAUDE.md §4).
//!
//! The `Serializer` and `Deserializer` interfaces are generic in Java; their
//! `T` crosses the boundary as a `void *` (§4, "Generic types"), which Rust
//! holds as a [`GenericValue`] and never reads. What the pointer means is the
//! business of the implementation on each side:
//!
//!   - a C implementation registered with `Serializer_new` /
//!     `Deserializer_new` decides it, and a `void *` a C deserializer
//!     produces is owned by the C side, never freed by Rust;
//!   - each built-in class documents its representation: `StringSerializer`
//!     reads a NUL-terminated `const char *`, `ByteArraySerializer` a
//!     `const kafka_Bytes_t *`; `StringDeserializer` returns an owned `char *`
//!     freed with `kafka_string_destroy`, `ByteArrayDeserializer` and
//!     `BytesDeserializer` an owned `kafka_Bytes_t *` freed with
//!     `kafka_Bytes_destroy`.
//!
//! Every built-in class is a handle ([`SerializerHandle`] /
//! [`DeserializerHandle`]) that hands out a borrowed view of its interface
//! through `__as_Serializer` / `__as_Deserializer` (§4, "Traits"), valid as
//! long as the class handle and never passed to the interface's `_destroy`.
//! Passing the view where a client consumes a `*mut` interface handle shares
//! the implementation and leaves the class handle valid.
//!
//! `Deserializer` has `&mut self` methods (`configure`, `close`), so its
//! handle guards the implementation with a mutex and `__as_Deserializer`
//! returns `*mut`; `Serializer` has none and its view is `*const`. The mutex
//! is Tokio's: a consumer holds it across the await of the deserializer's
//! future.
//!
//! A C implementation is registered in one of two modes:
//!
//!   - `_new` takes the synchronous `_fn_t`s, which return their result. A
//!     client calls them on Tokio's blocking pool and awaits the task, so a
//!     slow implementation holds no runtime worker; the invokers call them on
//!     their calling thread.
//!   - `_new_async` takes the `_async_fn_t`s, which return `void` and report
//!     their result later, from any thread, through the handle's
//!     `__set_callback_result` with the `callback_id` they were given. The
//!     handle keeps the pending calls in its own [`CallbackResults`], and a
//!     client awaits the report without holding any thread.
//!
//! The built-ins are always-ready futures; the invokers drive every future on
//! the calling thread, parking it while a report is awaited
//! ([`block_on_thread`]), and [`ready`] drives the built-ins.

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};
use std::pin::{Pin, pin};
use std::sync::{Arc, OnceLock};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::{self, Thread};

use bytes::Bytes;
use futures_util::FutureExt;

use crate::common::Error;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::callback_result::{CallbackResults, KeepAlive, ResultSink, WaitReport};
use crate::ffi::common::header::internals::record_headers::RecordHeadersInner;
use crate::ffi::common::panic_error;
use crate::ffi::common::serialization::deserializer::{
    DeserializerInner, FfiDeserializer, kafka_common_serialization_Deserializer_t,
};
use crate::ffi::common::serialization::serializer::{
    FfiSerializer, SerializerInner, kafka_common_serialization_Serializer_t,
};
use crate::ffi::util::{
    box_string_keyed_map, c_str_to_string, destroy_string_element, into_c_string, kafka_Map_destroy, kafka_Map_t,
    map_entries,
};

pub(crate) mod byte_array_deserializer;
pub(crate) mod byte_array_serializer;
pub(crate) mod bytes_deserializer;
pub(crate) mod deserializer;
pub(crate) mod serializer;
pub(crate) mod string_deserializer;
pub(crate) mod string_serializer;

/// What a serializer class handle points at: the implementation over the
/// `void *` representation, shared with every client it was handed to, plus
/// the `Serializer` view it hands out.
pub(crate) struct SerializerHandle {
    imp: Arc<FfiSerializer>,
    as_serializer: OnceLock<Arc<SerializerInner>>,
}

impl SerializerHandle {
    /// Hands `imp` to C as an owned handle.
    pub(crate) fn boxed(imp: FfiSerializer) -> *mut Self {
        Box::into_raw(Box::new(Self { imp: Arc::new(imp), as_serializer: OnceLock::new() }))
    }

    /// The handle behind a pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this class, alive for `'a`.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        // SAFETY: Per this function's `# Safety`, `ptr` is a valid handle of this class alive for
        // `'a`: the `Box::into_raw` of `boxed`, not yet passed to `destroy`. A `SerializerHandle`
        // is changed only through its `OnceLock`, which allows shared access, so the shared
        // reference is sound for `'a`.
        unsafe { &*ptr }
    }

    /// The implementation as a `Serializer`: a view valid as long as the
    /// handle.
    pub(crate) fn as_serializer(&self) -> *const kafka_common_serialization_Serializer_t {
        self.as_serializer
            .get_or_init(|| SerializerInner::view(Arc::clone(&self.imp)))
            .as_ptr()
    }

    /// Frees an owned handle; null is a no-op.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle not yet destroyed.
    pub(crate) unsafe fn destroy(ptr: *mut Self) {
        if !ptr.is_null() {
            // SAFETY: Per this function's `# Safety`, `ptr` is an owned handle not yet destroyed,
            // so it is the `Box::into_raw` of `boxed` and is reclaimed exactly once here; a view
            // handed out by `as_serializer` points into the handle and is never passed here.
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

/// What a deserializer class handle points at: the implementation over the
/// `void *` representation behind the mutex its `&mut self` methods need,
/// shared with every client it was handed to, plus the `Deserializer` view
/// it hands out.
pub(crate) struct DeserializerHandle {
    imp: Arc<tokio::sync::Mutex<FfiDeserializer>>,
    as_deserializer: OnceLock<Arc<DeserializerInner>>,
}

impl DeserializerHandle {
    /// Hands `imp` to C as an owned handle.
    pub(crate) fn boxed(imp: FfiDeserializer) -> *mut Self {
        Box::into_raw(Box::new(Self {
            imp: Arc::new(tokio::sync::Mutex::new(imp)),
            as_deserializer: OnceLock::new(),
        }))
    }

    /// The handle behind a pointer.
    ///
    /// # Safety
    ///
    /// `ptr` must be a valid handle of this class, alive for `'a`.
    pub(crate) unsafe fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        // SAFETY: Per this function's `# Safety`, `ptr` is a valid handle of this class alive for
        // `'a`: the `Box::into_raw` of `boxed`, not yet passed to `destroy`. A `DeserializerHandle`
        // is changed only through its `OnceLock` and the `Mutex` around the implementation, which
        // both allow shared access, so the shared reference is sound for `'a`.
        unsafe { &*ptr }
    }

    /// The implementation as a `Deserializer`: a view valid as long as the
    /// handle, `*mut` because the trait mutates. The view itself is never
    /// written through: the invokers lock the mutex behind it.
    pub(crate) fn as_deserializer(&self) -> *mut kafka_common_serialization_Deserializer_t {
        self.as_deserializer
            .get_or_init(|| DeserializerInner::view(Arc::clone(&self.imp)))
            .as_ptr()
            .cast_mut()
    }

    /// Frees an owned handle; null is a no-op.
    ///
    /// # Safety
    ///
    /// `ptr` must be null or an owned handle not yet destroyed.
    pub(crate) unsafe fn destroy(ptr: *mut Self) {
        if !ptr.is_null() {
            // SAFETY: Per this function's `# Safety`, `ptr` is an owned handle not yet destroyed,
            // so it is the `Box::into_raw` of `boxed` and is reclaimed exactly once here; a view
            // handed out by `as_deserializer` points into the handle and is never passed here.
            drop(unsafe { Box::from_raw(ptr) });
        }
    }
}

/// The output of a serde future that a blocking invoker drives on the
/// calling thread, polled once.
///
/// Every serde a C caller can reach completes in its first poll (the C
/// implementations are called synchronously, the built-ins never wait), so a
/// pending future is reported as an error rather than waited for.
pub(crate) fn ready<F: Future>(future: F) -> Result<F::Output, Error> {
    future
        .now_or_never()
        .ok_or_else(|| Error::local_illegal_state("The serde did not complete synchronously on the calling thread"))
}

/// Wakes the thread that parked waiting for a lock.
struct ThreadWaker(Thread);

impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

/// Locks `mutex` from a blocking invoker, parking the calling thread while
/// another holder (a consumer deserializing a record) has it, as the
/// `std::sync::Mutex` it replaces did. Needs no runtime; an uncontended lock
/// allocates nothing.
pub(crate) fn lock_blocking<T: ?Sized>(mutex: &tokio::sync::Mutex<T>) -> tokio::sync::MutexGuard<'_, T> {
    if let Ok(guard) = mutex.try_lock() {
        return guard;
    }
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut lock = pin!(mutex.lock());
    loop {
        if let Poll::Ready(guard) = lock.as_mut().poll(&mut context) {
            return guard;
        }
        thread::park();
    }
}

/// Drives `future` to completion on the calling thread, parking it while
/// the future waits (for an asynchronous C implementation's report). Needs no
/// runtime; a future ready at its first poll allocates nothing.
pub(crate) fn block_on_thread<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    if let Poll::Ready(output) = future.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        return output;
    }
    let waker = Waker::from(Arc::new(ThreadWaker(thread::current())));
    let mut context = Context::from_waker(&waker);
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        thread::park();
    }
}

/// Where a synchronous C implementation (`_new`) runs.
///
/// Has no Java counterpart (`definition-of-done.md` §7): Java calls a
/// serializer on the calling thread, which may block; a Rust future must not
/// block its runtime worker, so a client moves the call to the blocking pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Dispatch {
    /// On Tokio's blocking pool, awaited; on the calling thread when there is
    /// no runtime. What a client's future does.
    Blocking,
    /// On the calling thread. What the blocking invokers do.
    Inline,
}

/// The arguments an asynchronous C implementation (`_new_async`) reads until
/// it reports: one per pending call, kept by the handle's
/// [`CallbackResults`] slot and reused by the slot's next call, so a call
/// allocates nothing once the buffers have grown.
///
/// Has no Java counterpart (`definition-of-done.md` §7): Java's arguments are
/// garbage collected; here the slot keeps them alive until the report, even
/// when the client's future was dropped first.
#[derive(Default)]
pub(crate) struct CallArgs {
    /// The topic, NUL-terminated, truncated at an interior NUL.
    topic: Vec<u8>,
    /// A copy of the record's headers, for the methods that take them.
    headers: Option<RecordHeadersInner>,
    /// The record's bytes, for a deserializer: a refcounted slice of the
    /// fetch buffer, or `copy` when the method received a plain slice.
    data: Bytes,
    /// The copy of a slice argument, its capacity reused.
    copy: Vec<u8>,
    /// The owned map `configure` reads.
    configs: Option<SendPtr>,
    /// A serializer's reported bytes, copied on the reporting thread.
    pub(crate) output: Option<Vec<u8>>,
}

impl CallArgs {
    /// Stores `topic` and returns the NUL-terminated pointer C reads.
    pub(crate) fn set_topic(&mut self, topic: &str) -> *const std::ffi::c_char {
        let bytes = topic.as_bytes();
        let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
        self.topic.clear();
        self.topic.extend_from_slice(&bytes[..end]);
        self.topic.push(0);
        self.topic.as_ptr().cast()
    }

    /// Stores a copy of `headers` and returns the handle C reads (and, for a
    /// serializer, may modify), at the stable address of the boxed `CallArgs`.
    pub(crate) fn set_headers(
        &mut self,
        headers: &crate::common::header::RecordHeaders,
    ) -> *mut crate::ffi::common::header::internals::record_headers::kafka_common_header_internals_RecordHeaders_t {
        self.headers.insert(RecordHeadersInner::new(headers.clone())).as_mut_ptr()
    }

    /// Stores `data` and returns the view C reads.
    pub(crate) fn set_data(&mut self, data: Data<'_>) -> crate::ffi::util::kafka_Bytes_t {
        match data {
            Data::Slice(slice) => {
                self.copy.clear();
                self.copy.extend_from_slice(slice);
                crate::ffi::util::kafka_Bytes_t::from_slice(&self.copy)
            },
            Data::Bytes(bytes) => {
                self.data = bytes.clone();
                crate::ffi::util::kafka_Bytes_t::from_slice(&self.data)
            },
        }
    }

    /// Takes over `configs`, destroyed when the call is released.
    pub(crate) fn set_configs(&mut self, configs: *mut kafka_Map_t) {
        self.destroy_configs();
        self.configs = Some(SendPtr(configs.cast()));
    }

    fn destroy_configs(&mut self) {
        if let Some(configs) = self.configs.take() {
            // SAFETY: `configs` is the owned map `set_configs` took over, not yet destroyed: it is
            // taken out of the field here, so it is destroyed once. The C implementation read it only
            // until it reported, which is before the slot is released.
            unsafe { kafka_Map_destroy(configs.get().cast()) };
        }
    }
}

impl KeepAlive for CallArgs {
    fn clear(&mut self) {
        self.headers = None;
        self.data = Bytes::new();
        self.output = None;
        self.destroy_configs();
    }
}

impl Drop for CallArgs {
    fn drop(&mut self) {
        self.destroy_configs();
    }
}

/// The record bytes a deserializer method received.
#[derive(Clone, Copy)]
pub(crate) enum Data<'a> {
    /// A plain slice, copied when the call outlives it.
    Slice(&'a [u8]),
    /// A refcounted slice of the fetch buffer, shared, never copied.
    Bytes(&'a Bytes),
}

impl Data<'_> {
    /// The bytes.
    pub(crate) fn as_slice(&self) -> &[u8] {
        match self {
            Data::Slice(slice) => slice,
            Data::Bytes(bytes) => bytes,
        }
    }

    /// An owned buffer for a call on the blocking pool.
    pub(crate) fn to_bytes(self) -> Bytes {
        match self {
            Data::Slice(slice) => Bytes::copy_from_slice(slice),
            Data::Bytes(bytes) => bytes.clone(),
        }
    }
}

/// The registry of an asynchronous C implementation's pending calls.
pub(crate) type Results = CallbackResults<CallArgs>;

/// The future of a C implementation's call.
///
/// Has no Java counterpart (`definition-of-done.md` §7): a hand-written
/// future rather than an `async` block, so that a serde's future stays small
/// enough for `InlineFuture`.
pub(crate) enum Call<'a, R> {
    /// The result, known when the call was made (an inline call). The error
    /// is boxed so that it does not size every pending call: a failure
    /// allocates, a success does not.
    Done(Option<Result<R, Box<Error>>>),
    /// A synchronous implementation running on the blocking pool.
    Task(tokio::task::JoinHandle<Result<R, Error>>),
    /// An asynchronous implementation's pending report.
    Wait(WaitReport<'a, CallArgs, R>),
}

impl<R: Unpin> Future for Call<'_, R> {
    type Output = Result<R, Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Call::Done(result) => Poll::Ready(match result.take() {
                Some(result) => result.map_err(|error| *error),
                None => Err(Error::local_illegal_state("The serde call was already completed")),
            }),
            Call::Task(task) => Pin::new(task).poll(cx).map(|joined| match joined {
                Ok(result) => result,
                Err(error) => Err(match error.try_into_panic() {
                    Ok(payload) => panic_error("a C serde's blocking call", payload.as_ref()),
                    Err(_) => Error::local_illegal_state("A C serde's blocking call was cancelled"),
                }),
            }),
            Call::Wait(wait) => Pin::new(wait).poll(cx),
        }
    }
}

/// Runs `call` where `dispatch` says: inline, or on the blocking pool when a
/// runtime is there.
pub(crate) fn dispatch<'a, R: Send + 'static>(
    dispatch: Dispatch,
    call: impl FnOnce() -> Result<R, Error> + Send + 'static,
) -> Call<'a, R> {
    match (dispatch, tokio::runtime::Handle::try_current()) {
        (Dispatch::Blocking, Ok(handle)) => Call::Task(handle.spawn_blocking(call)),
        _ => Call::Done(Some(call().map_err(Box::new))),
    }
}

/// A `void *` the C side owns, carried to the blocking pool.
pub(crate) fn send_ptr<T>(ptr: *const T) -> SendPtr {
    SendPtr(ptr.cast_mut().cast::<c_void>())
}

/// The owned map a C `configure` reads: sorted by key, since Java's map is
/// unordered and index addressing through the C map is not.
pub(crate) fn build_configs(configs: &HashMap<String, String>) -> *mut kafka_Map_t {
    let sorted: BTreeMap<&str, &str> = configs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    box_string_keyed_map(
        sorted.into_iter().map(|(k, v)| (k, into_c_string(v) as *mut c_void)),
        Some(destroy_string_element),
    )
}

/// The configs a C caller passed, copied.
///
/// # Safety
///
/// `configs` must be null or a valid map of NUL-terminated strings.
pub(crate) unsafe fn read_configs(configs: *const kafka_Map_t) -> HashMap<String, String> {
    // SAFETY: Per this function's `# Safety`, `configs` is null or a valid map, as `map_entries`
    // requires (null reads as empty); the borrow of its entries ends when they are collected.
    unsafe { map_entries(configs) }
        .iter()
        .map(|&(k, v)| {
            // SAFETY: Per this function's `# Safety`, every key and value of `configs` is a valid
            // NUL-terminated string, as `c_str_to_string` requires; both are copied.
            (unsafe { c_str_to_string(k as *const c_char) }, unsafe {
                c_str_to_string(v as *const c_char)
            })
        })
        .collect()
}

/// Logs the failure of a `void` method (`configure` or `close`) of the
/// `serde` (`"serializer"` or `"deserializer"`), which has nowhere to report
/// it.
pub(crate) fn log_failure(what: &str, serde: &str, result: Result<(), Error>) {
    if let Err(error) = result {
        log::warn!("Failed to {what} the {serde}: {error}");
    }
}

/// Completion of the `void` `_cb` twins.
pub(crate) type OpaqueCb = unsafe extern "C" fn(opaque: *mut c_void);

/// Fires a `void` completion, logging a failure.
pub(crate) fn opaque_sink(what: &'static str, serde: &'static str, cb: OpaqueCb, opaque: *mut c_void) -> ResultSink {
    let opaque = SendPtr(opaque);
    Box::new(move |result| {
        log_failure(what, serde, result.map(|_| ()));
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to the `_cb` twin, a valid
        // completion until it fires; this `FnOnce` fires it once.
        unsafe { cb(opaque.get()) }
    })
}

#[cfg(test)]
mod tests {
    use std::ptr;

    use super::*;
    use crate::ffi::common::serialization::deserializer::{deserializer_ref, take_deserializer};
    use crate::ffi::common::serialization::serializer::{serializer_ref, take_serializer};
    use crate::ffi::common::serialization::string_deserializer::GenericStringDeserializer;
    use crate::ffi::common::serialization::string_serializer::GenericStringSerializer;

    #[test]
    fn serializer_views_are_cached_and_share_the_implementation() {
        let handle = SerializerHandle::boxed(FfiSerializer::String(GenericStringSerializer::default()));
        // SAFETY: Test code: `handle` comes from `boxed` and is destroyed once, at the end, after
        // the last use of `h` and of the views; the view passed to `take_serializer` and
        // `serializer_ref` comes from `as_serializer` and is not owned, so `take_serializer` clones
        // its `Arc` and frees nothing.
        unsafe {
            let h = SerializerHandle::from_ptr(handle);
            assert_eq!(h.as_serializer(), h.as_serializer());
            assert_eq!(Arc::strong_count(&h.imp), 2, "handle + view");
            // Taking the view shares it: the handle keeps working and the
            // taken `Arc` is the view itself, holding the same implementation.
            let shared = take_serializer(h.as_serializer().cast_mut());
            assert_eq!(Arc::strong_count(&h.imp), 2);
            assert!(ptr::addr_eq(Arc::as_ptr(&shared), h.as_serializer()));
            assert!(ptr::addr_eq(shared.get(), Arc::as_ptr(&h.imp)));
            assert_eq!(serializer_ref(h.as_serializer()).as_ptr(), h.as_serializer());
            drop(shared);
            SerializerHandle::destroy(handle);
            SerializerHandle::destroy(ptr::null_mut());
        }
    }

    #[test]
    fn deserializer_views_are_cached_and_share_the_implementation() {
        let handle = DeserializerHandle::boxed(FfiDeserializer::String(GenericStringDeserializer::default()));
        // SAFETY: Test code: `handle` comes from `boxed` and is destroyed once, at the end, after
        // the last use of `h` and of the views; the view passed to `take_deserializer` and
        // `deserializer_ref` comes from `as_deserializer` and is not owned, so `take_deserializer`
        // clones its `Arc` and frees nothing.
        unsafe {
            let h = DeserializerHandle::from_ptr(handle);
            assert_eq!(h.as_deserializer(), h.as_deserializer());
            assert_eq!(Arc::strong_count(&h.imp), 2, "handle + view");
            let shared = take_deserializer(h.as_deserializer());
            assert_eq!(Arc::strong_count(&h.imp), 2);
            assert!(ptr::addr_eq(Arc::as_ptr(&shared), h.as_deserializer()));
            assert!(ptr::addr_eq(shared.imp(), Arc::as_ptr(&h.imp)));
            assert_eq!(deserializer_ref(h.as_deserializer()).as_ptr(), h.as_deserializer().cast_const());
            drop(shared);
            DeserializerHandle::destroy(handle);
            DeserializerHandle::destroy(ptr::null_mut());
        }
    }

    #[test]
    fn ready_answers_a_ready_future_and_fails_a_pending_one() {
        assert_eq!(7, ready(async { 7 }).unwrap());
        let error = ready(std::future::pending::<()>()).unwrap_err();
        assert!(error.is_local_illegal_state_error());
        assert_eq!(
            "The serde did not complete synchronously on the calling thread",
            error.message()
        );
    }

    #[test]
    fn lock_blocking_waits_for_the_holder() {
        let mutex = Arc::new(tokio::sync::Mutex::new(0));
        *lock_blocking(&mutex) += 1;
        let guard = mutex.clone().try_lock_owned().unwrap();
        let (locked_tx, locked_rx) = std::sync::mpsc::channel();
        let waiter = {
            let mutex = Arc::clone(&mutex);
            thread::spawn(move || {
                let mut value = lock_blocking(&mutex);
                *value += 1;
                locked_tx.send(*value).unwrap();
            })
        };
        assert!(
            locked_rx.recv_timeout(std::time::Duration::from_millis(50)).is_err(),
            "the waiter must not get the lock while it is held"
        );
        drop(guard);
        assert_eq!(2, locked_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap());
        waiter.join().unwrap();
    }
}

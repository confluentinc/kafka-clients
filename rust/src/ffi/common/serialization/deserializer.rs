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

//! `kafka_common_serialization_Deserializer_t`: the
//! `org.apache.kafka.common.serialization.Deserializer<T>` interface
//! (CLAUDE.md §4, "Traits").
//!
//! `T` is a `void *` (§4, "Generic types"): the functions return whatever
//! the implementation produces. A `void *` a C implementation
//! produces is owned by the C side and never freed by Rust; what a built-in
//! class produces is documented by that class. The `kafka_Bytes_t` arrays an
//! implementation receives are borrowed until it returns (synchronous mode)
//! or reports (asynchronous mode), and `data == NULL` is read as an empty
//! array: the Rust trait takes a slice, never Java's `null`.
//!
//! `configure` and `close` take `&mut self`, so a handle guards its
//! implementation with a Tokio mutex, and the two invokers take a `*mut`
//! handle.
//!
//! A C implementation is registered synchronous
//! ([`kafka_common_serialization_Deserializer_new`]: a consumer calls it on
//! Tokio's blocking pool) or asynchronous
//! ([`kafka_common_serialization_Deserializer_new_async`]: it reports through
//! [`kafka_common_serialization_Deserializer__set_callback_result`]). The
//! blocking invokers call a synchronous one on their calling thread and park
//! that thread until an asynchronous one reports; the `_cb` twins fire their
//! completion on the calling thread, or, for an asynchronous implementation,
//! on the thread that reports.

#![expect(non_camel_case_types)]

use std::collections::{BTreeMap, HashMap};
use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::Arc;

use bytes::Bytes;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Deserializer;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::callback_result::{ResultSink, WaitReport};
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::serialization::byte_array_deserializer::GenericByteArrayDeserializer;
use crate::ffi::common::serialization::bytes_deserializer::GenericBytesDeserializer;
use crate::ffi::common::serialization::string_deserializer::GenericStringDeserializer;
use crate::ffi::common::serialization::{
    Call, CallArgs, Data, Dispatch, Results, block_on_thread, dispatch, lock_blocking, ready, send_ptr,
};
use crate::ffi::common::{
    box_error, deliver_panic_opaque, deliver_panic_value, deliver_value, kafka_common_Error_t, run_inline, take_error,
};
use crate::ffi::ffi_guard;
use crate::ffi::util::{
    GenericValue, box_string_keyed_map, c_str_to_string, destroy_string_element, into_c_string, kafka_Bytes_t,
    kafka_Map_destroy, kafka_Map_t, map_entries, owned_c_string,
};

/// Opaque handle to a [`Deserializer`] implementation over the `void *`
/// representation.
#[repr(C)]
pub struct kafka_common_serialization_Deserializer_t {
    _private: [u8; 0],
}

/// Every deserializer a C handle can stand for: a C implementation or a
/// built-in class. A `NULL` handle has none: the consumer boxes its
/// `BytesPassthroughDeserializer` directly.
///
/// Has no Java counterpart (`definition-of-done.md` §7): it replaces a
/// `dyn Deserializer` so that each call awaits the variant's concrete
/// future, which keeps the future of a wrapper around it (the consumer's
/// `ArcDeserializer`) small enough to be stored inline by `DynDeserializer`.
/// Every handle points at one behind a [`tokio::sync::Mutex`], which its
/// `&mut self` methods (`configure`, `close`) need.
pub(crate) enum FfiDeserializer {
    /// A C implementation registered with
    /// [`kafka_common_serialization_Deserializer_new`] or
    /// [`kafka_common_serialization_Deserializer_new_async`].
    C(CDeserializer),
    /// `kafka_common_serialization_StringDeserializer_t`.
    String(GenericStringDeserializer),
    /// `kafka_common_serialization_ByteArrayDeserializer_t`.
    ByteArray(GenericByteArrayDeserializer),
    /// `kafka_common_serialization_BytesDeserializer_t`.
    Bytes(GenericBytesDeserializer),
}

/// Calls the same method on whichever variant `$self` is.
macro_rules! dispatch {
    ($self:expr, $deserializer:ident => $call:expr) => {
        match $self {
            FfiDeserializer::C($deserializer) => $call,
            FfiDeserializer::String($deserializer) => $call,
            FfiDeserializer::ByteArray($deserializer) => $call,
            FfiDeserializer::Bytes($deserializer) => $call,
        }
    };
}

impl Deserializer<GenericValue> for FfiDeserializer {
    async fn deserialize(&self, topic: &str, data: &[u8]) -> Result<GenericValue, Error> {
        dispatch!(self, deserializer => deserializer.deserialize(topic, data).await)
    }

    async fn deserialize_with_headers(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: &[u8],
    ) -> Result<GenericValue, Error> {
        dispatch!(self, deserializer => deserializer.deserialize_with_headers(topic, headers, data).await)
    }

    async fn deserialize_with_headers_bytes(
        &self,
        topic: &str,
        headers: &RecordHeaders,
        data: &Bytes,
    ) -> Result<GenericValue, Error> {
        dispatch!(self, deserializer => deserializer.deserialize_with_headers_bytes(topic, headers, data).await)
    }

    async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        dispatch!(self, deserializer => deserializer.configure(configs, is_key).await)
    }

    async fn close(&mut self) {
        dispatch!(self, deserializer => deserializer.close().await)
    }
}

impl FfiDeserializer {
    /// `method` for a blocking invoker: a C implementation runs on the
    /// calling thread (synchronous) or is waited for there (asynchronous),
    /// a built-in completes at once.
    fn invoke(
        &self,
        method: Method,
        topic: &str,
        headers: &RecordHeaders,
        data: Data<'_>,
    ) -> Result<GenericValue, Error> {
        match self {
            FfiDeserializer::C(deserializer) => {
                block_on_thread(deserializer.start(Dispatch::Inline, method, topic, headers, data))
            },
            other => ready(async {
                match (method, data) {
                    (Method::Deserialize, data) => other.deserialize(topic, data.as_slice()).await,
                    (Method::WithHeaders, data) => {
                        other.deserialize_with_headers(topic, headers, data.as_slice()).await
                    },
                    (Method::WithHeadersBytes, Data::Bytes(data)) => {
                        other.deserialize_with_headers_bytes(topic, headers, data).await
                    },
                    (Method::WithHeadersBytes, Data::Slice(data)) => {
                        other
                            .deserialize_with_headers_bytes(topic, headers, &Bytes::copy_from_slice(data))
                            .await
                    },
                }
            })
            .and_then(|r| r),
        }
    }
}

/// What a deserializer handle guards its implementation with.
pub(crate) type Locked = tokio::sync::Mutex<FfiDeserializer>;

/// `T deserialize(String topic, byte[] data)` of a synchronous C
/// implementation: `topic` and `data` are borrowed for the call; the
/// `void *` it returns is owned by the C side. On failure it stores an owned
/// error in `*out_err`, which points at `NULL` on entry, and its return value
/// is ignored.
pub type kafka_common_serialization_Deserializer_deserialize_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    topic: *const c_char,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void;

/// `T deserialize(String topic, Headers headers, byte[] data)` of a
/// synchronous C implementation; `headers` is a copy borrowed for the call.
/// `NULL` is the Java default: `deserialize(topic, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void,
>;

/// `T deserialize(String topic, Headers headers, ByteBuffer data)` of a
/// synchronous C implementation (KIP-863), the overload a consumer calls for
/// every fetched key and value; `headers` is a copy and `data` the record's
/// bytes, both borrowed for the call. `NULL` is the Java default:
/// `deserialize(topic, headers, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void,
>;

/// `configure(Map<String, ?> configs, boolean isKey)` of a synchronous C
/// implementation: `configs` maps NUL-terminated keys to NUL-terminated
/// values, sorted by key and borrowed for the call. `NULL` is the Java
/// default: nothing.
pub type kafka_common_serialization_Deserializer_configure_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8)>;

/// `close()` of a synchronous C implementation. `NULL` is the Java default:
/// nothing.
pub type kafka_common_serialization_Deserializer_close_fn_t = Option<unsafe extern "C" fn(self_: *mut c_void)>;

/// `T deserialize(String topic, byte[] data)` of an asynchronous C
/// implementation: returns at once and reports the `void *` value (owned by
/// the C side) or an owned error through
/// [`kafka_common_serialization_Deserializer__set_callback_result`] with
/// `callback_id`, from any thread, during the call or later. `topic` and
/// `data` stay valid until it reports.
pub type kafka_common_serialization_Deserializer_deserialize_async_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, topic: *const c_char, data: kafka_Bytes_t, callback_id: i64);

/// `T deserialize(String topic, Headers headers, byte[] data)` of an
/// asynchronous C implementation, reporting as
/// [`kafka_common_serialization_Deserializer_deserialize_async_fn_t`];
/// `headers` is a copy, valid like the other arguments until it reports.
/// `NULL` is the Java default: `deserialize(topic, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_async_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        callback_id: i64,
    ),
>;

/// `T deserialize(String topic, Headers headers, ByteBuffer data)` of an
/// asynchronous C implementation (KIP-863), reporting as
/// [`kafka_common_serialization_Deserializer_deserialize_async_fn_t`]; the
/// arguments stay valid until it reports. `NULL` is the Java default:
/// `deserialize(topic, headers, data)`.
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_async_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        callback_id: i64,
    ),
>;

/// `configure(Map<String, ?> configs, boolean isKey)` of an asynchronous C
/// implementation: `configs` is as for the synchronous form, valid until it
/// reports, with a `NULL` result (an error it reports is logged). `NULL` is
/// the Java default: nothing.
pub type kafka_common_serialization_Deserializer_configure_async_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8, callback_id: i64)>;

/// `close()` of an asynchronous C implementation, reporting a `NULL` result
/// (an error it reports is logged). `NULL` is the Java default: nothing.
pub type kafka_common_serialization_Deserializer_close_async_fn_t =
    Option<unsafe extern "C" fn(self_: *mut c_void, callback_id: i64)>;

/// What a [`kafka_common_serialization_Deserializer_t`] points at: the
/// implementation behind its mutex, who owns the handle, and, for an
/// asynchronous C implementation, the registry its reports go to, reached
/// without the mutex, which a consumer holds while it awaits a report.
///
/// Lives in an `Arc`: the handle is `Arc::into_raw` (owned) or the `Arc` a
/// class handle caches (view), and a client consuming the handle keeps the
/// `Arc`, so the pointer stays valid for `__set_callback_result` until the
/// client is destroyed.
pub(crate) struct DeserializerInner {
    imp: Arc<Locked>,
    /// `true` for a handle the C caller owns (`_new`, `_new_async`), `false`
    /// for the view a class handle caches (`__as_Deserializer`).
    owned: bool,
    results: Option<Arc<Results>>,
}

impl DeserializerInner {
    /// A handle the C caller owns.
    fn owned(imp: FfiDeserializer, results: Option<Arc<Results>>) -> *mut kafka_common_serialization_Deserializer_t {
        Arc::into_raw(Arc::new(Self {
            imp: Arc::new(tokio::sync::Mutex::new(imp)),
            owned: true,
            results,
        }))
        .cast_mut()
        .cast()
    }

    /// The view a class handle caches.
    pub(crate) fn view(imp: Arc<Locked>) -> Arc<Self> {
        Arc::new(Self { imp, owned: false, results: None })
    }

    /// The implementation behind its mutex.
    pub(crate) fn imp(&self) -> &Locked {
        &self.imp
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_serialization_Deserializer_t {
        (self as *const Self).cast()
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `deserializer` must be a valid deserializer handle, alive for `'a`.
pub(crate) unsafe fn deserializer_ref<'a>(
    deserializer: *const kafka_common_serialization_Deserializer_t,
) -> &'a DeserializerInner {
    // SAFETY: Per this function's `# Safety`, `deserializer` is a valid deserializer handle alive
    // for `'a`, so it points at a live `DeserializerInner` inside an `Arc`: the one
    // `DeserializerInner::owned` handed out, kept by the C caller or the client that consumed it,
    // or the view a `DeserializerHandle` caches. A `DeserializerInner` is never mutated after it is
    // built, so the shared reference is sound for `'a`.
    unsafe { &*deserializer.cast::<DeserializerInner>() }
}

/// Takes the handle a `*mut` parameter received: an owned handle's reference
/// passes to the caller, a view's is cloned and the view stays with its class
/// handle. The consumer is `kafka_consumer_KafkaConsumer_new` (CLAUDE.md §4
/// rule 6); it keeps the returned `Arc`, so the handle pointer stays valid
/// for `__set_callback_result` until that client is destroyed.
///
/// # Safety
///
/// `deserializer` must be a valid deserializer handle; when it was owned,
/// the caller passes it on to nothing else but `__set_callback_result`.
pub(crate) unsafe fn take_deserializer(
    deserializer: *mut kafka_common_serialization_Deserializer_t,
) -> Arc<DeserializerInner> {
    let inner = deserializer.cast_const().cast::<DeserializerInner>();
    // SAFETY: Per this function's `# Safety`, `deserializer` is a valid deserializer handle: a
    // pointer into a live `Arc<DeserializerInner>`, `Arc::into_raw` (owned) or `Arc::as_ptr`
    // (view). For an owned one the caller's reference is taken over, once, as the caller does not
    // consume it again; a view gains a reference first, so the class handle keeps its own.
    unsafe {
        if !(*inner).owned {
            Arc::increment_strong_count(inner);
        }
        Arc::from_raw(inner)
    }
}

/// Which deserializer method a call is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Method {
    /// `deserialize(topic, data)`.
    Deserialize,
    /// `deserialize(topic, headers, data)`.
    WithHeaders,
    /// `deserialize(topic, headers, ByteBuffer data)`.
    WithHeadersBytes,
}

/// The functions of a synchronous C implementation.
#[derive(Clone, Copy)]
struct SyncFns {
    deserialize: kafka_common_serialization_Deserializer_deserialize_fn_t,
    deserialize_with_headers: kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t,
    deserialize_with_headers_bytes: kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_fn_t,
    configure: kafka_common_serialization_Deserializer_configure_fn_t,
    close: kafka_common_serialization_Deserializer_close_fn_t,
}

/// The functions of an asynchronous C implementation.
#[derive(Clone, Copy)]
struct AsyncFns {
    deserialize: kafka_common_serialization_Deserializer_deserialize_async_fn_t,
    deserialize_with_headers: kafka_common_serialization_Deserializer_deserialize_with_headers_async_fn_t,
    deserialize_with_headers_bytes: kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_async_fn_t,
    configure: kafka_common_serialization_Deserializer_configure_async_fn_t,
    close: kafka_common_serialization_Deserializer_close_async_fn_t,
}

/// How a C implementation is called.
enum Mode {
    /// [`kafka_common_serialization_Deserializer_new`].
    Sync(SyncFns),
    /// [`kafka_common_serialization_Deserializer_new_async`], with the
    /// registry its reports go to, shared with its handle.
    Async(AsyncFns, Arc<Results>),
}

/// A C implementation of [`Deserializer`]; a null optional method is the
/// Java default.
pub(crate) struct CDeserializer {
    self_: *mut c_void,
    mode: Mode,
}

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_serialization_Deserializer_new` or `_new_async`: the C user is responsible for the
// thread-safety of `self_`, which those functions' rustdoc states, and the function pointers are
// trivially shareable; the registry is `Send`. `CDeserializer` itself never dereferences `self_`;
// it only passes it back to the functions registered with it.
unsafe impl Send for CDeserializer {}
// SAFETY: `Sync` is required by the `Deserializer` trait. A `&CDeserializer` exposes only the
// opaque `self_`, the `Copy` function pointers, which the type never dereferences, and the `Sync`
// registry; the C user is responsible for the thread-safety of `self_` across the threads that call
// it, as the registering functions' rustdoc states.
unsafe impl Sync for CDeserializer {}

/// Reads a C implementation's result: the error it stored, or the `void *`
/// it returned.
///
/// # Safety
///
/// `error` must be null or an owned error.
unsafe fn collect(error: *mut kafka_common_Error_t, out: *mut c_void) -> Result<GenericValue, Error> {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error handle, the
    // `box_error` handle of a `kafka_common_Error_*` constructor or a fallible FFI function that
    // the C implementation handed over, not yet destroyed, as `take_error` requires; it is consumed
    // here.
    match unsafe { take_error(error) } {
        Some(error) => Err(error),
        None => Ok(GenericValue::new(out)),
    }
}

/// The C function a method resolves to, Java's defaults applied.
enum Target<P, H> {
    Plain(P),
    Headers(H),
}

/// Resolves `method` to a function: a `NULL` header method falls back to
/// the method Java's default calls.
fn resolve<P: Copy, H: Copy>(
    method: Method,
    plain: P,
    with_headers: Option<H>,
    with_headers_bytes: Option<H>,
) -> Target<P, H> {
    match (method, with_headers_bytes, with_headers) {
        (Method::WithHeadersBytes, Some(function), _) => Target::Headers(function),
        (Method::WithHeadersBytes | Method::WithHeaders, _, Some(function)) => Target::Headers(function),
        _ => Target::Plain(plain),
    }
}

/// The deserialized value of a report: the `void *` the C side owns.
fn finish_value(_: &mut CallArgs, result: Result<*mut c_void, Error>) -> Result<GenericValue, Error> {
    result.map(GenericValue::new)
}

/// The outcome of a `void` method's report.
fn finish_unit(_: &mut CallArgs, result: Result<*mut c_void, Error>) -> Result<(), Error> {
    result.map(|_| ())
}

/// A report's `result` needs no conversion on the reporting thread.
fn no_convert(_: &mut CallArgs, _: *mut c_void) {}

impl CDeserializer {
    /// Whether the implementation was registered with `_new_async`.
    fn is_async(&self) -> bool {
        matches!(self.mode, Mode::Async(..))
    }

    /// Starts `method`. A synchronous implementation runs where `dispatch`
    /// says, owning copies of the arguments when it leaves the calling
    /// thread; an asynchronous one is called at once, its arguments kept in
    /// the registry slot until it reports.
    pub(crate) fn start(
        &self,
        dispatch_to: Dispatch,
        method: Method,
        topic: &str,
        headers: &RecordHeaders,
        data: Data<'_>,
    ) -> Call<'_, GenericValue> {
        let self_ = SendPtr(self.self_);
        match &self.mode {
            Mode::Sync(fns) => {
                let topic = owned_c_string(topic);
                let data = data.to_bytes();
                match resolve(
                    method,
                    fns.deserialize,
                    fns.deserialize_with_headers,
                    fns.deserialize_with_headers_bytes,
                ) {
                    Target::Plain(function) => dispatch(dispatch_to, move || {
                        let mut error = ptr::null_mut();
                        // SAFETY: `function` and `self_` are the pair the C caller registered together
                        // through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the
                        // caller keep `self_` alive until every call made through the handle has
                        // returned. The arguments are borrowed for the call, as the `_fn_t` rustdoc
                        // states: `topic` and `data` are owned by this closure, and `out_err` is a
                        // local slot pointing at null.
                        let out = unsafe {
                            function(self_.get(), topic.as_ptr(), kafka_Bytes_t::from_slice(&data), &raw mut error)
                        };
                        // SAFETY: `error` is what the C function stored in its slot: null or an owned
                        // error handle, as its `_fn_t` rustdoc states and `collect` requires.
                        unsafe { collect(error, out) }
                    }),
                    Target::Headers(function) => {
                        let headers = RecordHeadersInner::new(headers.clone());
                        dispatch(dispatch_to, move || {
                            let mut error = ptr::null_mut();
                            // SAFETY: As for the plain form; `headers` is owned by this closure, which
                            // does not move it during the call.
                            let out = unsafe {
                                function(
                                    self_.get(),
                                    topic.as_ptr(),
                                    headers.as_ptr(),
                                    kafka_Bytes_t::from_slice(&data),
                                    &raw mut error,
                                )
                            };
                            // SAFETY: As for the plain form.
                            unsafe { collect(error, out) }
                        })
                    },
                }
            },
            Mode::Async(fns, results) => {
                let target = resolve(
                    method,
                    fns.deserialize,
                    fns.deserialize_with_headers,
                    fns.deserialize_with_headers_bytes,
                );
                let with_headers = matches!(target, Target::Headers(_));
                let (id, (topic, headers, data)) = results.register_waiting(no_convert, |args| {
                    let headers = if with_headers {
                        args.set_headers(headers)
                    } else {
                        ptr::null()
                    };
                    (args.set_topic(topic), headers, args.set_data(data))
                });
                // SAFETY: The function and `self_` are the pair the C caller registered together
                // through `kafka_common_serialization_Deserializer_new_async`, whose rustdoc has the
                // caller keep `self_` alive until every call made through the handle has reported.
                // `topic`, `headers` and `data` point into the registry slot's `CallArgs`, which the
                // slot keeps until the report for `id`, as the `_async_fn_t` rustdoc states, even if
                // the returned future is dropped first. The registry's lock is not held, so the
                // implementation may report during the call.
                unsafe {
                    match target {
                        Target::Plain(function) => function(self_.get(), topic, data, id),
                        Target::Headers(function) => function(self_.get(), topic, headers, data, id),
                    }
                }
                Call::Wait(WaitReport::new(results, id, finish_value))
            },
        }
    }

    /// Starts `configure`; `None` when the method is the Java default.
    fn start_configure(
        &self,
        dispatch_to: Dispatch,
        configs: &HashMap<String, String>,
        is_key: bool,
    ) -> Option<Call<'_, ()>> {
        let self_ = SendPtr(self.self_);
        let is_key = i8::from(is_key);
        match &self.mode {
            Mode::Sync(fns) => {
                let configure = fns.configure?;
                let configs = send_ptr(build_configs(configs));
                Some(dispatch(dispatch_to, move || {
                    let configs = configs.get().cast::<kafka_Map_t>();
                    // SAFETY: `configure` and `self_` are the pair the C caller registered together
                    // through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the
                    // caller keep `self_` alive until every call made through the handle has
                    // returned. `configs` is the owned map `build_configs` built, borrowed by the call
                    // as the `_fn_t` rustdoc states, and destroyed once the call returns.
                    unsafe { configure(self_.get(), configs, is_key) };
                    // SAFETY: `configs` is the owned map built above, not yet destroyed; the C
                    // `configure` only borrowed it for its call.
                    unsafe { kafka_Map_destroy(configs) };
                    Ok(())
                }))
            },
            Mode::Async(fns, results) => {
                let configure = fns.configure?;
                let configs = build_configs(configs);
                let (id, ()) = results.register_waiting(no_convert, |args| args.set_configs(configs));
                // SAFETY: `configure` and `self_` are the pair the C caller registered together
                // through `kafka_common_serialization_Deserializer_new_async`, whose rustdoc has the
                // caller keep `self_` alive until every call made through the handle has reported.
                // `configs` is owned by the registry slot until the report for `id`, as the
                // `_async_fn_t` rustdoc states.
                unsafe { configure(self_.get(), configs, is_key, id) };
                Some(Call::Wait(WaitReport::new(results, id, finish_unit)))
            },
        }
    }

    /// Starts `close`; `None` when the method is the Java default.
    fn start_close(&self, dispatch_to: Dispatch) -> Option<Call<'_, ()>> {
        let self_ = SendPtr(self.self_);
        match &self.mode {
            Mode::Sync(fns) => {
                let close = fns.close?;
                Some(dispatch(dispatch_to, move || {
                    // SAFETY: `close` and `self_` are the pair the C caller registered together
                    // through `kafka_common_serialization_Deserializer_new`, whose rustdoc has the
                    // caller keep `self_` alive until every call made through the handle has
                    // returned.
                    unsafe { close(self_.get()) };
                    Ok(())
                }))
            },
            Mode::Async(fns, results) => {
                let close = fns.close?;
                let (id, ()) = results.register_waiting(no_convert, |_| ());
                // SAFETY: `close` and `self_` are the pair the C caller registered together through
                // `kafka_common_serialization_Deserializer_new_async`, whose rustdoc has the caller
                // keep `self_` alive until every call made through the handle has reported.
                unsafe { close(self_.get(), id) };
                Some(Call::Wait(WaitReport::new(results, id, finish_unit)))
            },
        }
    }

    /// Starts `method` for a `_cb` twin, on an asynchronous implementation:
    /// `sink` receives the report on the reporting thread, and owns the
    /// arguments C reads until then.
    #[expect(clippy::too_many_arguments)]
    fn start_with_sink(
        &self,
        results: &Results,
        fns: AsyncFns,
        method: Method,
        topic: &str,
        headers: &RecordHeaders,
        data: &[u8],
        deliver: impl FnOnce(Result<*mut c_void, Error>) + Send + 'static,
    ) {
        let topic = owned_c_string(topic);
        let headers = Box::new(RecordHeadersInner::new(headers.clone()));
        let data = Bytes::copy_from_slice(data);
        let (topic_ptr, headers_ptr, bytes) = (topic.as_ptr(), headers.as_ptr(), kafka_Bytes_t::from_slice(&data));
        let sink: ResultSink = Box::new(move |result| {
            deliver(result);
            // The arguments the implementation read, released once it reported.
            drop((topic, headers, data));
        });
        let id = results.register(sink);
        // SAFETY: The function and `self.self_` are the pair the C caller registered together
        // through `kafka_common_serialization_Deserializer_new_async`, whose rustdoc has the caller
        // keep `self_` alive until every call made through the handle has reported. The arguments
        // point into the heap buffers the sink owns, which it keeps until the report for `id`.
        unsafe {
            match resolve(
                method,
                fns.deserialize,
                fns.deserialize_with_headers,
                fns.deserialize_with_headers_bytes,
            ) {
                Target::Plain(function) => function(self.self_, topic_ptr, bytes, id),
                Target::Headers(function) => function(self.self_, topic_ptr, headers_ptr, bytes, id),
            }
        }
    }
}

/// The owned map a C `configure` reads: sorted by key, since Java's map is
/// unordered and index addressing through the C map is not.
fn build_configs(configs: &HashMap<String, String>) -> *mut kafka_Map_t {
    let sorted: BTreeMap<&str, &str> = configs.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    box_string_keyed_map(
        sorted.into_iter().map(|(k, v)| (k, into_c_string(v) as *mut c_void)),
        Some(destroy_string_element),
    )
}

/// Logs the failure of a `void` method, which has nowhere to report it.
fn log_failure(what: &str, result: Result<(), Error>) {
    if let Err(error) = result {
        log::warn!("Failed to {what} the deserializer: {error}");
    }
}

// Each method returns the `Call` future itself rather than awaiting it in an
// `async fn`, so the consumer's wrapper stays within `InlineFuture`'s buffer.
impl Deserializer<GenericValue> for CDeserializer {
    fn deserialize<'a>(
        &'a self,
        topic: &'a str,
        data: &'a [u8],
    ) -> impl Future<Output = Result<GenericValue, Error>> + Send + 'a {
        self.start(
            Dispatch::Blocking,
            Method::Deserialize,
            topic,
            &EMPTY_HEADERS,
            Data::Slice(data),
        )
    }

    fn deserialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a [u8],
    ) -> impl Future<Output = Result<GenericValue, Error>> + Send + 'a {
        self.start(Dispatch::Blocking, Method::WithHeaders, topic, headers, Data::Slice(data))
    }

    fn deserialize_with_headers_bytes<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: &'a Bytes,
    ) -> impl Future<Output = Result<GenericValue, Error>> + Send + 'a {
        self.start(Dispatch::Blocking, Method::WithHeadersBytes, topic, headers, Data::Bytes(data))
    }

    async fn configure(&mut self, configs: &HashMap<String, String>, is_key: bool) {
        if let Some(call) = self.start_configure(Dispatch::Blocking, configs, is_key) {
            log_failure("configure", call.await);
        }
    }

    async fn close(&mut self) {
        if let Some(call) = self.start_close(Dispatch::Blocking) {
            log_failure("close", call.await);
        }
    }
}

/// The headers of a method that takes none: never read, since a plain
/// method never reaches a header-taking C function.
static EMPTY_HEADERS: std::sync::LazyLock<RecordHeaders> = std::sync::LazyLock::new(RecordHeaders::new);

/// Registers a synchronous C implementation of `Deserializer`; `deserialize`
/// is required, the other pointers may be `NULL` for the Java default. The
/// caller owns `self_` and keeps it alive until the handle is destroyed with
/// [`kafka_common_serialization_Deserializer_destroy`] or, once a client
/// consumed it, until that client is destroyed, and in any case until every
/// call made through the handle has returned. The functions are called one at
/// a time, under the handle's lock, but from whichever thread makes the call,
/// so the C caller is responsible for `self_` being usable from any thread. A
/// consumer calls them on Tokio's blocking pool, so a slow implementation
/// holds no runtime worker; the invokers call them on their calling thread.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_Deserializer_new(
    self_: *mut c_void,
    deserialize: kafka_common_serialization_Deserializer_deserialize_fn_t,
    deserialize_with_headers: kafka_common_serialization_Deserializer_deserialize_with_headers_fn_t,
    deserialize_with_headers_bytes: kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_fn_t,
    configure: kafka_common_serialization_Deserializer_configure_fn_t,
    close: kafka_common_serialization_Deserializer_close_fn_t,
) -> *mut kafka_common_serialization_Deserializer_t {
    DeserializerInner::owned(
        FfiDeserializer::C(CDeserializer {
            self_,
            mode: Mode::Sync(SyncFns {
                deserialize,
                deserialize_with_headers,
                deserialize_with_headers_bytes,
                configure,
                close,
            }),
        }),
        None,
    )
}

/// Registers an asynchronous C implementation of `Deserializer`;
/// `deserialize` is required, the other pointers may be `NULL` for the Java
/// default. Each function returns at once and reports its outcome through
/// [`kafka_common_serialization_Deserializer__set_callback_result`] on this
/// handle with the `callback_id` it was given, from any thread, during the
/// call or later; the pointers it was given stay valid until it reports. The
/// handle stays valid for that report until it is destroyed with
/// [`kafka_common_serialization_Deserializer_destroy`] or, once a client
/// consumed it, until that client is destroyed: every call must report
/// before then. The caller owns `self_` and keeps it alive until every call
/// made through the handle has reported. A consumer makes one call at a time,
/// under the handle's lock, and awaits the report without holding a thread;
/// the `_cb` twins do not take part in that lock, so the C caller is
/// responsible for `self_` being usable from any thread, concurrently.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_Deserializer_new_async(
    self_: *mut c_void,
    deserialize: kafka_common_serialization_Deserializer_deserialize_async_fn_t,
    deserialize_with_headers: kafka_common_serialization_Deserializer_deserialize_with_headers_async_fn_t,
    deserialize_with_headers_bytes: kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_async_fn_t,
    configure: kafka_common_serialization_Deserializer_configure_async_fn_t,
    close: kafka_common_serialization_Deserializer_close_async_fn_t,
) -> *mut kafka_common_serialization_Deserializer_t {
    let results = Arc::new(Results::with_keep_alive());
    DeserializerInner::owned(
        FfiDeserializer::C(CDeserializer {
            self_,
            mode: Mode::Async(
                AsyncFns {
                    deserialize,
                    deserialize_with_headers,
                    deserialize_with_headers_bytes,
                    configure,
                    close,
                },
                Arc::clone(&results),
            ),
        }),
        Some(results),
    )
}

/// Whether the handle stands for an asynchronous C implementation
/// ([`kafka_common_serialization_Deserializer_new_async`]): 1, else 0, which
/// includes a built-in class's view.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer__is_async(
    self_: *const kafka_common_serialization_Deserializer_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; the reference is used only within this call.
    i8::from(unsafe { deserializer_ref(self_) }.results.is_some())
}

/// Reports the outcome of the asynchronous call `callback_id`: on success
/// `error` is `NULL` and `result` the value (the `void *` for a
/// `deserialize*` call, `NULL` for `configure` / `close`); on failure
/// `error` is an owned error, taken over, and `result` is ignored. Callable
/// from any thread, during the call or later, even after a client consumed
/// the handle (until that client is destroyed). A report for an unknown id,
/// or a second one for the same id, is ignored and its error freed; so is any
/// report on a handle that is not asynchronous.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, or one a live client
/// consumed, and `error` null or an owned error.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer__set_callback_result(
    self_: *const kafka_common_serialization_Deserializer_t,
    callback_id: i64,
    result: *mut c_void,
    error: *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error, as `take_error`
    // requires; it is consumed here.
    let error = unsafe { take_error(error) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, kept alive by
    // its owner or by the client holding its `Arc`, as `deserializer_ref` requires.
    if let Some(results) = &unsafe { deserializer_ref(self_) }.results {
        results.complete(callback_id, result, error);
    }
}

/// Returns a deserialized value, or stores the owned error in `*out_err`.
///
/// # Safety
///
/// `out_err` must be valid for one write and point at null.
unsafe fn deliver(result: Result<GenericValue, Error>, out_err: *mut *mut kafka_common_Error_t) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. The
    // `void *` returned is the implementation's, owned as its class or C implementation documents.
    unsafe { deliver_value(result, out_err, |value| value.as_ptr()) }
}

/// The bytes behind `data`, an empty array for Java's `null`.
///
/// # Safety
///
/// As [`kafka_Bytes_t::as_slice`].
unsafe fn slice<'a>(data: kafka_Bytes_t) -> &'a [u8] {
    // SAFETY: This function's `# Safety` is `kafka_Bytes_t::as_slice`'s: `data.data` is null or
    // points at `len` readable bytes that outlive `'a` and are not written during it.
    unsafe { data.as_slice() }.unwrap_or(&[])
}

/// Runs `method` through a blocking invoker: a synchronous C implementation
/// on the calling thread, an asynchronous one waited for there, under the
/// handle's lock.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle.
unsafe fn invoke(
    self_: *const kafka_common_serialization_Deserializer_t,
    method: Method,
    topic: &str,
    headers: &RecordHeaders,
    data: &[u8],
) -> Result<GenericValue, Error> {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; it stays alive for this call, and the guard on its mutex is
    // released before return.
    let deserializer = lock_blocking(unsafe { deserializer_ref(self_) }.imp());
    let data = if method == Method::WithHeadersBytes {
        // `ByteBuffer` data the implementation may keep a slice of, as a `BytesDeserializer`
        // does: copied once into a shared buffer.
        return deserializer.invoke(method, topic, headers, Data::Bytes(&Bytes::copy_from_slice(data)));
    } else {
        Data::Slice(data)
    };
    deserializer.invoke(method, topic, headers, data)
}

/// The `_cb` form of `method`: a synchronous C implementation or a built-in
/// runs on the calling thread and `deliver` runs there; an asynchronous
/// implementation is called at once and `deliver` runs on the thread that
/// reports.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle.
unsafe fn invoke_cb(
    self_: *const kafka_common_serialization_Deserializer_t,
    method: Method,
    topic: &str,
    headers: &RecordHeaders,
    data: &[u8],
    deliver: impl FnOnce(Result<*mut c_void, Error>) + Send + 'static,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; it stays alive for this call, and the guard on its mutex is
    // released before return.
    let deserializer = lock_blocking(unsafe { deserializer_ref(self_) }.imp());
    if let FfiDeserializer::C(c) = &*deserializer
        && let Mode::Async(fns, results) = &c.mode
    {
        let (fns, results) = (*fns, Arc::clone(results));
        c.start_with_sink(&results, fns, method, topic, headers, data, deliver);
        return;
    }
    let result = if method == Method::WithHeadersBytes {
        deserializer.invoke(method, topic, headers, Data::Bytes(&Bytes::copy_from_slice(data)))
    } else {
        deserializer.invoke(method, topic, headers, Data::Slice(data))
    };
    drop(deserializer);
    deliver(result.map(|value| value.as_ptr()));
}

/// Completion of the `deserialize*` `_cb` twins: the value (`NULL` on
/// failure), an owned error (`NULL` on success) and the caller's `opaque`.
type ValueCompletion = unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Fires a `deserialize*` completion with `result`.
fn value_sink(cb: ValueCompletion, opaque: *mut c_void) -> impl FnOnce(Result<*mut c_void, Error>) + Send + 'static {
    let opaque = SendPtr(opaque);
    move |result| {
        let (value, error) = match result {
            Ok(value) => (value, ptr::null_mut()),
            Err(error) => (ptr::null_mut(), box_error(error)),
        };
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to the `_cb` twin, a valid
        // completion until it fires; this `FnOnce` fires it once. `error` is null or a fresh
        // `box_error` handle whose ownership passes to `cb`.
        unsafe { cb(value, error, opaque.get()) }
    }
}

/// `T deserialize(String topic, byte[] data)`: `data` is borrowed for the
/// call, `data.data == NULL` read as an empty array. Returns the
/// implementation's `void *`; on failure stores the owned error in
/// `*out_err` and returns `NULL`. Blocks until an asynchronous
/// implementation reports.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `data` null or a valid array and
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle and `data` null
    // or a valid array, read only during this call: an implementation that outlives the call gets a
    // copy.
    let result = unsafe { invoke(self_, Method::Deserialize, &topic, &EMPTY_HEADERS, slice(data)) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot pointing at null, as
    // `deliver` requires.
    unsafe { deliver(result, out_err) }
}

/// Completion of [`kafka_common_serialization_Deserializer_deserialize_cb`].
pub type kafka_common_serialization_Deserializer_deserialize_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking [`kafka_common_serialization_Deserializer_deserialize`]:
/// `cb` receives the value or an owned error, with `opaque`. A synchronous
/// implementation or a built-in runs on the calling thread and `cb` fires
/// there before this returns; an asynchronous implementation is called at
/// once and `cb` fires on the thread that reports (a deserializer has no
/// callbacks vector to queue on). The arguments are copied.
///
/// # Safety
///
/// As the blocking form, without `out_err`; `cb` must stay valid until it
/// fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as its last step, to the sink it registers or the completion it fires; nothing after that can
    // panic, as the C method cannot unwind into Rust. So `cb` has not fired for this `opaque`, as
    // `deliver_panic_value` requires; `run_inline` fires it on this thread.
    unsafe { deliver_panic_value(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_cb(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    data: kafka_Bytes_t,
    cb: kafka_common_serialization_Deserializer_deserialize_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle and `data` null
    // or a valid array, read only during this call.
    unsafe {
        invoke_cb(
            self_,
            Method::Deserialize,
            &topic,
            &EMPTY_HEADERS,
            slice(data),
            value_sink(cb, opaque),
        )
    }
}

/// `T deserialize(String topic, Headers headers, byte[] data)`; the value
/// is delivered as for [`kafka_common_serialization_Deserializer_deserialize`].
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid record-headers handle, `data`
/// null or a valid array and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_with_headers(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, alive and
    // unchanged for this call, as `record_headers_ref` requires.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle and `data` null
    // or a valid array, read only during this call.
    let result = unsafe { invoke(self_, Method::WithHeaders, &topic, headers, slice(data)) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot pointing at null.
    unsafe { deliver(result, out_err) }
}

/// Completion of
/// [`kafka_common_serialization_Deserializer_deserialize_with_headers_cb`].
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking
/// [`kafka_common_serialization_Deserializer_deserialize_with_headers`]; `cb`
/// fires as for [`kafka_common_serialization_Deserializer_deserialize_cb`].
///
/// # Safety
///
/// As the blocking form, without `out_err`; `cb` must stay valid until it
/// fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: As for `kafka_common_serialization_Deserializer_deserialize_cb`: `cb` has not fired
    // for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_with_headers_cb(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: kafka_Bytes_t,
    cb: kafka_common_serialization_Deserializer_deserialize_with_headers_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, alive and
    // unchanged for this call.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle and `data` null
    // or a valid array, read only during this call.
    unsafe { invoke_cb(self_, Method::WithHeaders, &topic, headers, slice(data), value_sink(cb, opaque)) }
}

/// `T deserialize(String topic, Headers headers, ByteBuffer data)`
/// (KIP-863): `data` is copied once into a buffer the implementation may
/// keep a slice of, as a `BytesDeserializer` does; the value is delivered as
/// for [`kafka_common_serialization_Deserializer_deserialize`].
///
/// # Safety
///
/// `self_` must be a valid deserializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid record-headers handle, `data`
/// null or a valid array and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: kafka_Bytes_t,
    out_err: *mut *mut kafka_common_Error_t,
) -> *mut c_void {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, alive and
    // unchanged for this call.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle and `data` null
    // or a valid array, read only during this call.
    let result = unsafe { invoke(self_, Method::WithHeadersBytes, &topic, headers, slice(data)) };
    // SAFETY: Per this function's `# Safety`, `out_err` is a valid slot pointing at null.
    unsafe { deliver(result, out_err) }
}

/// Completion of
/// [`kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_cb`].
pub type kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_cb_t =
    unsafe extern "C" fn(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking
/// [`kafka_common_serialization_Deserializer_deserialize_with_headers_bytes`];
/// `cb` fires as for
/// [`kafka_common_serialization_Deserializer_deserialize_cb`].
///
/// # Safety
///
/// As the blocking form, without `out_err`; `cb` must stay valid until it
/// fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: As for `kafka_common_serialization_Deserializer_deserialize_cb`: `cb` has not fired
    // for this `opaque`, as `deliver_panic_value` requires.
    unsafe { deliver_panic_value(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_cb(
    self_: *const kafka_common_serialization_Deserializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: kafka_Bytes_t,
    cb: kafka_common_serialization_Deserializer_deserialize_with_headers_bytes_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, alive and
    // unchanged for this call.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle and `data` null
    // or a valid array, read only during this call.
    unsafe {
        invoke_cb(
            self_,
            Method::WithHeadersBytes,
            &topic,
            headers,
            slice(data),
            value_sink(cb, opaque),
        )
    }
}

/// The configs a C caller passed, copied.
///
/// # Safety
///
/// `configs` must be null or a valid map of NUL-terminated strings.
unsafe fn read_configs(configs: *const kafka_Map_t) -> HashMap<String, String> {
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

/// Runs a `void` method (`configure` or `close`) through an invoker: the
/// call `start` makes, `None` for the Java default, on the calling thread,
/// or a built-in's future.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle.
unsafe fn invoke_void(
    self_: *const kafka_common_serialization_Deserializer_t,
    what: &str,
    start: impl FnOnce(&CDeserializer) -> Option<Call<'_, ()>>,
    builtin: impl AsyncFnOnce(&mut FfiDeserializer),
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires; the reference and the guard on its mutex are used only within
    // this call.
    let mut deserializer = lock_blocking(unsafe { deserializer_ref(self_) }.imp());
    match &mut *deserializer {
        FfiDeserializer::C(c) => {
            if let Some(call) = start(c) {
                log_failure(what, block_on_thread(call));
            }
        },
        other => log_failure(what, ready(builtin(other))),
    }
}

/// `configure(Map<String, ?> configs, boolean isKey)`: `configs` maps
/// NUL-terminated keys to NUL-terminated values, copied during the call.
/// Blocks until an asynchronous implementation reports; a failure is logged.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle and `configs` null or a
/// valid map of NUL-terminated strings.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_configure(
    self_: *mut kafka_common_serialization_Deserializer_t,
    configs: *const kafka_Map_t,
    is_key: i8,
) {
    // SAFETY: Per this function's `# Safety`, `configs` is null or a valid map of NUL-terminated
    // strings.
    let configs = unsafe { read_configs(configs) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle.
    unsafe {
        invoke_void(
            self_,
            "configure",
            |c| c.start_configure(Dispatch::Inline, &configs, is_key != 0),
            async |d| d.configure(&configs, is_key != 0).await,
        )
    }
}

/// Completion of [`kafka_common_serialization_Deserializer_configure_cb`]:
/// `configure` is void, so only `opaque` is passed.
pub type kafka_common_serialization_Deserializer_configure_cb_t = unsafe extern "C" fn(opaque: *mut c_void);

/// Non-blocking [`kafka_common_serialization_Deserializer_configure`]: `cb`
/// fires with `opaque` on the calling thread, or, for an asynchronous
/// implementation, on the thread that reports. A failure is logged.
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as its last step; nothing after that can panic, as the C method cannot unwind into Rust. So
    // `cb` has not fired for this `opaque`, as `deliver_panic_opaque` requires.
    unsafe { deliver_panic_opaque(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_configure_cb(
    self_: *mut kafka_common_serialization_Deserializer_t,
    configs: *const kafka_Map_t,
    is_key: i8,
    cb: kafka_common_serialization_Deserializer_configure_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `configs` is null or a valid map of NUL-terminated
    // strings.
    let configs = unsafe { read_configs(configs) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle.
    unsafe {
        invoke_void_cb(
            self_,
            "configure",
            |c| c.start_configure_with_sink(&configs, is_key != 0, opaque_sink("configure", cb, opaque)),
            |c| c.start_configure(Dispatch::Inline, &configs, is_key != 0),
            async |d| d.configure(&configs, is_key != 0).await,
            cb,
            opaque,
        )
    }
}

/// `close()`. Blocks until an asynchronous implementation reports; a
/// failure is logged.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_close(
    self_: *mut kafka_common_serialization_Deserializer_t,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle.
    unsafe { invoke_void(self_, "close", |c| c.start_close(Dispatch::Inline), async |d| d.close().await) }
}

/// Completion of [`kafka_common_serialization_Deserializer_close_cb`]:
/// `close` is void, so only `opaque` is passed.
pub type kafka_common_serialization_Deserializer_close_cb_t = unsafe extern "C" fn(opaque: *mut c_void);

/// Non-blocking [`kafka_common_serialization_Deserializer_close`]: `cb`
/// fires as for [`kafka_common_serialization_Deserializer_configure_cb`].
///
/// # Safety
///
/// As the blocking form; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: As for `kafka_common_serialization_Deserializer_configure_cb`: `cb` has not fired for
    // this `opaque`, as `deliver_panic_opaque` requires.
    unsafe { deliver_panic_opaque(run_inline, cb, opaque, err) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_close_cb(
    self_: *mut kafka_common_serialization_Deserializer_t,
    cb: kafka_common_serialization_Deserializer_close_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle.
    unsafe {
        invoke_void_cb(
            self_,
            "close",
            |c| c.start_close_with_sink(opaque_sink("close", cb, opaque)),
            |c| c.start_close(Dispatch::Inline),
            async |d| d.close().await,
            cb,
            opaque,
        )
    }
}

/// Completion of the `void` `_cb` twins.
type OpaqueCb = unsafe extern "C" fn(opaque: *mut c_void);

/// Fires a `void` completion, logging a failure.
fn opaque_sink(what: &'static str, cb: OpaqueCb, opaque: *mut c_void) -> ResultSink {
    let opaque = SendPtr(opaque);
    Box::new(move |result| {
        log_failure(what, result.map(|_| ()));
        // SAFETY: `cb` and `opaque` are the pair the C caller passed to the `_cb` twin, a valid
        // completion until it fires; this `FnOnce` fires it once.
        unsafe { cb(opaque.get()) }
    })
}

/// The `_cb` form of a `void` method: an asynchronous implementation is
/// started with `with_sink`, which fires `cb` when it reports (`false` for
/// the Java default, fired here); otherwise the method runs as for
/// [`invoke_void`] and `cb` fires on the calling thread.
///
/// # Safety
///
/// `self_` must be a valid deserializer handle and `cb` a valid completion.
unsafe fn invoke_void_cb(
    self_: *const kafka_common_serialization_Deserializer_t,
    what: &str,
    with_sink: impl FnOnce(&CDeserializer) -> bool,
    start: impl FnOnce(&CDeserializer) -> Option<Call<'_, ()>>,
    builtin: impl AsyncFnOnce(&mut FfiDeserializer),
    cb: OpaqueCb,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid deserializer handle, as
    // `deserializer_ref` requires.
    let mut deserializer = lock_blocking(unsafe { deserializer_ref(self_) }.imp());
    match &mut *deserializer {
        FfiDeserializer::C(c) if c.is_async() => {
            if with_sink(c) {
                return;
            }
        },
        FfiDeserializer::C(c) => {
            if let Some(call) = start(c) {
                log_failure(what, block_on_thread(call));
            }
        },
        other => log_failure(what, ready(builtin(other))),
    }
    drop(deserializer);
    // SAFETY: Per this function's `# Safety`, `cb` is a valid completion, not yet fired: no sink
    // took it (`with_sink` returned `false` or was not called).
    unsafe { cb(opaque) }
}

impl CDeserializer {
    /// `configure` for the `_cb` twin of an asynchronous implementation:
    /// `false` for the Java default, `sink` dropped unfired.
    fn start_configure_with_sink(&self, configs: &HashMap<String, String>, is_key: bool, sink: ResultSink) -> bool {
        let Mode::Async(AsyncFns { configure: Some(configure), .. }, results) = &self.mode else {
            return false;
        };
        let configs = SendPtr(build_configs(configs).cast());
        let id = results.register(Box::new(move |result| {
            sink(result);
            // SAFETY: `configs` is the owned map built above, read by the implementation until it
            // reported, which is now; destroyed once, here.
            unsafe { kafka_Map_destroy(configs.get().cast()) };
        }));
        // SAFETY: `configure` and `self.self_` are the pair the C caller registered together through
        // `kafka_common_serialization_Deserializer_new_async`; `configs` is owned by the sink until
        // the report for `id`, as the `_async_fn_t` rustdoc states.
        unsafe { configure(self.self_, configs.get().cast(), i8::from(is_key), id) };
        true
    }

    /// `close` for the `_cb` twin of an asynchronous implementation: `false`
    /// for the Java default, `sink` dropped unfired.
    fn start_close_with_sink(&self, sink: ResultSink) -> bool {
        let Mode::Async(AsyncFns { close: Some(close), .. }, results) = &self.mode else {
            return false;
        };
        let id = results.register(sink);
        // SAFETY: `close` and `self.self_` are the pair the C caller registered together through
        // `kafka_common_serialization_Deserializer_new_async`.
        unsafe { close(self.self_, id) };
        true
    }
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out (`__as_Deserializer`) is never passed here, nor a handle a client
/// consumed. Every asynchronous call made through the handle must have
/// reported.
///
/// # Safety
///
/// `self_` must be null or an owned deserializer handle neither destroyed
/// nor consumed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Deserializer_destroy(
    self_: *mut kafka_common_serialization_Deserializer_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` is an owned handle neither destroyed nor
        // consumed: the `Arc::into_raw` of `DeserializerInner::owned`, whose reference the C caller
        // still holds; it is released exactly once here.
        drop(unsafe { Arc::from_raw(self_.cast_const().cast::<DeserializerInner>()) });
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::{CStr, CString};
    use std::sync::Mutex;

    use super::*;
    use crate::common::header::Headers;
    use crate::ffi::common::header::internals::record_headers::{
        box_record_headers, kafka_common_header_internals_RecordHeaders_destroy,
    };
    use crate::ffi::common::{
        kafka_common_Error_destroy, kafka_common_Error_message, kafka_common_Error_serialization,
    };
    use crate::ffi::util::{
        kafka_Map_key, kafka_Map_new, kafka_Map_put, kafka_Map_size, kafka_Map_value, kafka_string_destroy,
    };

    // A C deserializer producing an owned `char *` with the bytes
    // upper-cased, logging what it was asked.
    #[derive(Default)]
    struct Upper {
        calls: Mutex<Vec<String>>,
    }
    fn upper<'a>(self_: *mut c_void) -> &'a Upper {
        // SAFETY: Test code: `self_` is the `&imp` the tests register, which outlives every call
        // that passes it back.
        unsafe { &*(self_ as *const Upper) }
    }
    fn log(self_: *mut c_void, entry: String) {
        upper(self_).calls.lock().unwrap().push(entry);
    }
    fn text(data: kafka_Bytes_t) -> String {
        // SAFETY: Test code: `data` is a view the invoker built over a live slice, read during the
        // call.
        String::from_utf8(unsafe { slice(data) }.to_vec()).unwrap()
    }
    fn write_upper(data: kafka_Bytes_t, out_err: *mut *mut kafka_common_Error_t) -> *mut c_void {
        let text = text(data);
        if text == "fail" {
            let message = CString::new("cannot deserialize fail").unwrap();
            // SAFETY: Test code: `message` outlives the constructor, which copies it; `out_err` is
            // the valid slot pointing at null the invoker passes, and the error stored there is
            // handed to Rust, which consumes it.
            unsafe { *out_err = kafka_common_Error_serialization(message.as_ptr()) };
            return ptr::null_mut();
        }
        into_c_string(&text.to_uppercase()) as *mut c_void
    }
    unsafe extern "C" fn c_deserialize(
        self_: *mut c_void,
        topic: *const c_char,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        log(
            self_,
            format!(
                "deserialize {} {}",
                // SAFETY: Test code: `topic` is the `CString` the invoker passes, alive for the
                // call.
                unsafe { CStr::from_ptr(topic) }.to_str().unwrap(),
                text(data)
            ),
        );
        write_upper(data, out_err)
    }
    unsafe extern "C" fn c_deserialize_with_headers(
        self_: *mut c_void,
        _topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: `headers` is the record-headers copy the invoker passes, alive and
        // unchanged for the call.
        let has_k = unsafe { record_headers_ref(headers) }.last_header("k").is_some();
        log(self_, format!("deserialize_with_headers k={has_k}"));
        write_upper(data, out_err)
    }
    unsafe extern "C" fn c_deserialize_with_headers_bytes(
        self_: *mut c_void,
        _topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: `headers` is the record-headers copy the invoker passes, alive and
        // unchanged for the call.
        let has_k = unsafe { record_headers_ref(headers) }.last_header("k").is_some();
        log(self_, format!("deserialize_with_headers_bytes {} k={has_k}", text(data)));
        write_upper(data, out_err)
    }
    unsafe extern "C" fn c_configure(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8) {
        // SAFETY: Test code: `configs` is the map the invoker passes, alive and unchanged for the
        // call, whose keys and values are NUL-terminated strings.
        let entries: Vec<String> = (0..unsafe { kafka_Map_size(configs) })
            // SAFETY: Test code: `configs` is the map the invoker passes, alive and unchanged for
            // the call, and `i` is below its size, so each key and value is a NUL-terminated
            // string.
            .map(|i| unsafe {
                format!(
                    "{}={}",
                    CStr::from_ptr(kafka_Map_key(configs, i) as *const c_char).to_str().unwrap(),
                    CStr::from_ptr(kafka_Map_value(configs, i) as *const c_char).to_str().unwrap()
                )
            })
            .collect();
        log(self_, format!("configure {} is_key={is_key}", entries.join(",")));
    }
    unsafe extern "C" fn c_close(self_: *mut c_void) {
        log(self_, "close".to_string());
    }

    unsafe fn take_string(out: *mut c_void) -> String {
        // SAFETY: Test code: `out` is an owned `char *` a C deserializer of these tests returned from
        // `into_c_string`, read before `kafka_string_destroy` frees it once.
        let s = unsafe { CStr::from_ptr(out as *const c_char) }.to_str().unwrap().to_string();
        // SAFETY: Test code: `out` is an owned `char *` from `into_c_string`, not yet freed, and
        // not used after this call.
        unsafe { kafka_string_destroy(out as *mut c_char) };
        s
    }

    #[test]
    fn c_deserializer_is_reached_through_the_invokers() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Deserializer_new(
            &imp as *const Upper as *mut c_void,
            c_deserialize,
            Some(c_deserialize_with_headers),
            Some(c_deserialize_with_headers_bytes),
            Some(c_configure),
            Some(c_close),
        );
        let topic = CString::new("t").unwrap();
        let mut record_headers = RecordHeaders::new();
        record_headers.add_with_key_value("k", Some(b"v")).unwrap();
        let headers = box_record_headers(record_headers);
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_Deserializer_new` and
        // is destroyed once, at the end; `imp` outlives it; `headers` comes from
        // `box_record_headers` and is destroyed once, after the last call reading it; `topic` and
        // the arrays outlive the calls reading them; `err` is a valid slot pointing at null, left
        // null by every call (asserted), and every value returned is an owned string
        // that `take_string` frees once; `map` comes from `kafka_Map_new`, borrows the `CString`s,
        // which outlive it, and is destroyed once.
        unsafe {
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"hello"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "HELLO");

            let out = kafka_common_serialization_Deserializer_deserialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"hello"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "HELLO");

            let out = kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"world"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "WORLD");

            // Null data is an empty array.
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::NULL,
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "");

            let map = kafka_Map_new();
            let (k1, v1) = (CString::new("zeta").unwrap(), CString::new("1").unwrap());
            let (k2, v2) = (CString::new("alpha").unwrap(), CString::new("2").unwrap());
            kafka_Map_put(map, k1.as_ptr() as *mut c_void, v1.as_ptr() as *mut c_void);
            kafka_Map_put(map, k2.as_ptr() as *mut c_void, v2.as_ptr() as *mut c_void);
            kafka_common_serialization_Deserializer_configure(handle, map, 1);
            kafka_Map_destroy(map);
            kafka_common_serialization_Deserializer_close(handle);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Deserializer_destroy(handle);
            kafka_common_serialization_Deserializer_destroy(ptr::null_mut());
        }
        assert_eq!(
            *imp.calls.lock().unwrap(),
            [
                "deserialize t hello",
                "deserialize_with_headers k=true",
                "deserialize_with_headers_bytes world k=true",
                "deserialize t ",
                "configure alpha=2,zeta=1 is_key=1",
                "close",
            ]
        );
    }

    #[test]
    fn c_error_is_stored_owned_and_null_optional_methods_are_the_java_default() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Deserializer_new(
            &imp as *const Upper as *mut c_void,
            c_deserialize,
            None,
            None,
            None,
            None,
        );
        let topic = CString::new("t").unwrap();
        let headers = box_record_headers(RecordHeaders::new());
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_Deserializer_new` and
        // is destroyed once, at the end; `imp` outlives it; `headers` comes from
        // `box_record_headers` and is destroyed once, after the last call reading it; `topic` and
        // the arrays outlive the calls reading them; `err` is a valid slot pointing at null before
        // every call (reset after each error is destroyed), every value returned is an
        // owned string that `take_string` frees once, and each error is destroyed once, after it is
        // read.
        unsafe {
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"fail"),
                &raw mut err,
            );
            assert!(!err.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "cannot deserialize fail"
            );
            assert!(out.is_null(), "a failure returns NULL");
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();

            // Every optional method falls back to `deserialize`; one call at
            // a time, since each one owns the string it returns.
            let err_slot = &raw mut err;
            let fallbacks: [&dyn Fn() -> *mut c_void; 2] = [
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_with_headers(
                        handle,
                        topic.as_ptr(),
                        headers,
                        kafka_Bytes_t::from_slice(b"ok"),
                        err_slot,
                    )
                },
                &|| {
                    kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(
                        handle,
                        topic.as_ptr(),
                        headers,
                        kafka_Bytes_t::from_slice(b"ok"),
                        err_slot,
                    )
                },
            ];
            for fallback in fallbacks {
                let out = fallback();
                assert!(err.is_null());
                assert_eq!(take_string(out), "OK");
            }
            // No-ops: nothing to observe, nothing crashes.
            kafka_common_serialization_Deserializer_configure(handle, ptr::null(), 0);
            kafka_common_serialization_Deserializer_close(handle);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Deserializer_destroy(handle);
        }
        assert_eq!(
            *imp.calls.lock().unwrap(),
            ["deserialize t fail", "deserialize t ok", "deserialize t ok"]
        );
    }

    /// A call an asynchronous C deserializer of these tests was handed: the
    /// pointers it reads when it reports, kept as addresses so the call can
    /// cross to the reporting thread.
    struct AsyncCall {
        id: i64,
        topic: usize,
        data: usize,
        len: i32,
    }

    /// An asynchronous C deserializer producing an owned `char *` with the
    /// bytes upper-cased: it reports during the call (`inline`) or leaves
    /// the call for the test to report.
    struct AsyncUpper {
        handle: std::sync::atomic::AtomicPtr<kafka_common_serialization_Deserializer_t>,
        inline: bool,
        pending: Mutex<Vec<AsyncCall>>,
        calls: Mutex<Vec<String>>,
    }

    impl AsyncUpper {
        fn new(inline: bool) -> Self {
            Self {
                handle: std::sync::atomic::AtomicPtr::new(ptr::null_mut()),
                inline,
                pending: Mutex::default(),
                calls: Mutex::default(),
            }
        }

        /// Registers `self` through `_new_async`, every method set.
        fn register(&self) -> *mut kafka_common_serialization_Deserializer_t {
            let handle = kafka_common_serialization_Deserializer_new_async(
                self as *const Self as *mut c_void,
                async_deserialize,
                None,
                Some(async_deserialize_with_headers_bytes),
                Some(async_configure),
                Some(async_close),
            );
            self.handle.store(handle, std::sync::atomic::Ordering::SeqCst);
            handle
        }

        /// The next call left for the test, waiting for it up to 10 s.
        fn next_call(&self) -> AsyncCall {
            for _ in 0..10_000 {
                if let Some(call) = self.pending.lock().unwrap().pop() {
                    return call;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            panic!("no asynchronous call arrived");
        }

        fn handle(&self) -> *const kafka_common_serialization_Deserializer_t {
            self.handle.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    fn async_upper<'a>(self_: *mut c_void) -> &'a AsyncUpper {
        // SAFETY: Test code: `self_` is the `&AsyncUpper` the tests register, which outlives every
        // call that passes it back.
        unsafe { &*(self_ as *const AsyncUpper) }
    }

    /// Reads what `call` was handed and reports the upper-cased bytes, or an
    /// error for "fail".
    ///
    /// # Safety
    ///
    /// `call` must not have been reported yet: its pointers are then still
    /// kept alive by the handle.
    unsafe fn report(imp: &AsyncUpper, call: &AsyncCall) {
        // SAFETY: Per this function's `# Safety`, the call is unreported, so the topic and data it
        // was handed are still valid, as the `_async_fn_t` rustdoc states.
        let (topic, text) = unsafe {
            (
                CStr::from_ptr(call.topic as *const c_char).to_str().unwrap().to_string(),
                text(kafka_Bytes_t { data: call.data as *const u8, len: call.len }),
            )
        };
        imp.calls.lock().unwrap().push(format!("{topic} {text}"));
        let (value, error) = if text == "fail" {
            let message = CString::new("cannot deserialize fail").unwrap();
            // SAFETY: Test code: `message` outlives the constructor, which copies it.
            (ptr::null_mut(), unsafe { kafka_common_Error_serialization(message.as_ptr()) })
        } else {
            (into_c_string(&text.to_uppercase()) as *mut c_void, ptr::null_mut())
        };
        // SAFETY: Test code: the handle is alive until the test destroys it, after every report;
        // `error` is null or a fresh owned error, taken over.
        unsafe { kafka_common_serialization_Deserializer__set_callback_result(imp.handle(), call.id, value, error) };
    }

    fn take_call(self_: *mut c_void, call: AsyncCall) {
        let imp = async_upper(self_);
        if imp.inline {
            // SAFETY: Test code: the call is being made, so it is unreported.
            unsafe { report(imp, &call) };
        } else {
            imp.pending.lock().unwrap().push(call);
        }
    }

    unsafe extern "C" fn async_deserialize(self_: *mut c_void, topic: *const c_char, data: kafka_Bytes_t, id: i64) {
        take_call(
            self_,
            AsyncCall { id, topic: topic as usize, data: data.data as usize, len: data.len },
        );
    }

    unsafe extern "C" fn async_deserialize_with_headers_bytes(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: kafka_Bytes_t,
        id: i64,
    ) {
        // SAFETY: Test code: `headers` is the copy the handle keeps until the report.
        let has_k = unsafe { record_headers_ref(headers) }.last_header("k").is_some();
        async_upper(self_)
            .calls
            .lock()
            .unwrap()
            .push(format!("with_headers_bytes k={has_k}"));
        take_call(
            self_,
            AsyncCall { id, topic: topic as usize, data: data.data as usize, len: data.len },
        );
    }

    unsafe extern "C" fn async_configure(self_: *mut c_void, configs: *const kafka_Map_t, is_key: i8, id: i64) {
        let imp = async_upper(self_);
        // SAFETY: Test code: `configs` is the map the handle keeps until the report.
        let size = unsafe { kafka_Map_size(configs) };
        imp.calls.lock().unwrap().push(format!("configure {size} is_key={is_key}"));
        // SAFETY: Test code: the handle is alive; a `NULL` result is the success of a `void` method.
        unsafe {
            kafka_common_serialization_Deserializer__set_callback_result(
                imp.handle(),
                id,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
    }

    unsafe extern "C" fn async_close(self_: *mut c_void, id: i64) {
        let imp = async_upper(self_);
        imp.calls.lock().unwrap().push("close".to_string());
        // SAFETY: Test code: the handle is alive; a `NULL` result is the success of a `void` method.
        unsafe {
            kafka_common_serialization_Deserializer__set_callback_result(
                imp.handle(),
                id,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };
    }

    /// Polls `future` once, with a waker that does nothing.
    fn poll_once<F: Future + ?Sized>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    /// What a `_cb` completion of these tests received, and on which thread.
    type Completions = Mutex<Vec<(Result<String, String>, std::thread::ThreadId)>>;

    unsafe extern "C" fn record_completion(value: *mut c_void, error: *mut kafka_common_Error_t, opaque: *mut c_void) {
        // SAFETY: Test code: `opaque` is the `&Completions` the test passed, alive until the
        // completion fired; `error` is null or an owned error, read and freed once; `value` is
        // otherwise an owned string, freed once by `take_string`.
        let outcome = unsafe {
            if error.is_null() {
                Ok(take_string(value))
            } else {
                let message = CStr::from_ptr(kafka_common_Error_message(error)).to_str().unwrap().to_string();
                kafka_common_Error_destroy(error);
                Err(message)
            }
        };
        // SAFETY: As above.
        let completions = unsafe { &*(opaque as *const Completions) };
        completions.lock().unwrap().push((outcome, std::thread::current().id()));
    }

    /// Runs the blocking `deserialize` invoker on `data`.
    ///
    /// # Safety
    ///
    /// `handle` must be a valid deserializer handle.
    unsafe fn blocking_deserialize(
        handle: *const kafka_common_serialization_Deserializer_t,
        data: &[u8],
    ) -> Result<String, String> {
        let topic = CString::new("t").unwrap();
        let mut err = ptr::null_mut();
        // SAFETY: Per this function's `# Safety`, `handle` is valid; `topic` and `data` outlive the
        // call; `err` is a valid slot pointing at null; the value or error returned is owned and
        // freed once.
        unsafe {
            let out = kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(data),
                &raw mut err,
            );
            if err.is_null() {
                Ok(take_string(out))
            } else {
                assert!(out.is_null());
                let message = CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap().to_string();
                kafka_common_Error_destroy(err);
                Err(message)
            }
        }
    }

    #[test]
    fn async_c_deserializer_reporting_inside_the_call_is_reached_through_the_invokers() {
        let imp = AsyncUpper::new(true);
        let handle = imp.register();
        let topic = CString::new("t").unwrap();
        let mut record_headers = RecordHeaders::new();
        record_headers.add_with_key_value("k", Some(b"v")).unwrap();
        let headers = box_record_headers(record_headers);
        let completions = Completions::default();
        // SAFETY: Test code: `handle` comes from `_new_async` and is destroyed once, at the end,
        // after every report; `imp` outlives it; `headers` is destroyed once, after the last call
        // reading it; `topic` and the arrays outlive the calls; `err` is a valid slot pointing at
        // null, and the values returned are owned strings `take_string` frees once; `completions`
        // outlives every completion, which fires before `_cb` returns.
        unsafe {
            assert_eq!(blocking_deserialize(handle, b"hello"), Ok("HELLO".to_string()));
            assert_eq!(
                blocking_deserialize(handle, b"fail"),
                Err("cannot deserialize fail".to_string())
            );
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Deserializer_deserialize_with_headers_bytes(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"world"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "WORLD");
            // `deserialize_with_headers` is `NULL`: Java's default, `deserialize(topic, data)`.
            let out = kafka_common_serialization_Deserializer_deserialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                kafka_Bytes_t::from_slice(b"plain"),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(take_string(out), "PLAIN");

            kafka_common_serialization_Deserializer_deserialize_cb(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"cb"),
                record_completion,
                &completions as *const Completions as *mut c_void,
            );
            assert_eq!(
                *completions.lock().unwrap(),
                [(Ok("CB".to_string()), std::thread::current().id())],
                "an inline report fires the completion before `_cb` returns"
            );

            let map = kafka_Map_new();
            kafka_common_serialization_Deserializer_configure(handle, map, 1);
            kafka_Map_destroy(map);
            kafka_common_serialization_Deserializer_close(handle);

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Deserializer_destroy(handle);
        }
        assert_eq!(
            *imp.calls.lock().unwrap(),
            [
                "t hello",
                "t fail",
                "with_headers_bytes k=true",
                "t world",
                "t plain",
                "t cb",
                "configure 0 is_key=1",
                "close",
            ]
        );
    }

    #[test]
    fn async_c_deserializer_reporting_later_from_another_thread() {
        let imp = AsyncUpper::new(false);
        let handle = SendPtr(imp.register().cast());
        let reporter = |count: usize| {
            let imp = &imp;
            move || {
                for _ in 0..count {
                    let call = imp.next_call();
                    // SAFETY: Test code: the call was just taken, unreported.
                    unsafe { report(imp, &call) };
                }
                std::thread::current().id()
            }
        };
        std::thread::scope(|scope| {
            // The blocking invoker parks the calling thread until the report.
            let thread = scope.spawn(reporter(2));
            // SAFETY: Test code: the handle is valid until destroyed below.
            unsafe {
                assert_eq!(blocking_deserialize(handle.get().cast(), b"late"), Ok("LATE".to_string()));
                assert_eq!(
                    blocking_deserialize(handle.get().cast(), b"fail"),
                    Err("cannot deserialize fail".to_string())
                );
            }
            thread.join().unwrap();

            // The `_cb` twin returns at once; the completion fires on the reporting thread.
            let completions = Completions::default();
            let topic = CString::new("t").unwrap();
            // SAFETY: Test code: the handle is valid; `topic` and the data are copied by `_cb`;
            // `completions` outlives the completion, which fires before the join below.
            unsafe {
                kafka_common_serialization_Deserializer_deserialize_cb(
                    handle.get().cast(),
                    topic.as_ptr(),
                    kafka_Bytes_t::from_slice(b"cb"),
                    record_completion,
                    &completions as *const Completions as *mut c_void,
                );
            }
            drop(topic);
            assert!(completions.lock().unwrap().is_empty(), "nothing reported yet");
            let reporting = scope.spawn(reporter(1)).join().unwrap();
            assert_eq!(*completions.lock().unwrap(), [(Ok("CB".to_string()), reporting)]);

            // The trait's future, as a consumer awaits it.
            let thread = scope.spawn(reporter(1));
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let value = runtime.block_on(async {
                // SAFETY: Test code: the handle is valid until destroyed below.
                let inner = unsafe { deserializer_ref(handle.get().cast()) };
                let guard = inner.imp().lock().await;
                let mut headers = RecordHeaders::new();
                headers.add_with_key_value("k", Some(b"v")).unwrap();
                let data = Bytes::from(b"async".to_vec());
                Deserializer::deserialize_with_headers_bytes(&*guard, "t", &headers, &data).await
            });
            thread.join().unwrap();
            // SAFETY: Test code: an owned string the implementation reported, freed once.
            assert_eq!(unsafe { take_string(value.unwrap().as_ptr()) }, "ASYNC");
        });
        // SAFETY: Test code: every call reported; the handle is destroyed once.
        unsafe { kafka_common_serialization_Deserializer_destroy(handle.get().cast()) };
        assert_eq!(
            *imp.calls.lock().unwrap(),
            ["t late", "t fail", "t cb", "with_headers_bytes k=true", "t async"]
        );
    }

    #[test]
    fn a_second_report_is_ignored_and_its_error_freed() {
        let imp = AsyncUpper::new(false);
        let handle = imp.register();
        let completions = Completions::default();
        let topic = CString::new("t").unwrap();
        // SAFETY: Test code: the handle is valid until destroyed at the end; `completions`
        // outlives every completion; the extra reports pass a null value or a fresh owned error.
        unsafe {
            kafka_common_serialization_Deserializer_deserialize_cb(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"once"),
                record_completion,
                &completions as *const Completions as *mut c_void,
            );
            let call = imp.next_call();
            report(&imp, &call);
            let message = CString::new("too late").unwrap();
            kafka_common_serialization_Deserializer__set_callback_result(
                handle,
                call.id,
                ptr::null_mut(),
                kafka_common_Error_serialization(message.as_ptr()),
            );
            kafka_common_serialization_Deserializer__set_callback_result(
                handle,
                call.id,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            // An id the handle never issued is ignored too.
            kafka_common_serialization_Deserializer__set_callback_result(
                handle,
                12345,
                ptr::null_mut(),
                ptr::null_mut(),
            );
        }
        assert_eq!(completions.lock().unwrap().len(), 1, "the completion fires once");
        assert_eq!(completions.lock().unwrap()[0].0, Ok("ONCE".to_string()));

        // The same through the trait's future: the first report wins.
        let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let value = runtime.block_on(async {
            // SAFETY: Test code: the handle is valid until destroyed below.
            let inner = unsafe { deserializer_ref(handle) };
            let guard = inner.imp().lock().await;
            let mut future = std::pin::pin!(Deserializer::deserialize(&*guard, "t", b"first"));
            assert!(poll_once(future.as_mut()).is_pending());
            let call = imp.next_call();
            // SAFETY: Test code: the call is unreported; the second report passes a fresh owned error.
            unsafe {
                report(&imp, &call);
                let message = CString::new("second").unwrap();
                kafka_common_serialization_Deserializer__set_callback_result(
                    handle,
                    call.id,
                    ptr::null_mut(),
                    kafka_common_Error_serialization(message.as_ptr()),
                );
            }
            future.await
        });
        // SAFETY: Test code: an owned string the implementation reported, freed once; the handle is
        // destroyed once, after every report.
        unsafe {
            assert_eq!(take_string(value.unwrap().as_ptr()), "FIRST");
            kafka_common_serialization_Deserializer_destroy(handle);
        }
    }

    #[test]
    fn is_async_tells_the_modes_apart() {
        let asynchronous = AsyncUpper::new(true);
        let async_handle = asynchronous.register();
        let sync_handle =
            kafka_common_serialization_Deserializer_new(ptr::null_mut(), c_deserialize, None, None, None, None);
        let class = crate::ffi::common::serialization::DeserializerHandle::boxed(FfiDeserializer::String(
            GenericStringDeserializer::default(),
        ));
        // SAFETY: Test code: each handle is valid and destroyed once, at the end; the view is the
        // class handle's, destroyed with it.
        unsafe {
            let view = (*class).as_deserializer();
            assert_eq!(kafka_common_serialization_Deserializer__is_async(async_handle), 1);
            assert_eq!(kafka_common_serialization_Deserializer__is_async(sync_handle), 0);
            assert_eq!(kafka_common_serialization_Deserializer__is_async(view), 0, "a built-in's view");
            // A report on a handle that is not asynchronous is ignored, its error freed.
            let message = CString::new("ignored").unwrap();
            kafka_common_serialization_Deserializer__set_callback_result(
                sync_handle,
                1,
                ptr::null_mut(),
                kafka_common_Error_serialization(message.as_ptr()),
            );
            kafka_common_serialization_Deserializer_destroy(async_handle);
            kafka_common_serialization_Deserializer_destroy(sync_handle);
            crate::ffi::common::serialization::DeserializerHandle::destroy(class);
        }
    }

    /// A synchronous C deserializer recording the thread it runs on, and,
    /// when `gate` is set, waiting for it before reading its arguments.
    #[derive(Default)]
    struct ThreadRecorder {
        threads: Mutex<Vec<std::thread::ThreadId>>,
        gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        read: Mutex<Option<std::sync::mpsc::Sender<String>>>,
    }

    unsafe extern "C" fn record_thread(
        self_: *mut c_void,
        topic: *const c_char,
        data: kafka_Bytes_t,
        _out_err: *mut *mut kafka_common_Error_t,
    ) -> *mut c_void {
        // SAFETY: Test code: `self_` is the `&ThreadRecorder` the tests register, which outlives
        // every call.
        let recorder = unsafe { &*(self_ as *const ThreadRecorder) };
        recorder.threads.lock().unwrap().push(std::thread::current().id());
        let gate = recorder.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.recv().unwrap();
            // SAFETY: Test code: the arguments are borrowed for the call, which has not returned.
            let topic = unsafe { CStr::from_ptr(topic) };
            let read = format!("{} {}", topic.to_str().unwrap(), text(data));
            recorder.read.lock().unwrap().take().unwrap().send(read).unwrap();
        }
        ptr::null_mut()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_c_deserializer_runs_on_the_blocking_pool() {
        let recorder = ThreadRecorder::default();
        let handle = kafka_common_serialization_Deserializer_new(
            &recorder as *const ThreadRecorder as *mut c_void,
            record_thread,
            None,
            None,
            None,
            None,
        );
        // SAFETY: Test code: the handle is valid until destroyed at the end.
        let inner = unsafe { deserializer_ref(handle) };
        {
            let guard = inner.imp().lock().await;
            Deserializer::deserialize(&*guard, "t", b"x").await.unwrap();
        }
        let topic = CString::new("t").unwrap();
        let mut err = ptr::null_mut();
        // SAFETY: Test code: the handle is valid; `topic` and the data outlive the call; `err` is a
        // valid slot pointing at null, left null (the function returns a null value).
        let out = unsafe {
            kafka_common_serialization_Deserializer_deserialize(
                handle,
                topic.as_ptr(),
                kafka_Bytes_t::from_slice(b"y"),
                &raw mut err,
            )
        };
        assert!(out.is_null() && err.is_null());
        // SAFETY: Test code: every call returned; the handle is destroyed once.
        unsafe { kafka_common_serialization_Deserializer_destroy(handle) };
        let here = std::thread::current().id();
        let threads = recorder.threads.lock().unwrap().clone();
        assert_ne!(threads[0], here, "a consumer's call runs on the blocking pool");
        assert_eq!(threads[1], here, "the blocking invoker runs on its calling thread");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_dropped_sync_call_keeps_its_arguments_until_it_returns() {
        let recorder = ThreadRecorder::default();
        let (release, gate) = std::sync::mpsc::channel();
        let (read_tx, read_rx) = std::sync::mpsc::channel();
        *recorder.gate.lock().unwrap() = Some(gate);
        *recorder.read.lock().unwrap() = Some(read_tx);
        let handle = kafka_common_serialization_Deserializer_new(
            &recorder as *const ThreadRecorder as *mut c_void,
            record_thread,
            None,
            None,
            None,
            None,
        );
        // SAFETY: Test code: the handle is valid until destroyed at the end.
        let inner = unsafe { deserializer_ref(handle) };
        {
            let guard = inner.imp().lock().await;
            let topic = String::from("topic");
            let data = b"keep".to_vec();
            let mut future = Box::pin(Deserializer::deserialize(&*guard, &topic, &data));
            assert!(poll_once(future.as_mut()).is_pending(), "the C function waits for its gate");
            drop(future);
            drop((topic, data));
        }
        release.send(()).unwrap();
        let read = tokio::task::spawn_blocking(move || read_rx.recv_timeout(std::time::Duration::from_secs(10)))
            .await
            .unwrap()
            .unwrap();
        // SAFETY: Test code: the call returned (it sent what it read last); the handle is destroyed
        // once.
        unsafe { kafka_common_serialization_Deserializer_destroy(handle) };
        assert_eq!(read, "topic keep");
    }

    #[test]
    fn a_dropped_async_call_keeps_its_arguments_until_it_reports() {
        let imp = AsyncUpper::new(false);
        let handle = SendPtr(imp.register().cast());
        // SAFETY: Test code: the handle is valid until destroyed at the end.
        let inner = unsafe { deserializer_ref(handle.get().cast()) };
        let poll_and_drop = |future: std::pin::Pin<&mut dyn Future<Output = Result<GenericValue, Error>>>| {
            assert!(poll_once(future).is_pending());
        };
        {
            let guard = inner.imp().try_lock().unwrap();
            // A slice argument, copied into the slot.
            let (topic, data) = (String::from("slice"), b"kept".to_vec());
            poll_and_drop(std::pin::pin!(Deserializer::deserialize(&*guard, &topic, &data)));
            drop((topic, data));
            // A fetch buffer, shared with the slot, and headers, copied into it.
            let (topic, data) = (String::from("bytes"), Bytes::from(b"shared".to_vec()));
            let mut headers = RecordHeaders::new();
            headers.add_with_key_value("k", Some(b"v")).unwrap();
            poll_and_drop(std::pin::pin!(Deserializer::deserialize_with_headers_bytes(
                &*guard, &topic, &headers, &data
            )));
            drop((topic, data, headers));
        }
        std::thread::scope(|scope| {
            scope.spawn(|| {
                for _ in 0..2 {
                    let call = imp.next_call();
                    // SAFETY: Test code: the call is unreported: its future was dropped, and the slot
                    // keeps what it was handed until this report. The reported string is
                    // dropped with the outcome, unread: it leaks, as a test's C side may.
                    unsafe { report(&imp, &call) };
                }
            });
        });
        let mut calls = imp.calls.lock().unwrap().clone();
        calls.sort();
        assert_eq!(calls, ["bytes shared", "slice kept", "with_headers_bytes k=true"]);
        // SAFETY: Test code: every call reported; the handle is destroyed once.
        unsafe { kafka_common_serialization_Deserializer_destroy(handle.get().cast()) };
    }
}

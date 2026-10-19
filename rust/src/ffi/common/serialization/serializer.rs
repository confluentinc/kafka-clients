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

//! `kafka_common_serialization_Serializer_t`: the
//! `org.apache.kafka.common.serialization.Serializer<T>` interface
//! (CLAUDE.md §4, "Traits").
//!
//! `T` is a `void *` (§4, "Generic types"): `data` is whatever the
//! implementation reads, `NULL` being Java's `null`; it must stay valid and
//! unchanged until the implementation has returned (synchronous mode) or
//! reported (asynchronous mode). A C implementation hands back the serialized
//! bytes as a `kafka_Bytes_t` that Rust copies before the function returns
//! (synchronous: the returned array, which must stay valid and unwritten until
//! the function returns to Rust) or before `__set_callback_result` returns
//! (asynchronous: the `kafka_Bytes_t *` passed as the result); `data == NULL`
//! is Java's `null` array.
//!
//! The invokers deliver the bytes the other way round: the `kafka_Bytes_t`
//! they return borrows a buffer the handle keeps, valid until the next
//! `serialize*` call on the same handle or its destruction — the same
//! lifetime a `const char *` getter gives. The `_cb` twins pass their
//! completion a `kafka_Bytes_t` valid only during that completion.
//!
//! `Serializer.configure` and `Serializer.close` have no Rust counterpart
//! (the Rust trait omits them), so neither has a C one.
//!
//! A C implementation is registered synchronous
//! ([`kafka_common_serialization_Serializer_new`]: a producer calls it on
//! Tokio's blocking pool) or asynchronous
//! ([`kafka_common_serialization_Serializer_new_async`]: it reports through
//! [`kafka_common_serialization_Serializer__set_callback_result`]). The
//! invokers call a synchronous one on their calling thread and park that
//! thread until an asynchronous one reports; the `_cb` twins fire their
//! completion on the calling thread, or, for an asynchronous implementation,
//! on the thread that reports.

#![expect(non_camel_case_types)]

use std::borrow::Cow;
use std::ffi::{c_char, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, PoisonError};

use futures_util::FutureExt;

use crate::common::Error;
use crate::common::header::RecordHeaders;
use crate::common::serialization::Serializer;
use crate::ffi::callback_queue::SendPtr;
use crate::ffi::callback_result::{ResultSink, WaitReport};
use crate::ffi::common::header::internals::record_headers::{
    RecordHeadersInner, kafka_common_header_internals_RecordHeaders_t, record_headers_ref,
};
use crate::ffi::common::serialization::byte_array_serializer::GenericByteArraySerializer;
use crate::ffi::common::serialization::string_serializer::GenericStringSerializer;
use crate::ffi::common::serialization::{
    Call, CallArgs, Dispatch, Results, block_on_thread, dispatch, ready, send_ptr,
};
use crate::ffi::common::{box_error, deliver_value, kafka_common_Error_t, take_error};
use crate::ffi::ffi_guard;
use crate::ffi::util::{GenericValue, c_str_to_string, kafka_Bytes_t, owned_c_string};

/// Opaque handle to a [`Serializer`] implementation over the `void *`
/// representation.
#[repr(C)]
pub struct kafka_common_serialization_Serializer_t {
    _private: [u8; 0],
}

/// Every serializer a C handle can stand for: a C implementation or a
/// built-in class. A `NULL` handle has none: the producer boxes its
/// `BytesPassthrough` directly.
///
/// Has no Java counterpart (`definition-of-done.md` §7): it replaces a
/// `dyn Serializer` so that each call awaits the variant's concrete future,
/// which keeps the future of a wrapper around it (the producer's
/// [`ArcSerializer`](crate::ffi::producer::kafka_producer::ArcSerializer))
/// small enough to be stored inline by `DynSerializer`.
pub(crate) enum FfiSerializer {
    /// A C implementation registered with
    /// [`kafka_common_serialization_Serializer_new`] or
    /// [`kafka_common_serialization_Serializer_new_async`].
    C(CSerializer),
    /// `kafka_common_serialization_StringSerializer_t`.
    String(GenericStringSerializer),
    /// `kafka_common_serialization_ByteArraySerializer_t`.
    ByteArray(GenericByteArraySerializer),
}

impl Serializer<GenericValue> for FfiSerializer {
    async fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        match self {
            Self::C(serializer) => serializer.serialize(topic, data).await,
            Self::String(serializer) => serializer.serialize(topic, data).await,
            Self::ByteArray(serializer) => serializer.serialize(topic, data).await,
        }
    }

    async fn serialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: Option<&'a GenericValue>,
    ) -> Result<Option<Cow<'a, [u8]>>, Error> {
        match self {
            Self::C(serializer) => serializer.serialize_with_headers(topic, headers, data).await,
            Self::String(serializer) => serializer.serialize_with_headers(topic, headers, data).await,
            Self::ByteArray(serializer) => serializer.serialize_with_headers(topic, headers, data).await,
        }
    }
}

impl FfiSerializer {
    /// Serializes for an invoker, on the calling thread: a C implementation
    /// is called there (synchronous) or waited for there (asynchronous), a
    /// built-in completes at once.
    fn invoke(
        &self,
        topic: &str,
        headers: Option<&RecordHeaders>,
        data: Option<&GenericValue>,
    ) -> Result<Option<Vec<u8>>, Error> {
        match self {
            Self::C(serializer) => block_on_thread(serializer.start(Dispatch::Inline, topic, headers, data)),
            other => {
                let output = match headers {
                    Some(headers) => ready(other.serialize_with_headers(topic, headers, data))?,
                    None => ready(other.serialize(topic, data))?,
                };
                Ok(output?.map(Cow::into_owned))
            },
        }
    }

    /// Whether this is an asynchronous C implementation.
    fn is_async(&self) -> bool {
        matches!(self, Self::C(CSerializer { mode: Mode::Async { .. }, .. }))
    }
}

/// `byte[] serialize(String topic, T data)` of a synchronous C
/// implementation: `data` is the `void *` (`NULL` for Java's `null`); the
/// bytes it returns are copied once the function returns, on the same thread
/// and before it is called again there, so they must stay valid and unwritten
/// until then (not on the function's stack). A returned `data == NULL` stands
/// for a `null` array. On failure it stores an owned error in `*out_err`,
/// which points at `NULL` on entry, and its return value is ignored.
pub type kafka_common_serialization_Serializer_serialize_fn_t = unsafe extern "C" fn(
    self_: *mut c_void,
    topic: *const c_char,
    data: *const c_void,
    out_err: *mut *mut kafka_common_Error_t,
) -> kafka_Bytes_t;

/// `byte[] serialize(String topic, Headers headers, T data)` of a
/// synchronous C implementation; `headers` is a copy borrowed for the call.
/// `NULL` is the Java default: `serialize(topic, data)`.
pub type kafka_common_serialization_Serializer_serialize_with_headers_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> kafka_Bytes_t,
>;

/// `byte[] serialize(String topic, T data)` of an asynchronous C
/// implementation: returns at once and reports through
/// [`kafka_common_serialization_Serializer__set_callback_result`] with
/// `callback_id`, from any thread, during the call or later, a
/// `kafka_Bytes_t *` result (copied before that call returns; `NULL`, or a
/// `data == NULL` array, for Java's `null`) or an owned error. `topic` and
/// `data` stay valid until it reports.
pub type kafka_common_serialization_Serializer_serialize_async_fn_t =
    unsafe extern "C" fn(self_: *mut c_void, topic: *const c_char, data: *const c_void, callback_id: i64);

/// `byte[] serialize(String topic, Headers headers, T data)` of an
/// asynchronous C implementation, reporting as
/// [`kafka_common_serialization_Serializer_serialize_async_fn_t`]; `headers`
/// is a copy, valid like the other arguments until it reports. `NULL` is the
/// Java default: `serialize(topic, data)`.
pub type kafka_common_serialization_Serializer_serialize_with_headers_async_fn_t = Option<
    unsafe extern "C" fn(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        callback_id: i64,
    ),
>;

/// What a [`kafka_common_serialization_Serializer_t`] points at: the
/// implementation, who owns the handle, and the bytes the last invoker
/// returned. The last two are what keeps this from being a plain
/// [`Interface`](crate::ffi::common::metrics::Interface): the `kafka_Bytes_t`
/// an invoker writes is a view, so the handle must own what it points into.
///
/// Lives in an `Arc`: the handle is `Arc::into_raw` (owned) or the `Arc` a
/// class handle caches (view), and a client consuming the handle keeps the
/// `Arc`, so the pointer stays valid for `__set_callback_result` until the
/// client is destroyed.
pub(crate) struct SerializerInner {
    imp: Arc<FfiSerializer>,
    /// `true` for a handle the C caller owns (`_new`, `_new_async`), `false`
    /// for the view a class handle caches (`__as_Serializer`).
    owned: bool,
    /// The bytes the last `serialize*` invoker returned, borrowed by the
    /// `kafka_Bytes_t` it wrote: valid until the next invoker call on this
    /// handle or its destruction.
    output: Mutex<Option<Vec<u8>>>,
}

impl SerializerInner {
    /// A handle the C caller owns.
    pub(crate) fn owned(imp: Arc<FfiSerializer>) -> *mut kafka_common_serialization_Serializer_t {
        Arc::into_raw(Arc::new(Self { imp, owned: true, output: Mutex::new(None) }))
            .cast_mut()
            .cast()
    }

    /// The view a class handle caches.
    pub(crate) fn view(imp: Arc<FfiSerializer>) -> Arc<Self> {
        Arc::new(Self { imp, owned: false, output: Mutex::new(None) })
    }

    /// The implementation.
    pub(crate) fn get(&self) -> &FfiSerializer {
        &self.imp
    }

    /// Whether this is a C implementation, synchronous or asynchronous, as
    /// opposed to a built-in class: a producer whose serializers include one
    /// waits for them on its runtime, never on the thread of a `_cb` call
    /// (see `ffi::producer`, "`send_cb` tries the producer first").
    pub(crate) fn is_c(&self) -> bool {
        matches!(*self.imp, FfiSerializer::C(_))
    }

    /// A borrowed handle on `self`, valid as long as `self`.
    pub(crate) fn as_ptr(&self) -> *const kafka_common_serialization_Serializer_t {
        (self as *const Self).cast()
    }

    /// Keeps `bytes` as the last output and returns the view over them.
    fn store(&self, bytes: Option<Vec<u8>>) -> kafka_Bytes_t {
        let mut output = self.output.lock().unwrap_or_else(PoisonError::into_inner);
        *output = bytes;
        kafka_Bytes_t::from_option(output.as_deref())
    }
}

/// The handle behind a pointer.
///
/// # Safety
///
/// `serializer` must be a valid serializer handle, alive for `'a`.
pub(crate) unsafe fn serializer_ref<'a>(
    serializer: *const kafka_common_serialization_Serializer_t,
) -> &'a SerializerInner {
    // SAFETY: Per this function's `# Safety`, `serializer` is a valid serializer handle alive for
    // `'a`, so it points at a live `SerializerInner` inside an `Arc`: the one
    // `SerializerInner::owned` handed out, kept by the C caller or the client that consumed it, or
    // the view a `SerializerHandle` caches. A `SerializerInner` is changed only through its `output`
    // mutex, so the shared reference is sound for `'a`.
    unsafe { &*serializer.cast::<SerializerInner>() }
}

/// Takes the handle a `*mut` parameter received: an owned handle's reference
/// passes to the caller, a view's is cloned and the view stays with its class
/// handle. The consumers are `KafkaProducer_new` and the `MockProducer`
/// options (CLAUDE.md §4 rule 6); they keep the returned `Arc`, so the
/// handle pointer stays valid for `__set_callback_result` until that client
/// is destroyed.
///
/// # Safety
///
/// `serializer` must be a valid serializer handle; when it was owned, the
/// caller passes it on to nothing else but `__set_callback_result`.
pub(crate) unsafe fn take_serializer(serializer: *mut kafka_common_serialization_Serializer_t) -> Arc<SerializerInner> {
    let inner = serializer.cast_const().cast::<SerializerInner>();
    // SAFETY: Per this function's `# Safety`, `serializer` is a valid serializer handle: a pointer
    // into a live `Arc<SerializerInner>`, `Arc::into_raw` (owned) or `Arc::as_ptr` (view). For an
    // owned one the caller's reference is taken over, once, as the caller does not consume it
    // again; a view gains a reference first, so the class handle keeps its own.
    unsafe {
        if !(*inner).owned {
            Arc::increment_strong_count(inner);
        }
        Arc::from_raw(inner)
    }
}

/// How a C implementation is called.
enum Mode {
    /// [`kafka_common_serialization_Serializer_new`].
    Sync {
        serialize: kafka_common_serialization_Serializer_serialize_fn_t,
        serialize_with_headers: kafka_common_serialization_Serializer_serialize_with_headers_fn_t,
    },
    /// [`kafka_common_serialization_Serializer_new_async`], with the registry
    /// its reports go to.
    Async {
        serialize: kafka_common_serialization_Serializer_serialize_async_fn_t,
        serialize_with_headers: kafka_common_serialization_Serializer_serialize_with_headers_async_fn_t,
        results: Results,
    },
}

/// A C implementation of [`Serializer`]; a null optional method is the Java
/// default.
pub(crate) struct CSerializer {
    self_: *mut c_void,
    mode: Mode,
}

// SAFETY: `self_` and the function pointers are what the C caller registered through
// `kafka_common_serialization_Serializer_new` or `_new_async`: the C user is responsible for the
// thread-safety of `self_`, which those functions' rustdoc states, and the function pointers are
// trivially shareable; the registry is `Send`. `CSerializer` itself never dereferences `self_`; it
// only passes it back to the functions registered with it.
unsafe impl Send for CSerializer {}
// SAFETY: `Sync` is required by the `Serializer` trait. A `&CSerializer` exposes only the opaque
// `self_`, the `Copy` function pointers, which the type never dereferences, and the `Sync`
// registry; it is shared across threads with no lock, as the registering functions' rustdoc states,
// and the C user is responsible for the thread-safety of `self_` under those concurrent calls.
unsafe impl Sync for CSerializer {}

/// The `void *` for `data`: null for Java's `null`.
fn data_ptr(data: Option<&GenericValue>) -> *const c_void {
    data.map_or(ptr::null(), |value| value.as_ptr().cast_const())
}

/// Reads a synchronous C implementation's result: the error it stored, or a
/// copy of the bytes it returned (`None` for a `NULL` array).
///
/// # Safety
///
/// `error` must be what the C implementation stored in its `out_err` slot,
/// null or an owned error; `out` what it returned: null or `len` readable
/// bytes.
unsafe fn collect(error: *mut kafka_common_Error_t, out: kafka_Bytes_t) -> Result<Option<Vec<u8>>, Error> {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error handle, the
    // `box_error` handle of a `kafka_common_Error_*` constructor or a fallible FFI function that
    // the C implementation handed over, not yet destroyed, as `take_error` requires; it is consumed
    // here.
    match unsafe { take_error(error) } {
        Some(error) => Err(error),
        // SAFETY: Per this function's `# Safety`, `out` is what the C implementation wrote: null or
        // `len` readable bytes, which the module rustdoc keeps valid and unwritten until Rust has
        // copied them, as `kafka_Bytes_t::as_slice` requires; `to_vec` copies them before this call
        // returns.
        None => Ok(unsafe { out.as_slice() }.map(<[u8]>::to_vec)),
    }
}

/// The bytes behind an asynchronous report's `kafka_Bytes_t *` result,
/// borrowed for the report: `None` for a `NULL` pointer or array.
///
/// # Safety
///
/// `result` must be null or point at a valid `kafka_Bytes_t` whose bytes stay
/// valid for `'a`.
unsafe fn reported_bytes<'a>(result: *mut c_void) -> Option<&'a [u8]> {
    // SAFETY: Per this function's `# Safety`, `result` is null or points at a valid `kafka_Bytes_t`
    // whose bytes are readable for `'a`, as `as_ref` and `kafka_Bytes_t::as_slice` require.
    unsafe { result.cast::<kafka_Bytes_t>().as_ref().and_then(|bytes| bytes.as_slice()) }
}

/// Copies a report's bytes into the slot, on the reporting thread, while the
/// C buffer is still valid.
fn copy_output(args: &mut CallArgs, result: *mut c_void) {
    // SAFETY: `result` is what the asynchronous implementation passed to `__set_callback_result`,
    // which runs this conversion before it returns: null or a `kafka_Bytes_t *` valid for that call,
    // as the `_async_fn_t` rustdoc requires.
    args.output = unsafe { reported_bytes(result) }.map(<[u8]>::to_vec);
}

/// The bytes of a report, copied by [`copy_output`].
fn finish_output(args: &mut CallArgs, result: Result<*mut c_void, Error>) -> Result<Option<Vec<u8>>, Error> {
    result.map(|_| args.output.take())
}

impl CSerializer {
    /// Starts `serialize` (`headers` is `None`) or
    /// `serialize_with_headers`. A synchronous implementation runs where
    /// `dispatch` says; an asynchronous one is called at once, its arguments
    /// kept in the registry slot until it reports.
    fn start(
        &self,
        dispatch_to: Dispatch,
        topic: &str,
        headers: Option<&RecordHeaders>,
        data: Option<&GenericValue>,
    ) -> Call<'_, Option<Vec<u8>>> {
        let self_ = SendPtr(self.self_);
        let data = send_ptr(data_ptr(data));
        match &self.mode {
            Mode::Sync { serialize, serialize_with_headers } => {
                let topic = owned_c_string(topic);
                match (headers, *serialize_with_headers) {
                    (Some(headers), Some(function)) => {
                        // A copy: the class handle owns its headers.
                        let headers = RecordHeadersInner::new(headers.clone());
                        dispatch(dispatch_to, move || {
                            let mut error = ptr::null_mut();
                            // SAFETY: `function` and `self_` are the pair the C caller registered
                            // together through `kafka_common_serialization_Serializer_new`, whose
                            // rustdoc has the caller keep `self_` alive until every call made
                            // through the handle has returned. `topic` and `headers` are owned by
                            // this closure; `data` is the caller's value, which the module rustdoc
                            // keeps valid until the function returns; `out_err` is a local slot
                            // pointing at null.
                            let out = unsafe {
                                function(
                                    self_.get(),
                                    topic.as_ptr(),
                                    headers.as_ptr(),
                                    data.get().cast_const(),
                                    &raw mut error,
                                )
                            };
                            // SAFETY: `error` is what the C function stored in its slot and `out`
                            // what it returned, valid until Rust has copied it, which `collect`
                            // does before returning.
                            unsafe { collect(error, out) }
                        })
                    },
                    _ => {
                        let function = *serialize;
                        dispatch(dispatch_to, move || {
                            let mut error = ptr::null_mut();
                            // SAFETY: As for the header form, without `headers`.
                            let out = unsafe {
                                function(self_.get(), topic.as_ptr(), data.get().cast_const(), &raw mut error)
                            };
                            // SAFETY: As for the header form.
                            unsafe { collect(error, out) }
                        })
                    },
                }
            },
            Mode::Async { serialize, serialize_with_headers, results } => {
                let with_headers = headers.filter(|_| serialize_with_headers.is_some());
                let (id, (topic, headers)) = results.register_waiting(copy_output, |args| {
                    (args.set_topic(topic), with_headers.map(|headers| args.set_headers(headers)))
                });
                // SAFETY: The function and `self_` are the pair the C caller registered together
                // through `kafka_common_serialization_Serializer_new_async`, whose rustdoc has the
                // caller keep `self_` alive until every call made through the handle has reported.
                // `topic` and `headers` point into the registry slot's `CallArgs`, kept until the
                // report for `id` even if the returned future is dropped first; `data` is the
                // caller's value, which the module rustdoc keeps valid until the report. The
                // registry's lock is not held, so the implementation may report during the call.
                unsafe {
                    match (headers, *serialize_with_headers) {
                        (Some(headers), Some(function)) => {
                            function(self_.get(), topic, headers, data.get().cast_const(), id);
                        },
                        _ => serialize(self_.get(), topic, data.get().cast_const(), id),
                    }
                }
                Call::Wait(WaitReport::new(results, id, finish_output))
            },
        }
    }
}

// Each method returns the `Call` future itself, mapped, rather than awaiting
// it in an `async fn`, so the producer's wrapper stays within
// `InlineFuture`'s buffer.
impl Serializer<GenericValue> for CSerializer {
    fn serialize<'a>(
        &'a self,
        topic: &'a str,
        data: Option<&'a GenericValue>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send + 'a {
        self.start(Dispatch::Blocking, topic, None, data)
            .map(|result| result.map(|bytes| bytes.map(Cow::Owned)))
    }

    fn serialize_with_headers<'a>(
        &'a self,
        topic: &'a str,
        headers: &'a RecordHeaders,
        data: Option<&'a GenericValue>,
    ) -> impl Future<Output = Result<Option<Cow<'a, [u8]>>, Error>> + Send + 'a {
        self.start(Dispatch::Blocking, topic, Some(headers), data)
            .map(|result| result.map(|bytes| bytes.map(Cow::Owned)))
    }
}

/// Registers a synchronous C implementation of `Serializer`; `serialize` is
/// required, the other pointer may be `NULL` for the Java default. The
/// caller owns `self_` and keeps it alive until the handle is destroyed with
/// [`kafka_common_serialization_Serializer_destroy`] or, once a client
/// consumed it, until that client is destroyed, and in any case until every
/// call made through the handle has returned. Nothing serialises the calls:
/// the functions may be called from any thread, concurrently, so the C
/// caller is responsible for their thread-safety on `self_`. A producer calls
/// them on Tokio's blocking pool, so a slow implementation holds no runtime
/// worker; the invokers call them on their calling thread.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_Serializer_new(
    self_: *mut c_void,
    serialize: kafka_common_serialization_Serializer_serialize_fn_t,
    serialize_with_headers: kafka_common_serialization_Serializer_serialize_with_headers_fn_t,
) -> *mut kafka_common_serialization_Serializer_t {
    SerializerInner::owned(Arc::new(FfiSerializer::C(CSerializer {
        self_,
        mode: Mode::Sync { serialize, serialize_with_headers },
    })))
}

/// Registers an asynchronous C implementation of `Serializer`; `serialize`
/// is required, the other pointer may be `NULL` for the Java default. Each
/// function returns at once and reports its outcome through
/// [`kafka_common_serialization_Serializer__set_callback_result`] on this
/// handle with the `callback_id` it was given, from any thread, during the
/// call or later; the pointers it was given, the caller's `data` included,
/// stay valid until it reports. The handle stays valid for that report until
/// it is destroyed with [`kafka_common_serialization_Serializer_destroy`] or,
/// once a client consumed it, until that client is destroyed: every call must
/// report before then. The caller owns `self_` and keeps it alive until every
/// call made through the handle has reported. The functions may be called
/// from any thread, concurrently, so the C caller is responsible for their
/// thread-safety on `self_`. A producer's `send_cb` never calls them, or
/// waits for their report, on the thread calling `send_cb`, so they may
/// report from that thread once it returned (a single-threaded event loop);
/// a blocking `send` waits for the report on its calling thread.
#[ffi_guard]
#[unsafe(no_mangle)]
pub extern "C" fn kafka_common_serialization_Serializer_new_async(
    self_: *mut c_void,
    serialize: kafka_common_serialization_Serializer_serialize_async_fn_t,
    serialize_with_headers: kafka_common_serialization_Serializer_serialize_with_headers_async_fn_t,
) -> *mut kafka_common_serialization_Serializer_t {
    SerializerInner::owned(Arc::new(FfiSerializer::C(CSerializer {
        self_,
        mode: Mode::Async { serialize, serialize_with_headers, results: Results::with_keep_alive() },
    })))
}

/// Whether the handle stands for an asynchronous C implementation
/// ([`kafka_common_serialization_Serializer_new_async`]): 1, else 0, which
/// includes a built-in class's view.
///
/// # Safety
///
/// `self_` must be a valid serializer handle.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer__is_async(
    self_: *const kafka_common_serialization_Serializer_t,
) -> i8 {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle, as
    // `serializer_ref` requires; the reference is used only within this call.
    i8::from(unsafe { serializer_ref(self_) }.get().is_async())
}

/// Reports the outcome of the asynchronous call `callback_id`: on success
/// `error` is `NULL` and `result` a `kafka_Bytes_t *` (`NULL`, or a
/// `data == NULL` array, for Java's `null` array), whose bytes are copied
/// before this returns; on failure `error` is an owned error, taken over, and
/// `result` is ignored. Callable from any thread, during the call or later,
/// even after a client consumed the handle (until that client is destroyed).
/// A report for an unknown id, or a second one for the same id, is ignored
/// and its error freed; so is any report on a handle that is not
/// asynchronous.
///
/// # Safety
///
/// `self_` must be a valid serializer handle, or one a live client consumed,
/// `result` null or a valid `kafka_Bytes_t`, and `error` null or an owned
/// error.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer__set_callback_result(
    self_: *const kafka_common_serialization_Serializer_t,
    callback_id: i64,
    result: *mut c_void,
    error: *mut kafka_common_Error_t,
) {
    // SAFETY: Per this function's `# Safety`, `error` is null or an owned error, as `take_error`
    // requires; it is consumed here.
    let error = unsafe { take_error(error) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle, kept alive by
    // its owner or by the client holding its `Arc`, as `serializer_ref` requires.
    let serializer = unsafe { serializer_ref(self_) };
    if let FfiSerializer::C(CSerializer { mode: Mode::Async { results, .. }, .. }) = serializer.get() {
        results.complete(callback_id, result, error);
    }
}

/// Runs a serialize method on the implementation behind `self_` and returns
/// its bytes, borrowed from the handle until its next call; on failure stores
/// the error in `*out_err`.
///
/// # Safety
///
/// `self_` must be a valid serializer handle and `out_err` as
/// `deliver_value` requires.
unsafe fn invoke(
    self_: *const kafka_common_serialization_Serializer_t,
    out_err: *mut *mut kafka_common_Error_t,
    topic: &str,
    headers: Option<&RecordHeaders>,
    data: Option<&GenericValue>,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle, as
    // `serializer_ref` requires; the reference is used only within this call.
    let inner = unsafe { serializer_ref(self_) };
    let result = inner.get().invoke(topic, headers, data);
    // SAFETY: Per this function's `# Safety`, `out_err` is as `deliver_value` requires. The array
    // returned borrows the handle's `output`, which `store` keeps until the next `serialize*` call
    // on this handle or its destruction, the lifetime the exported invokers document.
    unsafe { deliver_value(result, out_err, |bytes| inner.store(bytes)) }
}

/// The `Option<&GenericValue>` for a `void *` parameter.
fn generic(data: *const c_void) -> Option<GenericValue> {
    (!data.is_null()).then(|| GenericValue::new(data.cast_mut()))
}

/// Fires a `serialize*` completion with the bytes, valid for the call.
///
/// # Safety
///
/// `cb` and `opaque` must be a completion and its `opaque` not yet fired.
unsafe fn fire(cb: BytesCompletion, opaque: *mut c_void, result: Result<Option<&[u8]>, Error>) {
    let (bytes, error) = match result {
        Ok(bytes) => (kafka_Bytes_t::from_option(bytes), ptr::null_mut()),
        Err(error) => (kafka_Bytes_t::NULL, box_error(error)),
    };
    // SAFETY: Per this function's `# Safety`, `cb` and `opaque` are a completion not yet fired;
    // `bytes` borrows memory alive for this call and `error` is null or a fresh `box_error` handle
    // whose ownership passes to `cb`.
    unsafe { cb(bytes, error, opaque) }
}

/// Completion of the `serialize*` `_cb` twins.
type BytesCompletion =
    unsafe extern "C" fn(value: kafka_Bytes_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// The `_cb` form of a serialize method: an asynchronous implementation is
/// called at once and `cb` fires on the thread that reports; anything else
/// runs on the calling thread and `cb` fires there.
///
/// # Safety
///
/// `self_` must be a valid serializer handle, `data` null or the
/// representation the implementation reads, valid until `cb` fires, and
/// `cb` a valid completion.
unsafe fn invoke_cb(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: &str,
    headers: Option<&RecordHeaders>,
    data: *const c_void,
    cb: BytesCompletion,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle, as
    // `serializer_ref` requires; the reference is used only within this call.
    let serializer = unsafe { serializer_ref(self_) }.get();
    if let FfiSerializer::C(CSerializer {
        self_: c_self,
        mode: Mode::Async { serialize, serialize_with_headers, results },
    }) = serializer
    {
        let topic = owned_c_string(topic);
        let headers = headers
            .filter(|_| serialize_with_headers.is_some())
            .map(|headers| Box::new(RecordHeadersInner::new(headers.clone())));
        let (topic_ptr, headers_ptr) = (topic.as_ptr(), headers.as_deref().map(RecordHeadersInner::as_ptr));
        let opaque = SendPtr(opaque);
        let sink: ResultSink = Box::new(move |result| {
            // SAFETY: `result` is what the implementation passed to `__set_callback_result`, which
            // runs this sink before it returns: null or a `kafka_Bytes_t *` valid for that call.
            let result = result.map(|bytes| unsafe { reported_bytes(bytes) });
            // SAFETY: `cb` and `opaque` are the pair the C caller passed to the `_cb` twin, a valid
            // completion until it fires; this `FnOnce` fires it once.
            unsafe { fire(cb, opaque.get(), result) };
            // The arguments the implementation read, released once it reported.
            drop((topic, headers));
        });
        let id = results.register(sink);
        // SAFETY: The function and `c_self` are the pair the C caller registered together through
        // `kafka_common_serialization_Serializer_new_async`, whose rustdoc has the caller keep
        // `self_` alive until every call made through the handle has reported. `topic_ptr` and
        // `headers_ptr` point into heap buffers the sink owns until the report for `id`; `data` is
        // valid until then per this function's `# Safety`.
        unsafe {
            match (headers_ptr, *serialize_with_headers) {
                (Some(headers), Some(function)) => function(*c_self, topic_ptr, headers, data, id),
                _ => serialize(*c_self, topic_ptr, data, id),
            }
        }
        return;
    }
    let output = serializer.invoke(topic, headers, generic(data).as_ref());
    // SAFETY: Per this function's `# Safety`, `cb` is a valid completion, not yet fired; the bytes
    // it receives are `output`'s, alive for the call.
    unsafe {
        match &output {
            Ok(bytes) => fire(cb, opaque, Ok(bytes.as_deref())),
            Err(_) => fire(cb, opaque, output.map(|_| None)),
        }
    }
}

/// `byte[] serialize(String topic, T data)`: `data` is the `void *` the
/// implementation reads, `NULL` for Java's `null`. Returns the bytes
/// (`data == NULL` for a `null` array), borrowed from the handle until its
/// next `serialize*` call or its destruction; on failure stores the owned
/// error in `*out_err` and returns a `NULL` array. Blocks until an
/// asynchronous implementation reports.
///
/// # Safety
///
/// `self_` must be a valid serializer handle, `topic` a valid
/// NUL-terminated string, `data` null or the representation the
/// implementation reads, alive and unchanged for the call, and
/// `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    data: *const c_void,
    out_err: *mut *mut kafka_common_Error_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string, as
    // `c_str_to_string` requires; it is copied into an owned `String`.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle and `out_err` as
    // `invoke` requires; `data` is null or the representation the implementation reads, alive and
    // unchanged for this call, which returns only once the implementation returned or reported.
    unsafe { invoke(self_, out_err, &topic, None, generic(data).as_ref()) }
}

/// Completion of [`kafka_common_serialization_Serializer_serialize_cb`]: the
/// bytes (`data == NULL` for a `null` array or a failure), valid only during
/// the completion, an owned error (`NULL` on success) and the caller's
/// `opaque`.
pub type kafka_common_serialization_Serializer_serialize_cb_t =
    unsafe extern "C" fn(value: kafka_Bytes_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking [`kafka_common_serialization_Serializer_serialize`]: `cb`
/// receives the bytes or an owned error, with `opaque`. A synchronous
/// implementation or a built-in runs on the calling thread and `cb` fires
/// there before this returns; an asynchronous implementation is called at
/// once and `cb` fires on the thread that reports (a serializer has no
/// callbacks vector to queue on). `topic` is copied; `data` must stay valid
/// until `cb` fires.
///
/// # Safety
///
/// As the blocking form, without `out_err`, with `data` valid until `cb`
/// fires; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: The guard runs this only when the body did not return. The body hands `cb` over only
    // as its last step, to the sink it registers or the completion it fires; nothing after that can
    // panic, as the C method cannot unwind into Rust. So `cb` has not fired for this `opaque`, as
    // `fire` requires; it fires on this thread, as every completion of a handle without a callbacks
    // vector does.
    unsafe { fire(cb, opaque, Err(err)) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize_cb(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    data: *const c_void,
    cb: kafka_common_serialization_Serializer_serialize_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle, `data` valid
    // until `cb` fires and `cb` a valid completion.
    unsafe { invoke_cb(self_, &topic, None, data, cb, opaque) }
}

/// `byte[] serialize(String topic, Headers headers, T data)`; the bytes
/// are delivered as for [`kafka_common_serialization_Serializer_serialize`].
///
/// # Safety
///
/// `self_` must be a valid serializer handle, `topic` a valid
/// NUL-terminated string, `headers` a valid record-headers handle, `data`
/// null or the representation the implementation reads, alive and unchanged
/// for the call, and `out_err` a valid slot pointing at null.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize_with_headers(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: *const c_void,
    out_err: *mut *mut kafka_common_Error_t,
) -> kafka_Bytes_t {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, which the
    // caller keeps alive and does not mutate for this call, as `record_headers_ref` requires.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle and `out_err` as
    // `invoke` requires; `data` is null or the representation the implementation reads, alive and
    // unchanged for this call.
    unsafe { invoke(self_, out_err, &topic, Some(headers), generic(data).as_ref()) }
}

/// Completion of
/// [`kafka_common_serialization_Serializer_serialize_with_headers_cb`], as
/// [`kafka_common_serialization_Serializer_serialize_cb_t`].
pub type kafka_common_serialization_Serializer_serialize_with_headers_cb_t =
    unsafe extern "C" fn(value: kafka_Bytes_t, error: *mut kafka_common_Error_t, opaque: *mut c_void);

/// Non-blocking
/// [`kafka_common_serialization_Serializer_serialize_with_headers`]; `cb`
/// fires as for [`kafka_common_serialization_Serializer_serialize_cb`].
/// `topic` and `headers` are copied; `data` must stay valid until `cb`
/// fires.
///
/// # Safety
///
/// As the blocking form, without `out_err`, with `data` valid until `cb`
/// fires; `cb` must stay valid until it fires.
#[ffi_guard(on_panic = |err| {
    // SAFETY: As for `kafka_common_serialization_Serializer_serialize_cb`: `cb` has not fired for
    // this `opaque`, as `fire` requires.
    unsafe { fire(cb, opaque, Err(err)) }
})]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_serialize_with_headers_cb(
    self_: *const kafka_common_serialization_Serializer_t,
    topic: *const c_char,
    headers: *const kafka_common_header_internals_RecordHeaders_t,
    data: *const c_void,
    cb: kafka_common_serialization_Serializer_serialize_with_headers_cb_t,
    opaque: *mut c_void,
) {
    // SAFETY: Per this function's `# Safety`, `topic` is a valid NUL-terminated string.
    let topic = unsafe { c_str_to_string(topic) };
    // SAFETY: Per this function's `# Safety`, `headers` is a valid record-headers handle, alive and
    // unchanged for this call; the implementation receives a copy.
    let headers = unsafe { record_headers_ref(headers) };
    // SAFETY: Per this function's `# Safety`, `self_` is a valid serializer handle, `data` valid
    // until `cb` fires and `cb` a valid completion.
    unsafe { invoke_cb(self_, &topic, Some(headers), data, cb, opaque) }
}

/// Frees an owned handle; null is a no-op. The view a class handle hands
/// out (`__as_Serializer`) is never passed here, nor a handle a client
/// consumed. Every asynchronous call made through the handle must have
/// reported.
///
/// # Safety
///
/// `self_` must be null or an owned serializer handle neither destroyed nor
/// consumed.
#[ffi_guard]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn kafka_common_serialization_Serializer_destroy(
    self_: *mut kafka_common_serialization_Serializer_t,
) {
    if !self_.is_null() {
        // SAFETY: Per this function's `# Safety`, `self_` is an owned handle neither destroyed nor
        // consumed: the `Arc::into_raw` of `SerializerInner::owned`, whose reference the C caller
        // still holds; it is released exactly once here.
        drop(unsafe { Arc::from_raw(self_.cast_const().cast::<SerializerInner>()) });
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
        kafka_common_header_internals_RecordHeaders_to_string,
    };
    use crate::ffi::common::{
        kafka_common_Error_destroy, kafka_common_Error_message, kafka_common_Error_serialization,
    };

    // A C serializer: `data` is a `const char *`, serialized upper-cased,
    // logging what it was asked.
    #[derive(Default)]
    struct Upper {
        calls: Mutex<Vec<String>>,
        // The buffer behind the `kafka_Bytes_t` it writes, alive for the
        // call only — Rust copies before returning.
        buffer: Mutex<Vec<u8>>,
    }
    fn upper<'a>(self_: *mut c_void) -> &'a Upper {
        // SAFETY: Test code: `self_` is the `&imp` the tests register, which outlives every call
        // that passes it back.
        unsafe { &*(self_ as *const Upper) }
    }
    fn write_upper(self_: *mut c_void, data: *const c_void, out_err: *mut *mut kafka_common_Error_t) -> kafka_Bytes_t {
        if data.is_null() {
            return kafka_Bytes_t::NULL;
        }
        // SAFETY: Test code: `data` is non-null here, and every caller passes a `CString`'s
        // pointer, alive for the call.
        let text = unsafe { CStr::from_ptr(data as *const c_char) }.to_str().unwrap();
        if text == "fail" {
            let message = CString::new("cannot serialize fail").unwrap();
            // SAFETY: Test code: `message` outlives the constructor, which copies it; `out_err` is
            // the valid slot pointing at null the invoker passes, and the error stored there is
            // handed to Rust, which consumes it.
            unsafe { *out_err = kafka_common_Error_serialization(message.as_ptr()) };
            return kafka_Bytes_t::NULL;
        }
        let mut buffer = upper(self_).buffer.lock().unwrap();
        *buffer = text.to_uppercase().into_bytes();
        // The array returned points into `imp`'s buffer, which stays alive until Rust has copied
        // it.
        kafka_Bytes_t::from_slice(&buffer)
    }
    unsafe extern "C" fn c_serialize(
        self_: *mut c_void,
        topic: *const c_char,
        data: *const c_void,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> kafka_Bytes_t {
        // SAFETY: Test code: `topic` is the `CString` the invoker passes, alive for the call.
        let topic = unsafe { CStr::from_ptr(topic) }.to_str().unwrap();
        upper(self_).calls.lock().unwrap().push(format!("serialize {topic}"));
        write_upper(self_, data, out_err)
    }
    unsafe extern "C" fn c_serialize_with_headers(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        out_err: *mut *mut kafka_common_Error_t,
    ) -> kafka_Bytes_t {
        // SAFETY: Test code: `topic` is the `CString` the invoker passes, alive for the call.
        let topic = unsafe { CStr::from_ptr(topic) }.to_str().unwrap();
        // SAFETY: Test code: `headers` is the record-headers handle the invoker passes, alive for
        // the call.
        let headers = unsafe { kafka_common_header_internals_RecordHeaders_to_string(headers) };
        // SAFETY: Test code: `headers` is the owned string `to_string` just returned, not yet
        // freed.
        let text = unsafe { CStr::from_ptr(headers) }.to_str().unwrap().to_string();
        // SAFETY: Test code: `headers` is the owned string `to_string` returned, read above and not
        // used after this call.
        unsafe { crate::ffi::util::kafka_string_destroy(headers) };
        upper(self_)
            .calls
            .lock()
            .unwrap()
            .push(format!("serialize_with_headers {topic} {text}"));
        write_upper(self_, data, out_err)
    }

    unsafe fn bytes_of(out: kafka_Bytes_t) -> Option<Vec<u8>> {
        // SAFETY: Test code: `out` is null or the array the handle's last call returned, read
        // before the next call on that handle.
        unsafe { out.as_slice() }.map(<[u8]>::to_vec)
    }

    #[test]
    fn c_serializer_is_reached_through_the_invokers_and_its_bytes_are_copied() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Serializer_new(
            &imp as *const Upper as *mut c_void,
            c_serialize,
            Some(c_serialize_with_headers),
        );
        let topic = CString::new("t").unwrap();
        let hello = CString::new("hello").unwrap();
        let mut headers = RecordHeaders::new();
        headers.add_with_key_value("k", Some(b"v")).unwrap();
        let headers = box_record_headers(headers);
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_Serializer_new` and is
        // destroyed once, at the end; `imp` outlives it; `headers` comes from `box_record_headers`
        // and is destroyed once, after its last use; `topic` and `hello` outlive the calls reading
        // them; `err` is a valid slot pointing at null, left null by every call (asserted); each
        // returned view is read before the next call on the handle.
        unsafe {
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Serializer_serialize(
                handle,
                topic.as_ptr(),
                hello.as_ptr().cast(),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(bytes_of(out), Some(b"HELLO".to_vec()));
            // The C buffer is overwritten by the next call; the handle's copy
            // is what the first view pointed at, so it is a different buffer.
            assert_ne!(out.data, imp.buffer.lock().unwrap().as_ptr());

            let out = kafka_common_serialization_Serializer_serialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(bytes_of(out), Some(b"HELLO".to_vec()));

            // Null data is Java's null.
            let out =
                kafka_common_serialization_Serializer_serialize(handle, topic.as_ptr(), ptr::null(), &raw mut err);
            assert!(err.is_null());
            assert!(out.data.is_null());

            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Serializer_destroy(handle);
            kafka_common_serialization_Serializer_destroy(ptr::null_mut());
        }
        // The headers reach C as the same `RecordHeaders`, printed by the
        // Rust type's own `to_string`.
        let mut expected_headers = RecordHeaders::new();
        expected_headers.add_with_key_value("k", Some(b"v")).unwrap();
        let with_headers = format!("serialize_with_headers t {expected_headers}");
        assert_eq!(
            *imp.calls.lock().unwrap(),
            ["serialize t", with_headers.as_str(), "serialize t"]
        );
    }

    #[test]
    fn c_error_is_stored_owned_and_null_optional_methods_are_the_java_default() {
        let imp = Upper::default();
        let handle = kafka_common_serialization_Serializer_new(&imp as *const Upper as *mut c_void, c_serialize, None);
        let topic = CString::new("t").unwrap();
        let fail = CString::new("fail").unwrap();
        let headers = box_record_headers(RecordHeaders::new());
        // SAFETY: Test code: `handle` comes from `kafka_common_serialization_Serializer_new` and is
        // destroyed once, at the end; `imp` outlives it; `headers` comes from `box_record_headers`
        // and is destroyed once, after its last use; `topic`, `fail` and `hello` outlive the calls
        // reading them; `err` is a valid slot pointing at null before every call (reset after the
        // error is destroyed); each returned view is read before the next call on the handle; the
        // error is destroyed once, after it is read.
        unsafe {
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Serializer_serialize(
                handle,
                topic.as_ptr(),
                fail.as_ptr().cast(),
                &raw mut err,
            );
            assert!(!err.is_null());
            assert_eq!(
                CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap(),
                "cannot serialize fail"
            );
            assert!(out.data.is_null(), "a failure returns the null array");
            kafka_common_Error_destroy(err);
            err = ptr::null_mut();

            // The header variant reaches `serialize`.
            let hello = CString::new("hi").unwrap();
            let out = kafka_common_serialization_Serializer_serialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(bytes_of(out), Some(b"HI".to_vec()));
            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Serializer_destroy(handle);
        }
        assert_eq!(*imp.calls.lock().unwrap(), ["serialize t", "serialize t"]);
    }

    #[test]
    fn owned_handles_are_consumed_and_views_are_shared() {
        let imp = Arc::new(FfiSerializer::C(CSerializer {
            self_: ptr::null_mut(),
            mode: Mode::Sync { serialize: c_serialize, serialize_with_headers: None },
        }));
        let owned = SerializerInner::owned(Arc::clone(&imp));
        assert_eq!(Arc::strong_count(&imp), 2);
        // SAFETY: Test code: `owned` comes from `SerializerInner::owned` and is consumed once, by
        // this call.
        let taken = unsafe { take_serializer(owned) };
        assert!(ptr::addr_eq(Arc::as_ptr(&taken), owned), "the handle stays where it was");
        assert_eq!(Arc::strong_count(&taken), 1, "the caller's reference was taken over");
        assert_eq!(Arc::strong_count(&imp), 2);
        drop(taken);
        assert_eq!(Arc::strong_count(&imp), 1, "the owned handle was freed with its last holder");

        let view = SerializerInner::view(Arc::clone(&imp));
        // SAFETY: Test code: `view` is a live `SerializerInner` view, not owned, so
        // `take_serializer` only adds a reference to it; `view` is dropped after.
        let shared = unsafe { take_serializer(view.as_ptr().cast_mut()) };
        assert!(ptr::addr_eq(Arc::as_ptr(&shared), Arc::as_ptr(&view)));
        assert_eq!(Arc::strong_count(&view), 2, "the view keeps its own reference");
        drop(view);
        drop(shared);
        assert_eq!(Arc::strong_count(&imp), 1);
    }

    /// A call an asynchronous C serializer of these tests was handed, kept as
    /// addresses so it can cross to the reporting thread.
    struct AsyncCall {
        id: i64,
        topic: usize,
        data: usize,
    }

    /// An asynchronous C serializer: `data` is a `const char *`, reported
    /// upper-cased, during the call (`inline`) or when the test reports.
    struct AsyncUpper {
        handle: std::sync::atomic::AtomicPtr<kafka_common_serialization_Serializer_t>,
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

        fn register(&self) -> *mut kafka_common_serialization_Serializer_t {
            let handle = kafka_common_serialization_Serializer_new_async(
                self as *const Self as *mut c_void,
                async_serialize,
                Some(async_serialize_with_headers),
            );
            self.handle.store(handle, std::sync::atomic::Ordering::SeqCst);
            handle
        }

        fn handle(&self) -> *const kafka_common_serialization_Serializer_t {
            self.handle.load(std::sync::atomic::Ordering::SeqCst)
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
    }

    fn async_upper<'a>(self_: *mut c_void) -> &'a AsyncUpper {
        // SAFETY: Test code: `self_` is the `&AsyncUpper` the tests register, which outlives every
        // call that passes it back.
        unsafe { &*(self_ as *const AsyncUpper) }
    }

    /// Reads what `call` was handed and reports the upper-cased bytes from
    /// a buffer freed right after the report, an error for "fail", or a
    /// `NULL` result for a null `data`.
    ///
    /// # Safety
    ///
    /// `call` must not have been reported yet, and its `data` must still be
    /// alive (the caller's).
    unsafe fn report(imp: &AsyncUpper, call: &AsyncCall) {
        // SAFETY: Per this function's `# Safety`, the call is unreported, so its topic is still kept
        // by the handle, and its data is alive.
        let (topic, text) = unsafe {
            (
                CStr::from_ptr(call.topic as *const c_char).to_str().unwrap().to_string(),
                (call.data != 0).then(|| CStr::from_ptr(call.data as *const c_char).to_str().unwrap().to_string()),
            )
        };
        imp.calls
            .lock()
            .unwrap()
            .push(format!("{topic} {}", text.as_deref().unwrap_or("null")));
        // SAFETY: Test code: the handle is alive until the test destroys it, after every report;
        // the error is a fresh owned one; the reported array points at a buffer alive for the
        // report, which copies it.
        unsafe {
            match text.as_deref() {
                Some("fail") => {
                    let message = CString::new("cannot serialize fail").unwrap();
                    let error = kafka_common_Error_serialization(message.as_ptr());
                    kafka_common_serialization_Serializer__set_callback_result(
                        imp.handle(),
                        call.id,
                        ptr::null_mut(),
                        error,
                    );
                },
                Some(text) => {
                    let buffer = text.to_uppercase().into_bytes();
                    let mut bytes = kafka_Bytes_t::from_slice(&buffer);
                    kafka_common_serialization_Serializer__set_callback_result(
                        imp.handle(),
                        call.id,
                        (&raw mut bytes).cast(),
                        ptr::null_mut(),
                    );
                    // Overwritten and freed: the report copied the bytes.
                    drop(buffer);
                },
                None => kafka_common_serialization_Serializer__set_callback_result(
                    imp.handle(),
                    call.id,
                    ptr::null_mut(),
                    ptr::null_mut(),
                ),
            }
        }
    }

    fn take_call(self_: *mut c_void, call: AsyncCall) {
        let imp = async_upper(self_);
        if imp.inline {
            // SAFETY: Test code: the call is being made, so it is unreported, and its data alive.
            unsafe { report(imp, &call) };
        } else {
            imp.pending.lock().unwrap().push(call);
        }
    }

    unsafe extern "C" fn async_serialize(self_: *mut c_void, topic: *const c_char, data: *const c_void, id: i64) {
        take_call(self_, AsyncCall { id, topic: topic as usize, data: data as usize });
    }

    unsafe extern "C" fn async_serialize_with_headers(
        self_: *mut c_void,
        topic: *const c_char,
        headers: *const kafka_common_header_internals_RecordHeaders_t,
        data: *const c_void,
        id: i64,
    ) {
        // SAFETY: Test code: `headers` is the copy the handle keeps until the report.
        let has_k = unsafe { record_headers_ref(headers) }.last_header("k").is_some();
        async_upper(self_).calls.lock().unwrap().push(format!("with_headers k={has_k}"));
        take_call(self_, AsyncCall { id, topic: topic as usize, data: data as usize });
    }

    /// Polls `future` once, with a waker that does nothing.
    fn poll_once<F: Future + ?Sized>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    /// What a `_cb` completion of these tests received, and on which thread.
    type Completions = Mutex<Vec<(Result<Option<Vec<u8>>, String>, std::thread::ThreadId)>>;

    unsafe extern "C" fn record_completion(
        value: kafka_Bytes_t,
        error: *mut kafka_common_Error_t,
        opaque: *mut c_void,
    ) {
        // SAFETY: Test code: `opaque` is the `&Completions` the test passed, alive until the
        // completion fired; `value` is valid during the completion; `error` is null or an owned
        // error, read and freed once.
        let outcome = unsafe {
            if error.is_null() {
                Ok(bytes_of(value))
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

    /// Runs the blocking `serialize` invoker on `data` (`None`: Java's null).
    ///
    /// # Safety
    ///
    /// `handle` must be a valid serializer handle.
    unsafe fn blocking_serialize(
        handle: *const kafka_common_serialization_Serializer_t,
        data: Option<&CStr>,
    ) -> Result<Option<Vec<u8>>, String> {
        let topic = CString::new("t").unwrap();
        let mut err = ptr::null_mut();
        // SAFETY: Per this function's `# Safety`, `handle` is valid; `topic` and `data` outlive the
        // call; `err` is a valid slot pointing at null; the error returned is owned and freed once,
        // and the bytes are read before the next call on the handle.
        unsafe {
            let out = kafka_common_serialization_Serializer_serialize(
                handle,
                topic.as_ptr(),
                data.map_or(ptr::null(), |data| data.as_ptr().cast()),
                &raw mut err,
            );
            if err.is_null() {
                Ok(bytes_of(out))
            } else {
                let message = CStr::from_ptr(kafka_common_Error_message(err)).to_str().unwrap().to_string();
                kafka_common_Error_destroy(err);
                Err(message)
            }
        }
    }

    #[test]
    fn async_c_serializer_reporting_inside_the_call_is_reached_through_the_invokers() {
        let imp = AsyncUpper::new(true);
        let handle = imp.register();
        let topic = CString::new("t").unwrap();
        let (hello, cb) = (CString::new("hello").unwrap(), CString::new("cb").unwrap());
        let mut record_headers = RecordHeaders::new();
        record_headers.add_with_key_value("k", Some(b"v")).unwrap();
        let headers = box_record_headers(record_headers);
        let completions = Completions::default();
        // SAFETY: Test code: `handle` comes from `_new_async` and is destroyed once, at the end,
        // after every report; `imp` outlives it; `headers` is destroyed once, after its last use;
        // the strings outlive the calls (the `_cb` completion fires before `_cb` returns); `err`
        // is a valid slot pointing at null, and each returned view is read before the next call.
        unsafe {
            assert_eq!(blocking_serialize(handle, Some(&hello)), Ok(Some(b"HELLO".to_vec())));
            assert_eq!(
                blocking_serialize(handle, Some(c"fail")),
                Err("cannot serialize fail".to_string())
            );
            assert_eq!(blocking_serialize(handle, None), Ok(None), "a NULL result is Java's null");
            let mut err = ptr::null_mut();
            let out = kafka_common_serialization_Serializer_serialize_with_headers(
                handle,
                topic.as_ptr(),
                headers,
                hello.as_ptr().cast(),
                &raw mut err,
            );
            assert!(err.is_null());
            assert_eq!(bytes_of(out), Some(b"HELLO".to_vec()));

            kafka_common_serialization_Serializer_serialize_cb(
                handle,
                topic.as_ptr(),
                cb.as_ptr().cast(),
                record_completion,
                &completions as *const Completions as *mut c_void,
            );
            assert_eq!(
                *completions.lock().unwrap(),
                [(Ok(Some(b"CB".to_vec())), std::thread::current().id())],
                "an inline report fires the completion before `_cb` returns"
            );
            kafka_common_header_internals_RecordHeaders_destroy(headers);
            kafka_common_serialization_Serializer_destroy(handle);
        }
        assert_eq!(
            *imp.calls.lock().unwrap(),
            ["t hello", "t fail", "t null", "with_headers k=true", "t hello", "t cb"]
        );
    }

    #[test]
    fn async_c_serializer_reporting_later_from_another_thread() {
        let imp = AsyncUpper::new(false);
        let handle = SendPtr(imp.register().cast());
        let reporter = |count: usize| {
            let imp = &imp;
            move || {
                for _ in 0..count {
                    let call = imp.next_call();
                    // SAFETY: Test code: the call was just taken, unreported; the test keeps its
                    // data alive until it reports.
                    unsafe { report(imp, &call) };
                }
                std::thread::current().id()
            }
        };
        let late = CString::new("late").unwrap();
        std::thread::scope(|scope| {
            // The blocking invoker parks the calling thread until the report.
            let thread = scope.spawn(reporter(1));
            // SAFETY: Test code: the handle is valid until destroyed below.
            let out = unsafe { blocking_serialize(handle.get().cast(), Some(&late)) };
            assert_eq!(out, Ok(Some(b"LATE".to_vec())));
            thread.join().unwrap();

            // The `_cb` twin returns at once; the completion fires on the reporting thread.
            let completions = Completions::default();
            let topic = CString::new("t").unwrap();
            // SAFETY: Test code: the handle is valid; `topic` is copied by `_cb`; the data and
            // `completions` outlive the completion, which fires before the join below.
            unsafe {
                kafka_common_serialization_Serializer_serialize_cb(
                    handle.get().cast(),
                    topic.as_ptr(),
                    late.as_ptr().cast(),
                    record_completion,
                    &completions as *const Completions as *mut c_void,
                );
            }
            drop(topic);
            assert!(completions.lock().unwrap().is_empty(), "nothing reported yet");
            let reporting = scope.spawn(reporter(1)).join().unwrap();
            assert_eq!(*completions.lock().unwrap(), [(Ok(Some(b"LATE".to_vec())), reporting)]);

            // The trait's future, as a producer awaits it.
            let thread = scope.spawn(reporter(1));
            let runtime = tokio::runtime::Builder::new_current_thread().build().unwrap();
            let value = runtime.block_on(async {
                // SAFETY: Test code: the handle is valid until destroyed below.
                let serializer = unsafe { serializer_ref(handle.get().cast()) }.get();
                let mut headers = RecordHeaders::new();
                headers.add_with_key_value("k", Some(b"v")).unwrap();
                let data = GenericValue::new(late.as_ptr().cast_mut().cast());
                Serializer::serialize_with_headers(serializer, "t", &headers, Some(&data))
                    .await
                    .map(|bytes| bytes.map(Cow::into_owned))
            });
            thread.join().unwrap();
            assert_eq!(value.unwrap(), Some(b"LATE".to_vec()));
        });
        // SAFETY: Test code: every call reported; the handle is destroyed once.
        unsafe { kafka_common_serialization_Serializer_destroy(handle.get().cast()) };
        assert_eq!(
            *imp.calls.lock().unwrap(),
            ["t late", "t late", "with_headers k=true", "t late"]
        );
    }

    #[test]
    fn a_second_report_is_ignored_and_its_error_freed() {
        let imp = AsyncUpper::new(false);
        let handle = imp.register();
        let completions = Completions::default();
        let (topic, once) = (CString::new("t").unwrap(), CString::new("once").unwrap());
        // SAFETY: Test code: the handle is valid until destroyed at the end; the strings and
        // `completions` outlive every completion; the extra reports pass a null value or a fresh
        // owned error.
        unsafe {
            kafka_common_serialization_Serializer_serialize_cb(
                handle,
                topic.as_ptr(),
                once.as_ptr().cast(),
                record_completion,
                &completions as *const Completions as *mut c_void,
            );
            let call = imp.next_call();
            report(&imp, &call);
            let message = CString::new("too late").unwrap();
            kafka_common_serialization_Serializer__set_callback_result(
                handle,
                call.id,
                ptr::null_mut(),
                kafka_common_Error_serialization(message.as_ptr()),
            );
            kafka_common_serialization_Serializer__set_callback_result(
                handle,
                call.id,
                ptr::null_mut(),
                ptr::null_mut(),
            );
            kafka_common_serialization_Serializer__set_callback_result(handle, 12345, ptr::null_mut(), ptr::null_mut());
        }
        assert_eq!(completions.lock().unwrap().len(), 1, "the completion fires once");
        assert_eq!(completions.lock().unwrap()[0].0, Ok(Some(b"ONCE".to_vec())));

        // The same through the trait's future: the first report wins.
        let first = CString::new("first").unwrap();
        let data = GenericValue::new(first.as_ptr().cast_mut().cast());
        // SAFETY: Test code: the handle is valid until destroyed below.
        let serializer = unsafe { serializer_ref(handle) }.get();
        let mut future = std::pin::pin!(Serializer::serialize(serializer, "t", Some(&data)));
        assert!(poll_once(future.as_mut()).is_pending());
        let call = imp.next_call();
        // SAFETY: Test code: the call is unreported and its data alive; the second report passes a
        // fresh owned error.
        unsafe {
            report(&imp, &call);
            let message = CString::new("second").unwrap();
            kafka_common_serialization_Serializer__set_callback_result(
                handle,
                call.id,
                ptr::null_mut(),
                kafka_common_Error_serialization(message.as_ptr()),
            );
        }
        let std::task::Poll::Ready(value) = poll_once(future.as_mut()) else {
            panic!("reported");
        };
        assert_eq!(value.unwrap().as_deref(), Some(&b"FIRST"[..]));
        // SAFETY: Test code: every call reported; the handle is destroyed once.
        unsafe { kafka_common_serialization_Serializer_destroy(handle) };
    }

    #[test]
    fn is_async_tells_the_modes_apart() {
        let asynchronous = AsyncUpper::new(true);
        let async_handle = asynchronous.register();
        let sync_handle = kafka_common_serialization_Serializer_new(ptr::null_mut(), c_serialize, None);
        let class = crate::ffi::common::serialization::SerializerHandle::boxed(FfiSerializer::String(
            GenericStringSerializer::default(),
        ));
        // SAFETY: Test code: each handle is valid and destroyed once, at the end; the view is the
        // class handle's, destroyed with it.
        unsafe {
            let view = (*class).as_serializer();
            assert_eq!(kafka_common_serialization_Serializer__is_async(async_handle), 1);
            assert_eq!(kafka_common_serialization_Serializer__is_async(sync_handle), 0);
            assert_eq!(kafka_common_serialization_Serializer__is_async(view), 0, "a built-in's view");
            // A report on a handle that is not asynchronous is ignored, its error freed.
            let message = CString::new("ignored").unwrap();
            kafka_common_serialization_Serializer__set_callback_result(
                sync_handle,
                1,
                ptr::null_mut(),
                kafka_common_Error_serialization(message.as_ptr()),
            );
            kafka_common_serialization_Serializer_destroy(async_handle);
            kafka_common_serialization_Serializer_destroy(sync_handle);
            crate::ffi::common::serialization::SerializerHandle::destroy(class);
        }
    }

    /// A synchronous C serializer recording the thread it runs on, and, when
    /// `gate` is set, waiting for it before reading its topic.
    #[derive(Default)]
    struct ThreadRecorder {
        threads: Mutex<Vec<std::thread::ThreadId>>,
        gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
        read: Mutex<Option<std::sync::mpsc::Sender<String>>>,
    }

    unsafe extern "C" fn record_thread(
        self_: *mut c_void,
        topic: *const c_char,
        _data: *const c_void,
        _out_err: *mut *mut kafka_common_Error_t,
    ) -> kafka_Bytes_t {
        // SAFETY: Test code: `self_` is the `&ThreadRecorder` the tests register, which outlives
        // every call.
        let recorder = unsafe { &*(self_ as *const ThreadRecorder) };
        recorder.threads.lock().unwrap().push(std::thread::current().id());
        let gate = recorder.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.recv().unwrap();
            // SAFETY: Test code: the topic is borrowed for the call, which has not returned.
            let read = unsafe { CStr::from_ptr(topic) }.to_str().unwrap().to_string();
            recorder.read.lock().unwrap().take().unwrap().send(read).unwrap();
        }
        kafka_Bytes_t::NULL
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_c_serializer_runs_on_the_blocking_pool() {
        let recorder = ThreadRecorder::default();
        let handle = kafka_common_serialization_Serializer_new(
            &recorder as *const ThreadRecorder as *mut c_void,
            record_thread,
            None,
        );
        // SAFETY: Test code: the handle is valid until destroyed at the end.
        let serializer = unsafe { serializer_ref(handle) }.get();
        assert_eq!(Serializer::serialize(serializer, "t", None).await.unwrap(), None);
        // SAFETY: Test code: the handle is valid.
        assert_eq!(unsafe { blocking_serialize(handle, None) }, Ok(None));
        // SAFETY: Test code: every call returned; the handle is destroyed once.
        unsafe { kafka_common_serialization_Serializer_destroy(handle) };
        let here = std::thread::current().id();
        let threads = recorder.threads.lock().unwrap().clone();
        assert_ne!(threads[0], here, "a producer's call runs on the blocking pool");
        assert_eq!(threads[1], here, "the blocking invoker runs on its calling thread");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn a_dropped_sync_call_keeps_its_arguments_until_it_returns() {
        let recorder = ThreadRecorder::default();
        let (release, gate) = std::sync::mpsc::channel();
        let (read_tx, read_rx) = std::sync::mpsc::channel();
        *recorder.gate.lock().unwrap() = Some(gate);
        *recorder.read.lock().unwrap() = Some(read_tx);
        let handle = kafka_common_serialization_Serializer_new(
            &recorder as *const ThreadRecorder as *mut c_void,
            record_thread,
            None,
        );
        // SAFETY: Test code: the handle is valid until destroyed at the end.
        let serializer = unsafe { serializer_ref(handle) }.get();
        let topic = String::from("topic");
        let mut future = Box::pin(Serializer::serialize(serializer, &topic, None));
        assert!(poll_once(future.as_mut()).is_pending(), "the C function waits for its gate");
        drop(future);
        drop(topic);
        release.send(()).unwrap();
        let read = tokio::task::spawn_blocking(move || read_rx.recv_timeout(std::time::Duration::from_secs(10)))
            .await
            .unwrap()
            .unwrap();
        // SAFETY: Test code: the call returned (it sent what it read last); the handle is destroyed
        // once.
        unsafe { kafka_common_serialization_Serializer_destroy(handle) };
        assert_eq!(read, "topic");
    }

    #[test]
    fn a_dropped_async_call_keeps_its_arguments_until_it_reports() {
        let imp = AsyncUpper::new(false);
        let handle = SendPtr(imp.register().cast());
        // SAFETY: Test code: the handle is valid until destroyed at the end.
        let serializer = unsafe { serializer_ref(handle.get().cast()) }.get();
        // The caller's data stays valid until the report, as the `_async_fn_t` rustdoc requires;
        // the topic and headers are the handle's to keep.
        let value = CString::new("kept").unwrap();
        let data = GenericValue::new(value.as_ptr().cast_mut().cast());
        {
            let (topic, mut headers) = (String::from("topic"), RecordHeaders::new());
            headers.add_with_key_value("k", Some(b"v")).unwrap();
            let mut future =
                std::pin::pin!(Serializer::serialize_with_headers(serializer, &topic, &headers, Some(&data)));
            assert!(poll_once(future.as_mut()).is_pending());
        }
        std::thread::scope(|scope| {
            scope.spawn(|| {
                let call = imp.next_call();
                // SAFETY: Test code: the call is unreported: its future was dropped, and the slot
                // keeps its topic and headers until this report; its data is `value`, alive.
                unsafe { report(&imp, &call) };
            });
        });
        assert_eq!(*imp.calls.lock().unwrap(), ["with_headers k=true", "topic kept"]);
        // SAFETY: Test code: every call reported; the handle is destroyed once.
        unsafe { kafka_common_serialization_Serializer_destroy(handle.get().cast()) };
    }
}

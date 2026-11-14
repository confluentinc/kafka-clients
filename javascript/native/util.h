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

// Shared declarations of the Node-API addon. This is the only file that
// includes the bindings header (root CLAUDE.md §4, "C FFI Conventions"; the
// static layout test checks it): every other source includes this one. Every
// function without an `out_err` slot of its own is called through its `_err`
// twin.

#ifndef CONFLUENT_KAFKA_JS_UTIL_H
#define CONFLUENT_KAFKA_JS_UTIL_H

#include <node_api.h>
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>

#include "confluent_kafka_bindings.h"

// Evaluates a Node-API call; on failure throws a JS error describing it (unless
// one is already pending) and returns `ret` from the enclosing function.
#define NAPI_CHECK(env, call, ret)                                       \
    do {                                                                 \
        if ((call) != napi_ok) {                                         \
            js_throw_last_napi_error((env), #call);                      \
            return ret;                                                  \
        }                                                                \
    } while (0)

// ---- out_err ----------------------------------------------------------------

// The error a call stored in `*slot` (NULL if none), resetting the slot to
// NULL: a `kafka_common_Error_t **out_err` must point at NULL on entry, so one
// slot serves every call of a function.
static inline kafka_common_Error_t* ffi_take(kafka_common_Error_t** slot) {
    kafka_common_Error_t* e = *slot;
    *slot = NULL;
    return e;
}

// The out_err macros. Each expects a slot `kafka_common_Error_t* ffi_err = NULL`
// in scope, which `call` passes as its `out_err` (`&ffi_err`):
//
//   err = FFI_ERR(f(args, &ffi_err));     the OWNED error of a call, or NULL
//   err = FFI_SET(v, f(args, &ffi_err));  stores the call's result in `v` too
//   FFI_DISCARD(f(args, &ffi_err));       destroys the error, if any (a call
//                                         whose failure changes nothing here)
#define FFI_ERR(call) ((void)(call), ffi_take(&ffi_err))
#define FFI_SET(var, call) ((var) = (call), ffi_take(&ffi_err))
#define FFI_DISCARD(call) error_discard(FFI_ERR(call))

// ---- addon.c ----------------------------------------------------------------

// Per-addon-instance state (napi_set_instance_data).
typedef struct {
    // src/internal/native.ts's error factory, turning the plain error object
    // error_to_js builds into a KafkaError; NULL until registered.
    napi_ref error_factory;
    // src/internal/native.ts's factory for a RecordMetadata.
    napi_ref metadata_factory;
    // src/internal/native.ts's warning function: an error with nowhere else
    // to go goes to process.emitWarning (root CLAUDE.md, JavaScript Binding
    // Conventions, Errors).
    napi_ref warn;
} AddonData;

AddonData* addon_data(napi_env env);

// Throws a JS Error naming the failed Node-API call, unless an exception is
// already pending.
void js_throw_last_napi_error(napi_env env, const char* what);

// Calls the function `ref` holds with `argc` arguments, on the JS thread.
// Returns its result, or NULL when it threw: the exception is cleared and its
// message stored in `*thrown` (malloc'ed) when `thrown` is not NULL, or else
// handed to process.emitWarning (js_warn_thrown).
napi_value call_ref(napi_env env, napi_ref ref, size_t argc, napi_value* argv, char** thrown);

// Hands the pending JS exception, cleared, to process.emitWarning (the warn
// function src/internal/native.ts registered): an exception with nowhere
// else to go. A no-op when none is pending.
void js_warn_thrown(napi_env env);

// Deletes `*ref` (a no-op for NULL) and resets it to NULL.
void delete_ref(napi_env env, napi_ref* ref);

// ---- error.c ----------------------------------------------------------------

// A KafkaError for a BORROWED error (never destroyed here), or `null` for
// NULL. Returns NULL only when Node-API itself failed.
napi_value error_to_js(napi_env env, const kafka_common_Error_t* e);

// A KafkaError for an OWNED error, which is destroyed once converted;
// `null` for NULL.
napi_value error_take_to_js(napi_env env, kafka_common_Error_t* e);

// Throws a KafkaError for an OWNED error, which is destroyed.
void error_throw_owned(napi_env env, kafka_common_Error_t* e);

// Destroys an OWNED error, swallowing a panic reported by the destroy itself.
// Only where no JS can run (an environment teardown) or while converting an
// error: everywhere else an error with nowhere to go is error_warn'ed.
void error_discard(kafka_common_Error_t* e);

// An OWNED error with nowhere else to go: when `js` (JS may run here: not an
// environment teardown), it is converted and handed to process.emitWarning
// (a pending JS exception is kept and thrown again after), then destroyed;
// otherwise only destroyed. A no-op for NULL.
void error_warn(napi_env env, int js, kafka_common_Error_t* e);

// ---- callback_queue.c -------------------------------------------------------
//
// The callback queue of one client: the C FFI queues every callback of a
// client (delivery callbacks, `_cb` completions) on the client's callback
// vector, and fires the notify hook installed with `__set_callbacks_notify`
// when that vector goes from empty to non-empty, from any thread. The hook
// (cbq_notify) only calls a threadsafe function; its call_js then runs the
// drain on the JS thread, which calls the client's `__execute_callbacks`
// while it returns > 0. Drains never nest. The queue also keeps the process
// alive while operations are pending, keeps the client's handle object from
// garbage collection until the client is destroyed, and owns the threadsafe
// function's teardown (javascript/CLAUDE.md, rules 2, 3
// and 10). Each client embeds one and passes its own `__execute_callbacks`.

// The client's `__execute_callbacks_err` over `client`: how many callbacks ran.
typedef int32_t (*cbq_execute_fn)(void* client, kafka_common_Error_t** out_err);
// A client hook run on the JS thread.
typedef void (*cbq_client_fn)(napi_env env, void* client);

typedef struct {
    napi_env env;                       // the environment that created it (JS thread)
    void* client;                       // what the hooks receive
    cbq_execute_fn execute;             // the client's __execute_callbacks
    cbq_client_fn after_drain;          // run after every drain loop, outside it; may be NULL
    cbq_client_fn on_tsfn_finalize;     // the tsfn's finalizer: drops the tsfn's ownership of client
    napi_threadsafe_function tsfn;
    int tsfn_alive;                     // not released or finalized yet
    pthread_mutex_t notify_mtx;         // guards notify_open and the hook's tsfn call
    int notify_open;                    // the hook may call the tsfn
    napi_ref self_ref;                  // the client's handle object; strong until the client is destroyed
    int self_strong;                    // self_ref is strong
    int64_t pending;                    // operations in flight (sends, flushes, a close)
    int busy;                           // the tsfn is currently ref'd
    int draining;                       // a drain loop is running
    int stopped;                        // the client's destroy began: no drain from now on
    int finalizing;                     // the handle object is being finalized: run no JS
} CallbackQueue;

// Creates the threadsafe function (unref'd: idle) named `name`. Returns 0, or
// -1 with a JS exception pending and nothing left to free. From then on the
// tsfn is one owner of `client`: its finalizer calls `on_tsfn_finalize`.
int cbq_init(napi_env env, CallbackQueue* q, const char* name, void* client, cbq_execute_fn execute,
             cbq_client_fn after_drain, cbq_client_fn on_tsfn_finalize);

// Takes a strong reference on the client's handle object, released by
// cbq_release once the client is destroyed, so garbage collection never
// finalizes a client still running. Returns 0, or -1 with a JS exception
// pending.
int cbq_attach(napi_env env, CallbackQueue* q, napi_value handle);

// The notify hook (`kafka_producer_Producer__set_callbacks_notify`'s, opaque
// the CallbackQueue): from any thread, it only signals the threadsafe
// function, under `notify_mtx` and while `notify_open`.
void cbq_notify(void* opaque);

// Asks for a drain from the event loop (JS thread): for a completion that
// fired outside one.
void cbq_schedule(CallbackQueue* q);

// Adds `delta` operations in flight and refs or unrefs the tsfn accordingly.
// JS thread.
void cbq_pending_add(napi_env env, CallbackQueue* q, int64_t delta);

// The drain: the client's __execute_callbacks while it returns > 0, then
// `after_drain`. A no-op inside a drain, once stopped, or while finalizing.
void cbq_drain(napi_env env, CallbackQueue* q);

// Ends the queue once the client is destroyed (no callback can fire any
// more): closes the hook's way to the tsfn and releases the tsfn, whose
// finalizer then drops its ownership of the client, and lets the handle
// object be collected.
void cbq_release(napi_env env, CallbackQueue* q);

// The handle object is being finalized: drops the reference on it.
void cbq_detach(napi_env env, CallbackQueue* q);

// Frees what cbq_init allocated besides the tsfn; called when the client is
// freed.
void cbq_dispose(CallbackQueue* q);

// ---- record.c ---------------------------------------------------------------

// A borrowed byte view of a JS Uint8Array (a Buffer included) or `null`.
// `bytes.data` is NULL for `null`; an empty array points at a static byte, as
// `data == NULL` would read as Java's null.
typedef struct {
    kafka_Bytes_t bytes;
    int is_null;
} JsBytes;

// Reads `value` (a Uint8Array or null/undefined) into `out`; throws a
// TypeError naming `what` and returns -1 otherwise.
int js_bytes(napi_env env, napi_value value, const char* what, JsBytes* out);

// Reads a JS string into a malloc'ed NUL-terminated UTF-8 copy, or throws a
// TypeError and returns NULL: `what` for a value that is not a string, `what`
// followed by " without null bytes" for a string holding a NUL.
char* js_string_dup(napi_env env, napi_value value, const char* what);

// Reads bytes `value` (a Uint8Array: encodeLikeJava's bytes of a string, from
// src/internal/encode.ts) into a malloc'ed NUL-terminated copy, or throws a
// TypeError and returns NULL: `what` for a value that is not a Uint8Array,
// `what` followed by " without null bytes" for bytes holding a NUL.
char* js_bytes_cstring(napi_env env, napi_value value, const char* what);

// Builds the C record. `headers` is `null` or an array of `[key, value]`
// pairs, key the bytes of its string (encodeLikeJava's) and value a
// Uint8Array or null. Key and value point at
// `key` / `value` (or are Java's null), which must stay valid until the
// hand-over; the header bytes are copied by the core during this call. Returns NULL with a JS exception
// pending on failure.
kafka_producer_ProducerRecord_t* record_build(napi_env env, const char* topic, int32_t partition,
                                              int64_t timestamp, const JsBytes* key,
                                              const JsBytes* value, napi_value headers);

// ---- the native functions addon.c registers -----------------------------------

// error.c: setFactories(errorFactory, metadataFactory).
napi_value js_set_factories(napi_env env, napi_callback_info info);

// producer.c: the KafkaProducer handle.
napi_value js_producer_create(napi_env env, napi_callback_info info);
napi_value js_producer_send(napi_env env, napi_callback_info info);
napi_value js_producer_flush(napi_env env, napi_callback_info info);
napi_value js_producer_close(napi_env env, napi_callback_info info);
napi_value js_producer_pump(napi_env env, napi_callback_info info);

#endif  // CONFLUENT_KAFKA_JS_UTIL_H

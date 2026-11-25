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
#include <stdatomic.h>
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
void error_stat_owned(const kafka_common_Error_t* e);

static inline kafka_common_Error_t* ffi_take(kafka_common_Error_t** slot) {
    kafka_common_Error_t* e = *slot;
    *slot = NULL;
    error_stat_owned(e);
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
    // to go goes to process.emitWarning (javascript/CLAUDE.md, JavaScript Binding
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

// The steps the test-only faultNext() makes fail: a producer's notify
// registration, the conversion of a completion's answer to JS (a record's
// metadata, a core error, a send's slot number, a partitionsFor list, a
// close's answer), a delivery callback's wait for its turn (send_enqueue),
// and the failure js_take_failure makes of one.
typedef enum {
    FAULT_NONE,
    FAULT_NOTIFY,
    FAULT_METADATA,
    FAULT_ERROR,
    FAULT_SLOT,
    FAULT_PARTITIONS,
    FAULT_CLOSE,
    FAULT_ENQUEUE,
    FAULT_FAILURE,
    FAULT_STEPS
} FaultStep;

// The failures armed, every step's together: 0 when none is.
extern atomic_int fault_armed;

// Takes one of the failures armed for `step`: 1 when it had one, else 0.
int fault_take(FaultStep step);

// Whether the site of `step` fails this time, as its Node-API call failing
// would: once per failure armed. Unarmed, a site costs one load and branch.
static inline int fault_due(FaultStep step) {
    return atomic_load_explicit(&fault_armed, memory_order_relaxed) != 0 && fault_take(step);
}

// ---- error.c ----------------------------------------------------------------

// A KafkaError for a BORROWED error (never destroyed here), or `null` for
// NULL. Returns NULL only when Node-API itself failed.
napi_value error_to_js(napi_env env, const kafka_common_Error_t* e);

// A KafkaError for an OWNED error, which is destroyed once converted;
// `null` for NULL.
napi_value error_take_to_js(napi_env env, kafka_common_Error_t* e);

// Throws a KafkaError for an OWNED error, which is destroyed.
void error_throw_owned(napi_env env, kafka_common_Error_t* e);

// The counts errorStats() reads (test-only): an owned error received from
// the core (ffi_take, and a completion's `error`), one the addon made itself,
// and one whose ownership it gave the core. A no-op for NULL.
void error_stat_made(const kafka_common_Error_t* e);
void error_stat_given(const kafka_common_Error_t* e);

// Destroys an OWNED error, swallowing a panic reported by the destroy itself.
// Only where no JS can run (an environment teardown) or while converting an
// error: everywhere else an error with nowhere to go is error_warn'ed.
void error_discard(kafka_common_Error_t* e);

// The payload of an error the core reports, as a plain object keyed by Java
// field name, or NULL when its class has none (native/error_payload.c,
// generated). Each field is set by one of the payload_* helpers below
// (native/error.c), given the value an FFI accessor returned and the owned
// error it failed with, or NULL: a failure leaves the field out. They return
// -1 when a Node-API call failed, 0 otherwise, and free an owned value.
napi_value error_payload_to_js(napi_env env, int code, const kafka_common_Error_t* e);
int payload_string_list(napi_env env, napi_value obj, const char* name, kafka_List_t* list, kafka_common_Error_t* failed);
int payload_topic_partition_list(napi_env env, napi_value obj, const char* name, kafka_List_t* list,
                                 kafka_common_Error_t* failed);
int payload_topic_partition_long_map(napi_env env, napi_value obj, const char* name, kafka_Map_t* map,
                                     kafka_common_Error_t* failed);
int payload_string(napi_env env, napi_value obj, const char* name, const char* value, kafka_common_Error_t* failed);
int payload_int32(napi_env env, napi_value obj, const char* name, int32_t value, kafka_common_Error_t* failed);
int payload_double(napi_env env, napi_value obj, const char* name, double value, kafka_common_Error_t* failed);
// A borrowed metric name as { name, group, description, tags }, its tags an
// array of [key, value] pairs sorted by key; NULL as null.
int payload_metric_name(napi_env env, napi_value obj, const char* name, const kafka_common_MetricName_t* value,
                        kafka_common_Error_t* failed);
int payload_int64(napi_env env, napi_value obj, const char* name, int64_t value, kafka_common_Error_t* failed);
int payload_topic_partition(napi_env env, napi_value obj, const char* name, const kafka_common_TopicPartition_t* value,
                            kafka_common_Error_t* failed);
int payload_timestamp_type(napi_env env, napi_value obj, const char* name, const kafka_common_record_TimestampType_t* value,
                           kafka_common_Error_t* failed);
int payload_deserialization_origin(napi_env env, napi_value obj, const char* name,
                                   const kafka_common_RecordDeserializationError_DeserializationErrorOrigin_t* value,
                                   kafka_common_Error_t* failed);
int payload_bytes(napi_env env, napi_value obj, const char* name, kafka_Bytes_t value, kafka_common_Error_t* failed);
int payload_headers(napi_env env, napi_value obj, const char* name, const kafka_common_header_internals_RecordHeaders_t* value,
                    kafka_common_Error_t* failed);
int payload_topic_partition_offset_map(napi_env env, napi_value obj, const char* name, kafka_Map_t* map,
                                       kafka_common_Error_t* failed);

// An OWNED error with nowhere else to go: when `js` (JS may run here: not an
// environment teardown), it is converted and handed to process.emitWarning
// (a pending JS exception is kept and thrown again after), then destroyed;
// otherwise only destroyed. A no-op for NULL.
void error_warn(napi_env env, int js, kafka_common_Error_t* e);

// The failure of a completion whose answer can't be built, so that its call
// still settles: the pending JS exception, cleared, or, when none is pending
// (or it is null, which the JS side would read as success), the Error
// js_throw_last_napi_error(env, what) throws, taken back. NULL only when even
// that fails, the exception, if any, left pending for the caller's warning.
// A completion takes it after its other arguments, so none is lost.
napi_value js_take_failure(napi_env env, const char* what);

// ---- callback_queue.c -------------------------------------------------------
//
// The callback queue of one client: the C FFI queues every callback of a
// client (delivery callbacks, `_cb` completions) on the client's callback
// vector, and fires the notify hook installed with `__set_callbacks_notify`
// when that vector goes from empty to non-empty, from any thread. The hook
// (cbq_notify) only calls a threadsafe function; its call_js then runs the
// drain on the JS thread, which calls the client's `__execute_callbacks`
// while it returns > 0. Drains never nest. The queue also keeps Node running
// while the client is open or one of its `_cb` calls waits for its
// completion, keeps the client's handle object from garbage collection until
// the core reports the client closed, and owns the threadsafe function's
// teardown (javascript/CLAUDE.md, rules 2, 3 and 10, "Threads and
// liveness"). Each client embeds one and passes its own
// `__execute_callbacks`.

// The client's `__execute_callbacks_err` over `client`: how many callbacks ran.
typedef int32_t (*cbq_execute_fn)(void* client, kafka_common_Error_t** out_err);
// A client hook run on the JS thread.
typedef void (*cbq_client_fn)(napi_env env, void* client);

typedef struct {
    napi_env env;                       // the environment that created it (JS thread)
    void* client;                       // what the hooks receive
    cbq_execute_fn execute;             // the client's __execute_callbacks
    cbq_client_fn on_tsfn_finalize;     // the tsfn's finalizer: drops the tsfn's ownership of client
    napi_threadsafe_function tsfn;
    int tsfn_alive;                     // not released or finalized yet
    pthread_mutex_t notify_mtx;         // guards notify_open and the hook's tsfn call
    int notify_open;                    // the hook may call the tsfn
    napi_ref self_ref;                  // the client's handle object; strong until the core reports it closed
    int self_strong;                    // self_ref is strong: no close the core reported yet
    int pending;                        // the client's `_cb` calls whose completion has not run yet
    int held;                           // the tsfn is referenced now (Node-API creates it referenced)
    int draining;                       // a drain loop is running
    int stopped;                        // the client's destroy began: no drain from now on
    int finalizing;                     // the handle object is being finalized: run no JS
} CallbackQueue;

// Creates the threadsafe function, referenced (the client keeps Node running
// while it is open or a call made on it waits for its answer), named `name`.
// Returns 0, or -1 with a JS exception pending and nothing left to free. From
// then on the tsfn is one owner of `client`: its finalizer calls
// `on_tsfn_finalize`.
int cbq_init(napi_env env, CallbackQueue* q, const char* name, void* client, cbq_execute_fn execute,
             cbq_client_fn on_tsfn_finalize);

// Takes a strong reference on the client's handle object, made weak by
// cbq_closed, so garbage collection never finalizes an open client. Returns
// 0, or -1 with a JS exception pending.
int cbq_attach(napi_env env, CallbackQueue* q, napi_value handle);

// The notify hook (`kafka_producer_Producer__set_callbacks_notify`'s, opaque
// the CallbackQueue): from any thread, it only signals the threadsafe
// function, under `notify_mtx` and while `notify_open`.
void cbq_notify(void* opaque);

// The core reported the client closed (a close completion without error, or
// with any error but a rejected timeout): Node may exit once no call made on
// the client waits for its answer (the tsfn stays alive for those answers),
// and the reference on the handle object becomes weak, so its finalizer may
// destroy the client. Returns 1 the first time, 0 for a client already
// closed. JS thread. This records the core's answer: no call is refused on
// it (javascript/CLAUDE.md, rule 1).
int cbq_closed(napi_env env, CallbackQueue* q);

// Counts one `_cb` call of the client, made right after this returns (after
// every early return of the function making it): until its completion runs
// cbq_call_end, the call holds Node, so a call made on a closed client gets
// its answer. JS thread.
void cbq_call_begin(napi_env env, CallbackQueue* q);

// The last statement of the completion of a `_cb` call cbq_call_begin
// counted, on every path: run by the drain, inline inside the call, or by the
// destroy. Only the release of the client struct holding the queue, which may
// free it, comes after. Once no call waits and the client is closed, Node may
// exit: after the JS callback ran, so the code awaiting it runs first. JS
// thread.
void cbq_call_end(napi_env env, CallbackQueue* q);

// Whether the core reported the client closed (cbq_closed ran): what the
// handle object's finalizer asks before it destroys the client.
int cbq_is_closed(const CallbackQueue* q);

// The drain: the client's __execute_callbacks while it returns > 0. A no-op
// inside a drain, once stopped, or while finalizing.
void cbq_drain(napi_env env, CallbackQueue* q);

// Ends the queue once the client is destroyed (no callback can fire any
// more): closes the hook's way to the tsfn and releases the tsfn, whose
// finalizer then drops its ownership of the client.
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

// Builds the C record. `headers` is `null` or the record's headers as
// headers_build (native/headers.c) reads them, copied into the record during
// this call. Key and value point at `key` / `value` (or are Java's null),
// which must stay valid until the hand-over. Returns NULL with a JS exception
// pending on failure.
kafka_producer_ProducerRecord_t* record_build(napi_env env, const char* topic, int32_t partition,
                                              int64_t timestamp, const JsBytes* key,
                                              const JsBytes* value, napi_value headers);

// ---- values.c ---------------------------------------------------------------

// A client's configuration as a kafka_Map_t: the map borrows the C strings,
// which config_map_free frees after it.
typedef struct {
    kafka_Map_t* map;
    char** strings;
    uint32_t count;
} ConfigMap;

// Builds `out` from `entries`, an array of [key, value] pairs, the bytes of
// each string (encodeLikeJava's, src/internal/config.ts). Returns 0, or -1
// with a JS exception pending and nothing left to free.
int config_map_build(napi_env env, napi_value entries, ConfigMap* out);

// Destroys the map, then frees its strings: after the client's config is built.
void config_map_free(napi_env env, ConfigMap* map);

// ---- headers.c -------------------------------------------------------------

// A new native RecordHeaders holding a copy of a record's headers: `pairs` is
// an array of each header's key, encodeLikeJava's bytes (a Uint8Array), then
// its value, a Uint8Array or null (src/common/header/headers.ts
// `internalToNative`). The caller destroys it with headers_destroy. Returns
// NULL with a JS exception pending on failure: a `TypeError` for a key that
// is not bytes or holds a NUL, or a value that is neither bytes nor null.
kafka_common_header_internals_RecordHeaders_t* headers_build(napi_env env, napi_value pairs);

// Destroys a RecordHeaders headers_build made.
void headers_destroy(napi_env env, kafka_common_header_internals_RecordHeaders_t* headers);

// The headers a RecordHeaders the core lends out holds, copied into JS: an
// array of [key, value] pairs, each key a string (null for a NULL key) and
// each value a Buffer of its own or null. NULL with a JS exception pending on
// failure.
napi_value headers_to_js(napi_env env, const kafka_common_header_internals_RecordHeaders_t* headers);

// The number of headers a RecordHeaders the core lends out holds, 0 when it
// can't be read.
int32_t headers_count(const kafka_common_header_internals_RecordHeaders_t* headers);

// ---- metrics.c --------------------------------------------------------------

// A handle object owning `view`, which its finalizer destroys; on failure the
// view is destroyed and NULL returned with a JS exception pending.
napi_value metrics_view_take_to_js(napi_env env, kafka_common_metrics_MetricsView_t* view);

// ---- record_spy.c ----------------------------------------------------------

// Test-only: while a test armed recordSpy(), keeps what the record a send
// built holds, read back, and whether its key and value are the memory of
// the caller's typed arrays `key` and `value`. A no-op otherwise.
void record_spy_capture(napi_env env, const kafka_producer_ProducerRecord_t* record, napi_value key, napi_value value);

// producer.c: the JS value an error JS gave a mock carries, for the
// LocalCallbackError `opaque` the mock's error holds, or NULL when `opaque`
// is not one (another callback's error) or its value is gone (the mock
// destroyed).
napi_value injected_error_value(napi_env env, void* opaque);

// ---- the native functions addon.c registers -----------------------------------

// error.c: setFactories(errorFactory, metadataFactory).
napi_value js_set_factories(napi_env env, napi_callback_info info);
napi_value js_error_stats(napi_env env, napi_callback_info info);

// producer.c: the KafkaProducer handle.
napi_value js_producer_create(napi_env env, napi_callback_info info);
napi_value js_mock_producer_create(napi_env env, napi_callback_info info);
napi_value js_mock_producer_send(napi_env env, napi_callback_info info);
napi_value js_mock_producer_history(napi_env env, napi_callback_info info);
napi_value js_mock_producer_clear(napi_env env, napi_callback_info info);
napi_value js_mock_producer_set_error(napi_env env, napi_callback_info info);
napi_value js_mock_producer_complete_next(napi_env env, napi_callback_info info);
napi_value js_mock_producer_state(napi_env env, napi_callback_info info);
napi_value js_producer_send(napi_env env, napi_callback_info info);
napi_value js_producer_flush(napi_env env, napi_callback_info info);
napi_value js_producer_partitions_for(napi_env env, napi_callback_info info);
napi_value js_producer_metrics(napi_env env, napi_callback_info info);
napi_value js_producer_close(napi_env env, napi_callback_info info);
napi_value js_producer_pump(napi_env env, napi_callback_info info);
napi_value js_record_future_get(napi_env env, napi_callback_info info);
napi_value js_producer_stats(napi_env env, napi_callback_info info);

// metrics.c: the live view and the metrics taken from it.
napi_value js_metrics_view_size(napi_env env, napi_callback_info info);
napi_value js_metrics_view_has(napi_env env, napi_callback_info info);
napi_value js_metrics_view_get(napi_env env, napi_callback_info info);
napi_value js_metrics_view_entries(napi_env env, napi_callback_info info);
napi_value js_metric_value(napi_env env, napi_callback_info info);
napi_value js_metric_is_measurable(napi_env env, napi_callback_info info);
napi_value js_metrics_stats(napi_env env, napi_callback_info info);

// headers.c: the counts of the RecordHeaders the sends made.
napi_value js_header_stats(napi_env env, napi_callback_info info);

// record_spy.c: the records the sends built, for the tests.
napi_value js_record_spy(napi_env env, napi_callback_info info);

#endif  // CONFLUENT_KAFKA_JS_UTIL_H

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

// The producer: a marshaling-only bridge to the Rust producer FFI
// (javascript/CLAUDE.md). It runs no thread of its own and holds no Kafka
// logic.
//
// Every operation uses the `_cb` twin of its Java method (send_with_callback_cb,
// flush_cb, close_cb / close_with_timeout_cb), whose completion, like every
// delivery callback, the core QUEUES on the producer's callback vector. The
// hook installed with __set_callbacks_notify fires, from any thread, once each
// time that vector goes from empty to non-empty, and only calls
// napi_call_threadsafe_function; the threadsafe function's call_js then runs
// on the JS main thread and drains the vector with __execute_callbacks while it
// returns > 0. So every completion runs on the JS thread, the one drainer, and
// builds its JS values there. A send's hand-over completion may also fire
// inside send_with_callback_cb itself, on the JS thread (the record taken or
// refused at once), so a send's whole state is in place before that call.
//
// Liveness: the threadsafe function is ref'd, and the handle object kept from
// garbage collection by a strong reference, while sends, flushes or a close are
// outstanding (`pending`), and unref'd when idle.
//
// Teardown: close's completion only marks the producer; call_js destroys it
// after its drain loop (never inside a drain), on the JS thread, then releases
// the threadsafe function, whose finalizer frees this struct together with the
// handle object's finalizer (two owners). `KafkaProducer_destroy_err` runs the
// callbacks still pending, inline, exactly once each.

#include <stdlib.h>
#include <string.h>

#include "util.h"

typedef struct SendCtx SendCtx;

typedef struct {
    napi_env env;                              // the environment that created it (JS thread)
    kafka_producer_KafkaProducer_t* kp;        // owned class handle, NULL once destroyed
    const kafka_producer_Producer_t* view;     // borrowed __as_Producer view of kp
    napi_threadsafe_function tsfn;
    int tsfn_alive;                            // not released yet
    napi_ref self_ref;                         // the handle object; strong while busy
    int64_t pending;                           // outstanding sends, flushes and close
    int busy;                                  // tsfn and self_ref currently ref'd
    int closing;                               // a close is in progress
    int destroy_requested;                     // close completed: destroy after the drain
    int destroying;                            // KafkaProducer_destroy_err began
    int finalizing;                            // the handle object is being finalized: no JS
    napi_ref close_done;                       // close's JS completion
    kafka_common_Error_t* close_error;         // close's owned error, until reported
    SendCtx* orphans;                          // contexts of sends that failed at hand-over
    int owners;                                // the handle object and the tsfn
} Producer;

// One send. Two owners: the hand-over completion and the delivery callback.
struct SendCtx {
    Producer* p;
    JsBytes key;                               // borrowed bytes, until the hand-over
    JsBytes value;
    napi_ref key_ref;                          // keeps the key Buffer alive until the hand-over
    napi_ref value_ref;
    napi_ref on_handover;                      // JS (error | null) => void
    napi_ref on_complete;                      // JS (metadata, error | null) => string | undefined
    int refs;                                  // owners left
    int counted;                               // holds one unit of p->pending
    int handed_over;
    int handover_failed;
    int delivered;
    int orphaned;                              // on p->orphans: freed after destroy
    SendCtx* next;
};

typedef struct {
    Producer* p;
    napi_ref done;                             // JS (error | null) => void
} FlushCtx;

static void producer_release(Producer* p) {
    if (--p->owners == 0) free(p);
}

// Refs the tsfn and the handle object while there is pending work, unrefs them
// when idle or destroyed. JS thread only.
static void update_liveness(napi_env env, Producer* p) {
    int want = p->pending > 0 && p->tsfn_alive && !p->finalizing;
    if (want == p->busy) return;
    if (want) {
        napi_ref_threadsafe_function(env, p->tsfn);
        napi_reference_ref(env, p->self_ref, NULL);
    } else {
        if (p->tsfn_alive) napi_unref_threadsafe_function(env, p->tsfn);
        if (!p->finalizing) napi_reference_unref(env, p->self_ref, NULL);
    }
    p->busy = want;
}

static void pending_add(napi_env env, Producer* p, int64_t delta) {
    p->pending += delta;
    update_liveness(env, p);
}

static void delete_ref(napi_env env, napi_ref* ref) {
    if (*ref != NULL) {
        napi_delete_reference(env, *ref);
        *ref = NULL;
    }
}

// Calls the function `ref` holds with `argc` arguments, on the JS thread.
// Returns its result, or NULL when it threw: the exception is cleared and its
// message stored in `*thrown` (malloc'ed) when `thrown` is not NULL.
static napi_value call_ref(napi_env env, napi_ref ref, size_t argc, napi_value* argv, char** thrown) {
    napi_value fn, global, result = NULL;
    if (napi_get_reference_value(env, ref, &fn) != napi_ok || fn == NULL || napi_get_global(env, &global) != napi_ok)
        return NULL;
    napi_status status = napi_call_function(env, global, fn, argc, argv, &result);
    if (status == napi_ok) return result;
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) {
        napi_value exc, text;
        napi_get_and_clear_last_exception(env, &exc);
        if (thrown != NULL && napi_coerce_to_string(env, exc, &text) == napi_ok) {
            size_t len = 0;
            if (napi_get_value_string_utf8(env, text, NULL, 0, &len) == napi_ok) {
                char* s = (char*)malloc(len + 1);
                if (s != NULL && napi_get_value_string_utf8(env, text, s, len + 1, &len) == napi_ok) *thrown = s;
                else free(s);
            }
        }
    }
    return NULL;
}

// ---- RecordMetadata ---------------------------------------------------------

// A RecordMetadata (through lib's factory) from a BORROWED metadata handle.
static napi_value metadata_to_js(napi_env env, const kafka_producer_RecordMetadata_t* m) {
    napi_value result;
    if (m == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &result), NULL);
        return result;
    }
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = kafka_producer_RecordMetadata_topic_err(m, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int32_t partition = kafka_producer_RecordMetadata_partition_err(m, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int8_t has_offset = kafka_producer_RecordMetadata_has_offset_err(m, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int64_t offset = -1;
    if (has_offset) {
        offset = kafka_producer_RecordMetadata_offset_err(m, &ffi_err);
        error_discard(ffi_take(&ffi_err));
    }
    int8_t has_timestamp = kafka_producer_RecordMetadata_has_timestamp_err(m, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int64_t timestamp = -1;
    if (has_timestamp) {
        timestamp = kafka_producer_RecordMetadata_timestamp_err(m, &ffi_err);
        error_discard(ffi_take(&ffi_err));
    }
    int32_t key_size = kafka_producer_RecordMetadata_serialized_key_size_err(m, &ffi_err);
    error_discard(ffi_take(&ffi_err));
    int32_t value_size = kafka_producer_RecordMetadata_serialized_value_size_err(m, &ffi_err);
    error_discard(ffi_take(&ffi_err));

    napi_value obj, v;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, topic != NULL ? topic : "", NAPI_AUTO_LENGTH, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "topic", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, partition, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "partition", v), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, has_offset != 0, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "hasOffset", v), NULL);
    NAPI_CHECK(env, napi_create_int64(env, offset, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "offset", v), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, has_timestamp != 0, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "hasTimestamp", v), NULL);
    NAPI_CHECK(env, napi_create_int64(env, timestamp, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "timestamp", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, key_size, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "serializedKeySize", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, value_size, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "serializedValueSize", v), NULL);

    AddonData* data = addon_data(env);
    if (data == NULL || data->metadata_factory == NULL) return obj;
    return call_ref(env, data->metadata_factory, 1, &obj, NULL);
}

// ---- send -------------------------------------------------------------------

static void send_ctx_free(napi_env env, SendCtx* ctx) {
    delete_ref(env, &ctx->key_ref);
    delete_ref(env, &ctx->value_ref);
    delete_ref(env, &ctx->on_handover);
    delete_ref(env, &ctx->on_complete);
    free(ctx);
}

// Gives back the record's unit of `pending` once it needs nothing more: the
// hand-over failed (a later delivery callback is not waited for), or it was
// handed over and its delivery callback ran.
static void send_maybe_idle(napi_env env, SendCtx* ctx) {
    if (!ctx->counted) return;
    if (ctx->handover_failed || (ctx->handed_over && ctx->delivered)) {
        ctx->counted = 0;
        pending_add(env, ctx->p, -1);
    }
}

// Drops one owner; the last one frees the context unless it is an orphan,
// which destroy frees.
static void send_ctx_release(napi_env env, SendCtx* ctx) {
    send_maybe_idle(env, ctx);
    if (--ctx->refs != 0 || ctx->orphaned) return;
    send_ctx_free(env, ctx);
}

// The send's hand-over completion (kafka_producer_Producer_send_with_callback_cb_t):
// inside send_with_callback_cb for a record taken or refused at once, else from
// the drain, or from destroy; always on the JS thread. `value` (the record's
// future) is destroyed unread: the outcome comes from the delivery callback.
static void send_on_handover(kafka_common_KafkaFuture_t* value, kafka_common_Error_t* error, void* opaque) {
    SendCtx* ctx = (SendCtx*)opaque;
    Producer* p = ctx->p;
    kafka_common_Error_t* ffi_err = NULL;
    if (value != NULL) error_discard((kafka_common_KafkaFuture_destroy_err(value, &ffi_err), ffi_take(&ffi_err)));
    napi_env env = p->env;
    // The core no longer reads the key and value bytes.
    delete_ref(env, &ctx->key_ref);
    delete_ref(env, &ctx->value_ref);
    ctx->handed_over = 1;
    ctx->handover_failed = error != NULL;
    if (!p->finalizing && ctx->on_handover != NULL) {
        napi_value arg = error_take_to_js(env, error);
        error = NULL;
        if (arg != NULL) call_ref(env, ctx->on_handover, 1, &arg, NULL);
        else {
            napi_value ignored;
            napi_get_and_clear_last_exception(env, &ignored);
        }
    }
    error_discard(error);
    delete_ref(env, &ctx->on_handover);
    if (ctx->handover_failed && --ctx->refs > 0) {
        // The delivery callback may still fire, once (the record had reached
        // the accumulator): keep the context until destroy.
        send_maybe_idle(env, ctx);
        ctx->orphaned = 1;
        ctx->next = p->orphans;
        p->orphans = ctx;
        return;
    }
    if (ctx->handover_failed) {
        send_maybe_idle(env, ctx);
        send_ctx_free(env, ctx);
        return;
    }
    send_ctx_release(env, ctx);
}

// Java's Callback.onCompletion (kafka_producer_Callback_on_completion_fn_t),
// run by the drain or by destroy on the JS thread. Both arguments are
// BORROWED. Order (javascript/CLAUDE.md, rule 6): the JS side runs the user
// callback and then settles the Promise; then the result is reported, once.
static void send_on_completion(void* self_, const kafka_producer_RecordMetadata_t* metadata,
                               const kafka_common_Error_t* error, int64_t callback_id) {
    SendCtx* ctx = (SendCtx*)self_;
    Producer* p = ctx->p;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    if (!p->finalizing && ctx->on_complete != NULL) {
        napi_value argv[2];
        argv[0] = metadata_to_js(env, metadata);
        argv[1] = argv[0] != NULL ? error_to_js(env, error) : NULL;
        char* thrown = NULL;
        if (argv[0] != NULL && argv[1] != NULL) {
            napi_value r = call_ref(env, ctx->on_complete, 2, argv, &thrown);
            napi_valuetype type;
            if (r != NULL && napi_typeof(env, r, &type) == napi_ok && type == napi_string) {
                // The user callback threw: lib/ returns its message.
                size_t len = 0;
                if (napi_get_value_string_utf8(env, r, NULL, 0, &len) == napi_ok) {
                    free(thrown);
                    thrown = (char*)malloc(len + 1);
                    if (thrown != NULL) napi_get_value_string_utf8(env, r, thrown, len + 1, &len);
                }
            }
        } else {
            napi_value ignored;
            napi_get_and_clear_last_exception(env, &ignored);
        }
        if (thrown != NULL) {
            report = kafka_common_Error_local_callback_err(thrown, NULL, NULL, &ffi_err);
            kafka_common_Error_t* panic = ffi_take(&ffi_err);
            if (report == NULL) report = panic;
            else error_discard(panic);
            free(thrown);
        }
    }
    if (!p->destroying) {
        error_discard((kafka_producer_Producer__set_callback_result_err(p->view, callback_id, NULL, report, &ffi_err),
                       ffi_take(&ffi_err)));
    } else {
        // Destroy completes the unreported callbacks itself.
        error_discard(report);
    }
    ctx->delivered = 1;
    delete_ref(env, &ctx->on_complete);
    send_ctx_release(env, ctx);
}

static Producer* unwrap_producer(napi_env env, napi_value handle) {
    void* data = NULL;
    if (napi_unwrap(env, handle, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "not a producer handle");
        return NULL;
    }
    Producer* p = (Producer*)data;
    if (p->kp == NULL || p->destroying) {
        napi_throw_error(env, NULL, "the producer has been destroyed");
        return NULL;
    }
    return p;
}

static int is_function(napi_env env, napi_value v) {
    napi_valuetype type;
    return napi_typeof(env, v, &type) == napi_ok && type == napi_function;
}

// send(handle, topic, partition, timestamp, key, value, headers, onHandover,
// onComplete): partition and timestamp -1 for Java's null; key and value a
// Uint8Array or null; headers null or an array of [key, Uint8Array | null].
// Throws a KafkaError when the record cannot be built (nothing is sent then).
static napi_value js_send(napi_env env, napi_callback_info info) {
    size_t argc = 9;
    napi_value argv[9];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 9 || !is_function(env, argv[7]) || !is_function(env, argv[8])) {
        napi_throw_type_error(env, NULL,
                              "send(handle, topic, partition, timestamp, key, value, headers, onHandover, onComplete)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    int32_t partition = -1;
    int64_t timestamp = -1;
    NAPI_CHECK(env, napi_get_value_int32(env, argv[2], &partition), NULL);
    NAPI_CHECK(env, napi_get_value_int64(env, argv[3], &timestamp), NULL);
    char* topic = js_string_dup(env, argv[1], "topic must be a string");
    if (topic == NULL) return NULL;

    SendCtx* ctx = (SendCtx*)calloc(1, sizeof(SendCtx));
    if (ctx == NULL) {
        free(topic);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    ctx->p = p;
    ctx->refs = 2;
    if (js_bytes(env, argv[4], "key must be a Buffer, a Uint8Array, a string or null", &ctx->key)
        || js_bytes(env, argv[5], "value must be a Buffer, a Uint8Array, a string or null", &ctx->value)) {
        free(topic);
        free(ctx);
        return NULL;
    }
    kafka_producer_ProducerRecord_t* record =
        record_build(env, topic, partition, timestamp, &ctx->key, &ctx->value, argv[6]);
    free(topic);
    if (record == NULL) {
        free(ctx);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    if ((!ctx->key.is_null && napi_create_reference(env, argv[4], 1, &ctx->key_ref) != napi_ok)
        || (!ctx->value.is_null && napi_create_reference(env, argv[5], 1, &ctx->value_ref) != napi_ok)
        || napi_create_reference(env, argv[7], 1, &ctx->on_handover) != napi_ok
        || napi_create_reference(env, argv[8], 1, &ctx->on_complete) != napi_ok) {
        error_discard((kafka_producer_ProducerRecord_destroy_err(record, &ffi_err), ffi_take(&ffi_err)));
        send_ctx_free(env, ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    kafka_producer_Callback_t* callback = NULL;
    kafka_common_Error_t* err =
        (callback = kafka_producer_Callback_new_err(ctx, send_on_completion, &ffi_err), ffi_take(&ffi_err));
    if (err != NULL) {
        // Never registered: neither completion can fire.
        error_discard((kafka_producer_ProducerRecord_destroy_err(record, &ffi_err), ffi_take(&ffi_err)));
        send_ctx_free(env, ctx);
        error_throw_owned(env, err);
        return NULL;
    }
    // Everything the completions read is in place: the hand-over completion can
    // fire inside this call.
    ctx->counted = 1;
    pending_add(env, p, 1);
    kafka_producer_Producer_send_with_callback_cb(p->view, record, callback, send_on_handover, ctx);
    // The call consumed the callback and copied the record; the key and value
    // bytes stay borrowed until send_on_handover (ctx holds them).
    error_discard((kafka_producer_ProducerRecord_destroy_err(record, &ffi_err), ffi_take(&ffi_err)));
    return NULL;
}

// ---- flush ------------------------------------------------------------------

static void flush_on_complete(kafka_common_Error_t* error, void* opaque) {
    FlushCtx* ctx = (FlushCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    if (!p->finalizing) {
        napi_value arg = error_take_to_js(env, error);
        error = NULL;
        if (arg != NULL) call_ref(env, ctx->done, 1, &arg, NULL);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    pending_add(env, p, -1);
}

// flush(handle, done): Producer.flush() through flush_cb; `done(error | null)`
// runs once every record sent before it completed and its callback reported.
static napi_value js_flush(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2 || !is_function(env, argv[1])) {
        napi_throw_type_error(env, NULL, "flush(handle, done)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    FlushCtx* ctx = (FlushCtx*)calloc(1, sizeof(FlushCtx));
    if (ctx == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    ctx->p = p;
    if (napi_create_reference(env, argv[1], 1, &ctx->done) != napi_ok) {
        free(ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    pending_add(env, p, 1);
    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);
    return NULL;
}

// ---- close and destroy ------------------------------------------------------

static void free_orphans(napi_env env, Producer* p) {
    while (p->orphans != NULL) {
        SendCtx* next = p->orphans->next;
        send_ctx_free(env, p->orphans);
        p->orphans = next;
    }
}

static void noop_notify(void* opaque) {
    (void)opaque;
}

// Destroys the class handle on the JS thread, outside any drain: the core runs
// the still-pending callbacks here, inline, once each. Then releases the tsfn.
static void producer_destroy(napi_env env, Producer* p) {
    kafka_common_Error_t* ffi_err = NULL;
    p->destroying = 1;
    if (p->finalizing) {
        // The tsfn may already be finalized (environment teardown): the hook
        // must not reach it.
        error_discard((kafka_producer_Producer__set_callbacks_notify_err(p->view, noop_notify, NULL, &ffi_err),
                       ffi_take(&ffi_err)));
    }
    error_discard((kafka_producer_KafkaProducer_destroy_err(p->kp, &ffi_err), ffi_take(&ffi_err)));
    p->kp = NULL;
    p->view = NULL;
    // No callback can fire past this point.
    free_orphans(env, p);
    if (p->tsfn_alive) {
        if (p->busy) napi_unref_threadsafe_function(env, p->tsfn);
        p->tsfn_alive = 0;
        napi_release_threadsafe_function(p->tsfn, napi_tsfn_release);
    }
    update_liveness(env, p);
}

// The close completion (close_cb / close_with_timeout_cb), on the JS thread.
// A timeout the core rejected (Java's IllegalArgumentException) closed
// nothing: the producer stays usable. Otherwise the producer is destroyed
// after the drain this runs in, and `done` runs then.
static void close_on_complete(kafka_common_Error_t* error, void* opaque) {
    Producer* p = (Producer*)opaque;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    int rejected = 0;
    if (error != NULL) {
        int code = (int)kafka_common_Error__enum_err(error, &ffi_err);
        error_discard(ffi_take(&ffi_err));
        rejected = code == kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT;
    }
    if (rejected) {
        p->closing = 0;
        if (!p->finalizing) {
            napi_value argv[2];
            argv[0] = error_take_to_js(env, error);
            if (argv[0] != NULL && napi_get_boolean(env, false, &argv[1]) == napi_ok)
                call_ref(env, p->close_done, 2, argv, NULL);
        } else {
            error_discard(error);
        }
        delete_ref(env, &p->close_done);
        pending_add(env, p, -1);
        return;
    }
    p->close_error = error;
    p->destroy_requested = 1;
    // call_js destroys it once its drain loop ended; this may also have fired
    // outside a drain, so schedule one.
    if (p->tsfn_alive) napi_call_threadsafe_function(p->tsfn, NULL, napi_tsfn_nonblocking);
}

// Runs the destroy a close completion requested, then reports the close.
static void producer_finish_close(napi_env env, Producer* p) {
    producer_destroy(env, p);
    napi_value argv[2];
    argv[0] = error_take_to_js(env, p->close_error);
    p->close_error = NULL;
    if (argv[0] != NULL && napi_get_boolean(env, true, &argv[1]) == napi_ok && p->close_done != NULL)
        call_ref(env, p->close_done, 2, argv, NULL);
    delete_ref(env, &p->close_done);
    p->closing = 0;
    pending_add(env, p, -1);
}

// close(handle, timeoutMs, done): Producer.close() for a null timeoutMs, else
// close(Duration.ofMillis(timeoutMs)), through the `_cb` twins.
// `done(error | null, destroyed)` runs once the close completed and, unless the
// timeout was rejected (`destroyed` false), the native producer was destroyed.
static napi_value js_close(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "close(handle, timeoutMs, done)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    if (p->closing) {
        napi_throw_error(env, NULL, "a close is already in progress");
        return NULL;
    }
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    int64_t timeout = 0;
    if (type != napi_null && type != napi_undefined) NAPI_CHECK(env, napi_get_value_int64(env, argv[1], &timeout), NULL);
    NAPI_CHECK(env, napi_create_reference(env, argv[2], 1, &p->close_done), NULL);
    p->closing = 1;
    pending_add(env, p, 1);
    if (type == napi_null || type == napi_undefined) kafka_producer_Producer_close_cb(p->view, close_on_complete, p);
    else kafka_producer_Producer_close_with_timeout_cb(p->view, timeout, close_on_complete, p);
    return NULL;
}

// ---- the pump ---------------------------------------------------------------

// Fired by the core once per empty -> non-empty transition of the callback
// vector, from any thread. It only signals.
static void producer_callbacks_notify(void* opaque) {
    Producer* p = (Producer*)opaque;
    napi_call_threadsafe_function(p->tsfn, NULL, napi_tsfn_nonblocking);
}

// The tsfn's call_js, on the JS thread: the one drainer.
static void producer_call_js(napi_env env, napi_value js_cb, void* context, void* data) {
    (void)js_cb;
    (void)data;
    Producer* p = (Producer*)context;
    if (env == NULL || p->kp == NULL || p->destroying || p->finalizing) return;
    kafka_common_Error_t* ffi_err = NULL;
    while (p->kp != NULL) {
        int32_t ran = kafka_producer_Producer__execute_callbacks_err(p->view, &ffi_err);
        error_discard(ffi_take(&ffi_err));
        if (ran <= 0) break;
    }
    if (p->destroy_requested && !p->destroying) producer_finish_close(env, p);
}

static void producer_tsfn_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    Producer* p = (Producer*)data;
    p->tsfn_alive = 0;
    if (p->kp != NULL && !p->destroying) {
        // Environment teardown finalized the tsfn before the handle object: a
        // later hook call must not reach it.
        kafka_common_Error_t* ffi_err = NULL;
        error_discard((kafka_producer_Producer__set_callbacks_notify_err(p->view, noop_notify, NULL, &ffi_err),
                       ffi_take(&ffi_err)));
    }
    producer_release(p);
}

// The handle object's finalizer: garbage collection of a producer not closed,
// or environment teardown. It destroys the class handle without running JS.
static void producer_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    Producer* p = (Producer*)data;
    p->finalizing = 1;
    if (p->kp != NULL && !p->destroying) producer_destroy(env, p);
    error_discard(p->close_error);
    p->close_error = NULL;
    delete_ref(env, &p->close_done);
    delete_ref(env, &p->self_ref);
    producer_release(p);
}

// ---- construction -----------------------------------------------------------

// create(entries): a native producer handle over KafkaProducer(configs) with
// NULL serializers (key and value cross as kafka_Bytes_t), `entries` an array
// of [key, value] strings. Throws a KafkaError when the configuration is
// rejected or the producer cannot be built.
static napi_value js_create(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool is_array = false;
    if (argc != 1 || napi_is_array(env, argv[0], &is_array) != napi_ok || !is_array) {
        napi_throw_type_error(env, NULL, "create(entries)");
        return NULL;
    }
    uint32_t count = 0;
    NAPI_CHECK(env, napi_get_array_length(env, argv[0], &count), NULL);
    char** strings = (char**)calloc((size_t)count * 2 + 1, sizeof(char*));
    if (strings == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* props = NULL;
    kafka_producer_ProducerConfig_t* config = NULL;
    kafka_producer_KafkaProducer_t* kp = NULL;
    napi_value result = NULL;
    int thrown = 0;
    // A C-built kafka_Map_t borrows its char * keys and values: they are freed
    // after the map.
    kafka_common_Error_t* err = (props = kafka_Map_new_err(&ffi_err), ffi_take(&ffi_err));
    for (uint32_t i = 0; err == NULL && !thrown && i < count; i++) {
        napi_value entry, k, v;
        if (napi_get_element(env, argv[0], i, &entry) != napi_ok || napi_get_element(env, entry, 0, &k) != napi_ok
            || napi_get_element(env, entry, 1, &v) != napi_ok) {
            js_throw_last_napi_error(env, "reading a config entry");
            thrown = 1;
            break;
        }
        strings[2 * i] = js_string_dup(env, k, "config keys must be strings");
        strings[2 * i + 1] = strings[2 * i] != NULL ? js_string_dup(env, v, "config values must be strings") : NULL;
        if (strings[2 * i + 1] == NULL) {
            thrown = 1;
            break;
        }
        err = (kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err), ffi_take(&ffi_err));
    }
    if (err == NULL && !thrown) err = (config = kafka_producer_ProducerConfig_new(props, &ffi_err), ffi_take(&ffi_err));
    if (props != NULL) error_discard((kafka_Map_destroy_err(props, &ffi_err), ffi_take(&ffi_err)));
    for (uint32_t i = 0; i < count * 2; i++) free(strings[i]);
    free(strings);
    if (err == NULL && !thrown) {
        err = (kp = kafka_producer_KafkaProducer_new(config, NULL, NULL, &ffi_err), ffi_take(&ffi_err));
    }
    if (config != NULL) error_discard((kafka_producer_ProducerConfig_destroy_err(config, &ffi_err), ffi_take(&ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (thrown) return NULL;

    Producer* p = (Producer*)calloc(1, sizeof(Producer));
    if (p == NULL) {
        error_discard((kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err), ffi_take(&ffi_err)));
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    p->env = env;
    p->kp = kp;
    err = (p->view = kafka_producer_KafkaProducer__as_Producer_err(kp, &ffi_err), ffi_take(&ffi_err));
    napi_value name;
    if (err == NULL
        && (napi_create_string_utf8(env, "confluent-kafka producer callbacks", NAPI_AUTO_LENGTH, &name) != napi_ok
            || napi_create_threadsafe_function(env, NULL, NULL, name, 0, 1, p, producer_tsfn_finalize, p,
                                               producer_call_js, &p->tsfn) != napi_ok)) {
        js_throw_last_napi_error(env, "napi_create_threadsafe_function");
        thrown = 1;
    }
    if (err == NULL && !thrown) {
        p->tsfn_alive = 1;
        p->owners = 1;  // the tsfn
        // Idle until something is pending.
        napi_unref_threadsafe_function(env, p->tsfn);
        if (napi_create_object(env, &result) != napi_ok || napi_wrap(env, result, p, producer_finalize, NULL, NULL) != napi_ok) {
            js_throw_last_napi_error(env, "napi_wrap");
            thrown = 1;
        } else {
            p->owners = 2;  // and the handle object
            if (napi_create_reference(env, result, 0, &p->self_ref) != napi_ok) {
                js_throw_last_napi_error(env, "napi_create_reference");
                thrown = 1;
            }
        }
    }
    if (err == NULL && !thrown)
        err = (kafka_producer_Producer__set_callbacks_notify_err(p->view, producer_callbacks_notify, p, &ffi_err),
               ffi_take(&ffi_err));
    if (err != NULL || thrown) {
        error_discard((kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err), ffi_take(&ffi_err)));
        p->kp = NULL;
        p->destroying = 1;
        if (p->tsfn_alive) {
            p->tsfn_alive = 0;
            napi_release_threadsafe_function(p->tsfn, napi_tsfn_release);  // frees p when it was the only owner
        } else {
            free(p);
        }
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return result;
}

napi_value producer_init(napi_env env, napi_value exports) {
    napi_property_descriptor props[] = {
        {"producerCreate", NULL, js_create, NULL, NULL, NULL, napi_default, NULL},
        {"producerSend", NULL, js_send, NULL, NULL, NULL, napi_default, NULL},
        {"producerFlush", NULL, js_flush, NULL, NULL, NULL, napi_default, NULL},
        {"producerClose", NULL, js_close, NULL, NULL, NULL, napi_default, NULL},
    };
    NAPI_CHECK(env, napi_define_properties(env, exports, sizeof(props) / sizeof(props[0]), props), NULL);
    return exports;
}

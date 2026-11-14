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

// The KafkaProducer handle: a marshaling-only bridge to the Rust producer FFI
// (javascript/CLAUDE.md). It runs no thread of its own and holds no Kafka
// logic.
//
// Every operation uses the `_cb` twin of its Java method (send_with_callback_cb,
// flush_cb, close_cb / close_with_timeout_cb), whose completion, like every
// delivery callback, the core QUEUES on the producer's callback vector; the
// producer's CallbackQueue (callback_queue.c) drains it on the JS thread, from
// the event loop or from send()'s `pump`. So every completion runs on the JS
// thread and builds its JS values there. A send's hand-over completion may
// also fire inside send_with_callback_cb itself, on the JS thread (the record
// taken or refused at once), so a send's whole state is in place before that
// call.
//
// Each request has its own context (SendCtx, FlushCtx, CloseCtx,
// PartitionsForCtx), whose
// pointer is what the core receives as the completion's opaque. A send that
// fails at hand-over parks its context on the orphan list, freed only after
// destroy.
//
// A delivery callback that returns a Promise is awaited: its report through
// `__set_callback_result` waits until the Promise settled (send_on_settled),
// so the record's future, `flush` and `close` wait for it as for a callback
// that returned. The callback id stays in C, on the context, meanwhile; the
// context and the producer struct stay allocated until the function the
// Promise calls back is finalized.
//
// Closed state and teardown (javascript/CLAUDE.md, rules 1 and 10): the
// binding keeps no closed flag and refuses no call; a call after close() goes
// to the FFI, whose error is the core's. close() does not free the native
// handle: its completion only lets Node exit and lets the handle object be
// collected (cbq_closed). The handle object's finalizer then destroys the
// closed producer (outside any drain, running no JS), and releases the
// threadsafe function, whose finalizer frees this struct together with the
// handle object's finalizer (two owners). `KafkaProducer_destroy_err` runs the
// callbacks still pending, inline, exactly once each. No finalizer closes or
// destroys a producer still open: one finalized open (only an environment
// teardown can, as the handle object is held strongly until the close) is
// left to the core, and its memory to the process.

#include <stdlib.h>
#include <string.h>

#include "util.h"

typedef struct SendCtx SendCtx;

typedef struct {
    napi_env env;                              // the environment that created it (JS thread)
    kafka_producer_KafkaProducer_t* kp;        // owned class handle, NULL once destroyed
    const kafka_producer_Producer_t* view;     // borrowed __as_Producer view of kp
    CallbackQueue queue;                       // notify hook, tsfn, drain, liveness
    int destroying;                            // KafkaProducer_destroy_err began: no report from now on
    SendCtx* orphans;                          // contexts of sends that failed at hand-over
    SendCtx* awaiting;                         // contexts whose callback's Promise has not settled
    int owners;                                // the handle object, the tsfn, each awaited callback's function
    // The send contexts whose record's future JS may wait on, by the slot
    // number JS holds (a small integer, so no object is made per record);
    // a free slot holds NULL and is chained through `free_next`.
    SendCtx** slots;
    int32_t* free_next;
    int32_t slot_count;
    int32_t free_head;                         // -1: none free
} Producer;

// One send. Two owners: the hand-over completion and the delivery callback.
struct SendCtx {
    Producer* p;
    JsBytes key;                               // the bytes the core reads, borrowed until the hand-over
    JsBytes value;
    napi_ref key_ref;                          // keeps the key's Buffer alive until the hand-over
    napi_ref value_ref;                        // keeps the value's Buffer alive until the hand-over
    napi_ref on_handover;                      // JS (error | null) => void
    napi_ref on_complete;                      // JS (metadata, error | null) => string | undefined
    int refs;                                  // owners left
    int handed_over;
    int handover_failed;
    int delivered;
    int orphaned;                              // on p->orphans: freed after destroy
    SendCtx* next;
    int64_t callback_id;                       // the report's id while the callback's Promise is awaited
    int awaiting;                              // on p->awaiting: the report waits for the Promise
    int settle_fn;                             // the function the Promise calls back is alive: it owns the context
    napi_ref settle_ref;                       // holds that function strongly until the report: no GC reports early
    SendCtx* await_prev;
    SendCtx* await_next;
    kafka_common_KafkaFuture_t* future;        // the record's future (owned), from the hand-over until destroyed
    int waits;                                 // the waits on `future` still pending (each one also holds a ref)
    int js_holds;                              // JS was handed the future's slot number (`slot`)
    int32_t slot;                              // its slot in p->slots while js_holds, else -1
    int js_settled;                            // JS recorded the delivery: it reads the future no more
};

typedef struct {
    Producer* p;
    napi_ref done;                             // JS (error | null) => void
} FlushCtx;

typedef struct {
    Producer* p;
    napi_ref done;                             // JS (error | null, closedNow) => void
} CloseCtx;

typedef struct {
    Producer* p;
    napi_ref done;                             // JS (error | null, partitions | null) => void
} PartitionsForCtx;

static void producer_release(Producer* p) {
    if (--p->owners != 0) return;
    cbq_dispose(&p->queue);
    free(p->slots);
    free(p->free_next);
    free(p);
}

// A free slot for `ctx`, growing the table when none is: its number, or -1
// when out of memory.
static int32_t slot_take(Producer* p, SendCtx* ctx) {
    if (p->free_head < 0) {
        int32_t grown = p->slot_count == 0 ? 64 : p->slot_count * 2;
        if (grown <= p->slot_count) return -1;
        SendCtx** slots = (SendCtx**)realloc(p->slots, (size_t)grown * sizeof(SendCtx*));
        if (slots == NULL) return -1;
        p->slots = slots;
        int32_t* next = (int32_t*)realloc(p->free_next, (size_t)grown * sizeof(int32_t));
        if (next == NULL) return -1;
        p->free_next = next;
        for (int32_t i = grown - 1; i >= p->slot_count; i--) {
            p->slots[i] = NULL;
            p->free_next[i] = p->free_head;
            p->free_head = i;
        }
        p->slot_count = grown;
    }
    int32_t i = p->free_head;
    p->free_head = p->free_next[i];
    p->slots[i] = ctx;
    return i;
}

static void slot_give_back(Producer* p, int32_t i) {
    p->slots[i] = NULL;
    p->free_next[i] = p->free_head;
    p->free_head = i;
}

// ---- RecordMetadata ---------------------------------------------------------

// A RecordMetadata (through the factory src/internal/native.ts registers) from a
// BORROWED metadata handle.
static napi_value metadata_to_js(napi_env env, const kafka_producer_RecordMetadata_t* m) {
    napi_value result;
    if (m == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &result), NULL);
        return result;
    }
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = -1, key_size = -1, value_size = -1;
    int8_t has_offset = 0, has_timestamp = 0;
    int64_t offset = -1, timestamp = -1;
    error_warn(env, 1, FFI_SET(topic, kafka_producer_RecordMetadata_topic_err(m, &ffi_err)));
    error_warn(env, 1, FFI_SET(partition, kafka_producer_RecordMetadata_partition_err(m, &ffi_err)));
    error_warn(env, 1, FFI_SET(has_offset, kafka_producer_RecordMetadata_has_offset_err(m, &ffi_err)));
    if (has_offset) error_warn(env, 1, FFI_SET(offset, kafka_producer_RecordMetadata_offset_err(m, &ffi_err)));
    error_warn(env, 1, FFI_SET(has_timestamp, kafka_producer_RecordMetadata_has_timestamp_err(m, &ffi_err)));
    if (has_timestamp) error_warn(env, 1, FFI_SET(timestamp, kafka_producer_RecordMetadata_timestamp_err(m, &ffi_err)));
    error_warn(env, 1, FFI_SET(key_size, kafka_producer_RecordMetadata_serialized_key_size_err(m, &ffi_err)));
    error_warn(env, 1, FFI_SET(value_size, kafka_producer_RecordMetadata_serialized_value_size_err(m, &ffi_err)));

    napi_value obj, v;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, topic != NULL ? topic : "", NAPI_AUTO_LENGTH, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "topic", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, partition, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "partition", v), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, has_offset != 0, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "hasOffset", v), NULL);
    // Java's long: a bigint (root CLAUDE.md, JavaScript Binding Conventions, Types).
    NAPI_CHECK(env, napi_create_bigint_int64(env, offset, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "offset", v), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, has_timestamp != 0, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "hasTimestamp", v), NULL);
    NAPI_CHECK(env, napi_create_bigint_int64(env, timestamp, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "timestamp", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, key_size, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "serializedKeySize", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, value_size, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "serializedValueSize", v), NULL);

    AddonData* data = addon_data(env);
    if (data == NULL || data->metadata_factory == NULL) return obj;
    return call_ref(env, data->metadata_factory, 1, &obj, NULL);
}

static int is_function(napi_env env, napi_value v) {
    napi_valuetype type;
    return napi_typeof(env, v, &type) == napi_ok && type == napi_function;
}

// ---- send -------------------------------------------------------------------

// The core no longer reads the key and value (the hand-over completion, or a
// record never handed over): the references on their Buffers are dropped,
// once (delete_ref clears each).
static void send_ctx_release_buffers(napi_env env, SendCtx* ctx) {
    delete_ref(env, &ctx->key_ref);
    delete_ref(env, &ctx->value_ref);
}

// Destroys the record's future, once (javascript/CLAUDE.md, rule 5): run on
// the JS thread, it runs no JS.
static void send_ctx_destroy_future(napi_env env, SendCtx* ctx) {
    if (ctx->slot >= 0) {
        slot_give_back(ctx->p, ctx->slot);
        ctx->slot = -1;
    }
    if (ctx->future == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, !ctx->p->queue.finalizing, FFI_ERR(kafka_common_KafkaFuture_destroy_err(ctx->future, &ffi_err)));
    ctx->future = NULL;
}

// The record's future is no longer needed once its callback reported, no
// wait on it is pending, and JS reads it no more: JS recorded the delivery,
// or was never handed it. It is destroyed then, on the JS thread, with no
// finalizer of its own.
static void send_ctx_maybe_destroy_future(napi_env env, SendCtx* ctx) {
    if (ctx->delivered && ctx->waits == 0 && (ctx->js_settled || !ctx->js_holds)) send_ctx_destroy_future(env, ctx);
}

static void send_ctx_free(napi_env env, SendCtx* ctx) {
    send_ctx_release_buffers(env, ctx);
    delete_ref(env, &ctx->on_handover);
    delete_ref(env, &ctx->on_complete);
    // A future JS still holds and never recorded the delivery of (its
    // callback reached no JS: an environment teardown, or a callback Promise
    // the destroy reported as returned, which a completed close rules out)
    // stays readable through the handle JS has, so its context is kept.
    if (ctx->future != NULL && ctx->js_holds && !ctx->js_settled) return;
    send_ctx_destroy_future(env, ctx);
    free(ctx);
}

// Drops one owner; the last one frees the context unless it is an orphan,
// which destroy frees.
static void send_ctx_release(napi_env env, SendCtx* ctx) {
    if (--ctx->refs != 0 || ctx->orphaned) return;
    send_ctx_free(env, ctx);
}

// ---- the record's future -----------------------------------------------------

// A record's future (javascript/CLAUDE.md, rule 5): the owned
// `kafka_common_KafkaFuture_t` of its hand-over, kept on the send's context
// (`SendCtx.future`) and handed to JS as the number of the context's slot in
// the producer's table (`Producer.slots`): no object, and no finalizer, is
// made per record. JS waits
// on it only until it records the delivery; the context destroys it once
// the callback reported and the last wait completed
// (send_ctx_maybe_destroy_future).

// One wait on a record's future: `get_cb` or `get_with_timeout_cb`. It holds
// one reference on the send's context and one unit of the producer struct
// until its completion.
typedef struct {
    SendCtx* ctx;
    Producer* p;
    int timed;
    napi_ref done;                             // JS (metadata | null, error | null, timedOut) => void
} FutureWait;

// The completion of a wait (kafka_common_KafkaFuture_get_cb_t and
// kafka_common_KafkaFuture_get_with_timeout_cb_t): run by the producer's
// drain, inline inside the wait's call once the producer is destroyed, or by
// its destroy, on the JS thread. `value` is the record's metadata, borrowed;
// `error` is owned. A completion the destroy runs, from the handle object's
// finalizer, reaches no JS, and its JS wait never settles: nothing in the FFI
// orders it before the close's completion, and no other wait is known to
// stay pending past a completed close.
static void future_on_wait(void* value, kafka_common_Error_t* error, void* opaque) {
    FutureWait* w = (FutureWait*)opaque;
    Producer* p = w->p;
    SendCtx* ctx = w->ctx;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (!(p->queue.finalizing && p->kp != NULL)) {
        int timed_out = 0;
        if (w->timed && error != NULL) {
            int code = 0;
            error_warn(env, 1, FFI_SET(code, (int)kafka_common_Error__enum_err(error, &ffi_err)));
            // The wait timed out (Java's java.util.concurrent.TimeoutException),
            // not the record.
            timed_out = code == kafka_common_Error_LOCAL_TIMEOUT;
        }
        napi_value argv[3];
        argv[0] = metadata_to_js(env, (const kafka_producer_RecordMetadata_t*)value);
        argv[1] = argv[0] != NULL ? error_take_to_js(env, error) : NULL;
        error = NULL;
        if (argv[1] != NULL && napi_get_boolean(env, timed_out != 0, &argv[2]) == napi_ok) call_ref(env, w->done, 3, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &w->done);
    free(w);
    // The value is borrowed from the future until this returns: only now may
    // the last wait let the context destroy it.
    ctx->waits--;
    send_ctx_maybe_destroy_future(env, ctx);
    send_ctx_release(env, ctx);
    producer_release(p);
}

// recordFutureGet(handle, future, timeoutMs, done): a wait on a record's future
// through its `_cb` twins, never the blocking get (javascript/CLAUDE.md, rule
// 5): `get_cb` for a timeoutMs of null, else `get_with_timeout_cb`, a
// negative timeoutMs passed on as it is (the FFI waits no time then).
// `future` is the slot number the hand-over gave JS for the record, which JS
// calls this with, and the producer's handle, only until it records the
// delivery. `done(metadata | null, error | null,
// timedOut)` runs once it completed (future_on_wait), from the drain, or
// inside this call once the producer is destroyed.
napi_value js_record_future_get(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    int32_t slot = -1;
    if (argc != 4 || !is_function(env, argv[3]) || napi_get_value_int32(env, argv[1], &slot) != napi_ok) {
        napi_throw_type_error(env, NULL, "recordFutureGet(handle, future, timeoutMs, done)");
        return NULL;
    }
    void* data = NULL;
    if (napi_unwrap(env, argv[0], &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "not a producer handle");
        return NULL;
    }
    Producer* p = (Producer*)data;
    // Unreachable for a slot JS was given: it is given back only once JS
    // reads the future no more (send_ctx_destroy_future).
    if (slot < 0 || slot >= p->slot_count || p->slots[slot] == NULL || p->slots[slot]->future == NULL) {
        napi_throw_error(env, NULL, "no record's future in this slot");
        return NULL;
    }
    SendCtx* ctx = p->slots[slot];
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    int64_t timeout = 0;
    if (type != napi_null) NAPI_CHECK(env, napi_get_value_int64(env, argv[2], &timeout), NULL);
    FutureWait* w = (FutureWait*)calloc(1, sizeof(FutureWait));
    if (w == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    w->ctx = ctx;
    w->p = p;
    w->timed = type != napi_null;
    if (napi_create_reference(env, argv[3], 1, &w->done) != napi_ok) {
        free(w);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    ctx->refs++;
    ctx->waits++;
    p->owners++;
    if (!w->timed) kafka_common_KafkaFuture_get_cb(ctx->future, future_on_wait, w);
    else kafka_common_KafkaFuture_get_with_timeout_cb(ctx->future, timeout, future_on_wait, w);
    return NULL;
}

// The send's hand-over completion (kafka_producer_Producer_send_with_callback_cb_t):
// inside send_with_callback_cb for a record taken or refused at once, else from
// the drain, or from destroy; always on the JS thread. `value`, the record's
// future, is kept on the context, and JS gets the context's slot number to
// wait on it with (the record's future section above).
static void send_on_handover(kafka_common_KafkaFuture_t* value, kafka_common_Error_t* error, void* opaque) {
    SendCtx* ctx = (SendCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    ctx->handed_over = 1;
    ctx->handover_failed = error != NULL;
    // The FFI borrows the key and value only until this completion
    // (rust/src/ffi/producer/mod.rs, module docs): the Buffers are released
    // now, whether the record was taken or refused.
    // The context owns the record's future from now on (NULL for a refused
    // record).
    ctx->future = value;
    send_ctx_release_buffers(env, ctx);
    if (!p->queue.finalizing && ctx->on_handover != NULL) {
        napi_value argv[2];
        argv[0] = error_take_to_js(env, error);
        error = NULL;
        argv[1] = NULL;
        if (argv[0] != NULL) {
            if (value == NULL) {
                if (napi_get_null(env, &argv[1]) != napi_ok) argv[1] = NULL;
            } else {
                ctx->slot = slot_take(p, ctx);
                if (ctx->slot < 0 || napi_create_int32(env, ctx->slot, &argv[1]) != napi_ok) argv[1] = NULL;
            }
        }
        if (argv[0] != NULL && argv[1] != NULL) {
            ctx->js_holds = value != NULL;
            call_ref(env, ctx->on_handover, 2, argv, NULL);
        } else {
            js_warn_thrown(env);
        }
    }
    // A delivery that came first has already reported.
    send_ctx_maybe_destroy_future(env, ctx);
    error_discard(error);
    delete_ref(env, &ctx->on_handover);
    if (ctx->handover_failed && --ctx->refs > 0) {
        // The delivery callback may still fire, once (the record had reached
        // the accumulator): keep the context until destroy.
        ctx->orphaned = 1;
        ctx->next = p->orphans;
        p->orphans = ctx;
        // Refused once the core reported the close: no delivery callback can
        // fire for it any more, so its JS function is dropped now, as
        // close_on_complete drops the others'. Its closure holds the
        // KafkaProducer, which would otherwise never be collected, nor the
        // closed producer destroyed.
        if (cbq_is_closed(&p->queue)) delete_ref(env, &ctx->on_complete);
        return;
    }
    if (ctx->handover_failed) {
        send_ctx_free(env, ctx);
        return;
    }
    send_ctx_release(env, ctx);
}

// The owned error a callback's failure is reported with: the core logs it, as
// Java logs a throwing onCompletion. `thrown` is the text src/ returned.
static kafka_common_Error_t* callback_error(const char* thrown) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    kafka_common_Error_t* panic = FFI_SET(report, kafka_common_Error_local_callback_err(thrown, NULL, NULL, &ffi_err));
    if (report == NULL) return panic;
    error_discard(panic);
    return report;
}

// A malloc'ed copy of JS string `value`, or NULL for any other value.
static char* string_or_null(napi_env env, napi_value value) {
    napi_valuetype type;
    size_t len = 0;
    if (value == NULL || napi_typeof(env, value, &type) != napi_ok || type != napi_string
        || napi_get_value_string_utf8(env, value, NULL, 0, &len) != napi_ok)
        return NULL;
    char* out = (char*)malloc(len + 1);
    if (out != NULL && napi_get_value_string_utf8(env, value, out, len + 1, &len) != napi_ok) {
        free(out);
        out = NULL;
    }
    return out;
}

static void await_unlink(SendCtx* ctx) {
    Producer* p = ctx->p;
    if (ctx->await_prev != NULL) ctx->await_prev->await_next = ctx->await_next;
    else p->awaiting = ctx->await_next;
    if (ctx->await_next != NULL) ctx->await_next->await_prev = ctx->await_prev;
    ctx->await_prev = NULL;
    ctx->await_next = NULL;
    ctx->awaiting = 0;
}

// The one report of an awaited callback, once its Promise settled (or for
// the producer's end, below): `thrown` NULL, or the text of its rejection. A
// no-op once reported. No report once destroy has begun (the destroy
// completes the unreported callbacks itself).
static void send_awaited_report(napi_env env, SendCtx* ctx, const char* thrown) {
    if (!ctx->awaiting) return;
    Producer* p = ctx->p;
    await_unlink(ctx);
    kafka_common_Error_t* report = thrown != NULL ? callback_error(thrown) : NULL;
    kafka_common_Error_t* ffi_err = NULL;
    if (!p->destroying && p->view != NULL) {
        error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_Producer__set_callback_result_err(p->view, ctx->callback_id, NULL, report, &ffi_err)));
    } else {
        error_discard(report);
    }
    ctx->delivered = 1;
    send_ctx_maybe_destroy_future(env, ctx);
    // The function may be collected from now on; its finalizer releases.
    if (ctx->settle_ref != NULL) {
        napi_delete_reference(env, ctx->settle_ref);
        ctx->settle_ref = NULL;
    }
}

// Reports every awaited callback as returned, before the producer is
// destroyed (producer_destroy, from the handle object's finalizer): no JS
// will settle them, and destroy refuses a report once it began.
static void send_report_awaiting(napi_env env, Producer* p) {
    while (p->awaiting != NULL) send_awaited_report(env, p->awaiting, NULL);
}

// The function a callback's Promise calls once settled, as both its handlers
// (src/ resolves it with undefined, or with the text of what the callback
// threw or rejected with; it never rejects, but a rejection, whatever its
// reason, is still reported once, as a failure).
static napi_value js_send_on_settled(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1] = {NULL};
    void* data = NULL;
    if (napi_get_cb_info(env, info, &argc, argv, NULL, &data) != napi_ok || data == NULL) return NULL;
    SendCtx* ctx = (SendCtx*)data;
    // src/ recorded the delivery before its Promise called back.
    ctx->js_settled = 1;
    napi_valuetype type = napi_undefined;
    if (argc >= 1 && argv[0] != NULL && napi_typeof(env, argv[0], &type) != napi_ok) type = napi_object;
    char* thrown = type == napi_string ? string_or_null(env, argv[0]) : NULL;
    if (type != napi_undefined && thrown == NULL) {
        send_awaited_report(env, ctx, "the callback's completion failed");
    } else {
        send_awaited_report(env, ctx, thrown);
    }
    free(thrown);
    return NULL;
}

// That function's finalizer: it owned the context and one unit of the
// producer struct. It is held strongly until the report (settle_ref), so it
// runs before one only at an environment teardown, when no JS will settle the
// Promise: it is then reported as returned first, so a flush or close
// waiting for it ends. The reference itself is the runtime's to drop then.
static void send_on_settled_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    SendCtx* ctx = (SendCtx*)data;
    Producer* p = ctx->p;
    ctx->settle_ref = NULL;
    send_awaited_report(env, ctx, NULL);
    ctx->settle_fn = 0;
    send_ctx_release(env, ctx);
    producer_release(p);
}

// Defers the report of the callback whose JS side returned `promise` until it
// settles: `promise.then(onSettled, onSettled)`, the function owning the
// context (the delivery's unit of ctx->refs) and a unit of the producer
// struct until it is finalized, and held strongly until it reports, so a
// garbage collection of a Promise nothing references never reports it. Returns
// -1, nothing deferred, when `promise` is no Promise or a Node-API call failed
// (the report is then made at once).
static int send_defer(napi_env env, SendCtx* ctx, napi_value promise, int64_t callback_id) {
    bool is_promise = false;
    napi_value then, fn, ignored;
    if (napi_is_promise(env, promise, &is_promise) != napi_ok || !is_promise
        || napi_get_named_property(env, promise, "then", &then) != napi_ok
        || napi_create_function(env, "onSettled", NAPI_AUTO_LENGTH, js_send_on_settled, ctx, &fn) != napi_ok) {
        js_warn_thrown(env);
        return -1;
    }
    napi_ref settle_ref = NULL;
    if (napi_create_reference(env, fn, 1, &settle_ref) != napi_ok) {
        js_warn_thrown(env);
        return -1;
    }
    if (napi_add_finalizer(env, fn, ctx, send_on_settled_finalize, NULL, NULL) != napi_ok) {
        napi_delete_reference(env, settle_ref);
        js_warn_thrown(env);
        return -1;
    }
    ctx->settle_ref = settle_ref;
    Producer* p = ctx->p;
    ctx->callback_id = callback_id;
    ctx->settle_fn = 1;
    p->owners++;
    ctx->awaiting = 1;
    ctx->await_prev = NULL;
    ctx->await_next = p->awaiting;
    if (p->awaiting != NULL) p->awaiting->await_prev = ctx;
    p->awaiting = ctx;
    napi_value handlers[2] = {fn, fn};
    if (napi_call_function(env, promise, then, 2, handlers, &ignored) != napi_ok) {
        // Never called back: report now; the finalizer still releases.
        js_warn_thrown(env);
        send_awaited_report(env, ctx, NULL);
    }
    return 0;
}

// Java's Callback.onCompletion (kafka_producer_Callback_on_completion_fn_t),
// run by the drain or by destroy on the JS thread. Both arguments are
// BORROWED. Order (javascript/CLAUDE.md, rule 6): the JS side runs the user
// callback and then completes the KafkaFuture; then the result is reported, once.
//
// What runs inside the drain here (D8): the user's Java-style Callback, the
// one piece of user code called synchronously, and the settling of the
// record's Promises (KafkaFuture.get()'s), whose reactions run as microtasks
// once the drain has returned to JS, never inside it.
//
// The JS side returns undefined (reported now, as returned), a string (what
// the callback threw: reported now with it) or a Promise (the callback
// returned one: the future completes and the report is made once it settled,
// send_defer).
//
// `callback_id` (D13) stays in C: it is an int64_t the core may number past
// 2^53, which a JS number cannot hold exactly, so it is never handed to JS. It
// is used only here, in the one __set_callback_result report, or kept on the
// context for an awaited callback's.
static void send_on_completion(void* self_, const kafka_producer_RecordMetadata_t* metadata,
                               const kafka_common_Error_t* error, int64_t callback_id) {
    SendCtx* ctx = (SendCtx*)self_;
    Producer* p = ctx->p;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    int deferred = 0;
    if (!p->queue.finalizing && ctx->on_complete != NULL) {
        napi_value argv[2];
        argv[0] = metadata_to_js(env, metadata);
        argv[1] = argv[0] != NULL ? error_to_js(env, error) : NULL;
        char* thrown = NULL;
        if (argv[0] != NULL && argv[1] != NULL) {
            napi_value r = call_ref(env, ctx->on_complete, 2, argv, &thrown);
            napi_valuetype type = napi_undefined;
            if (r != NULL && napi_typeof(env, r, &type) != napi_ok) type = napi_undefined;
            // JS recorded the delivery unless its callback returned a Promise
            // (send_defer), which records it once settled (js_send_on_settled).
            if (r != NULL && type != napi_object) ctx->js_settled = 1;
            if (type == napi_string) {
                // The user callback threw: src/ returns its message.
                free(thrown);
                thrown = string_or_null(env, r);
            } else if (type == napi_object && send_defer(env, ctx, r, callback_id) == 0) {
                deferred = 1;
            }
        } else {
            js_warn_thrown(env);
        }
        if (thrown != NULL) {
            report = callback_error(thrown);
            free(thrown);
        }
    }
    delete_ref(env, &ctx->on_complete);
    // An awaited callback reports once its Promise settled (send_on_settled);
    // its function owns the context from now on.
    if (deferred) return;
    if (!p->destroying) {
        error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_Producer__set_callback_result_err(p->view, callback_id, NULL, report, &ffi_err)));
    } else {
        // Destroy completes the unreported callbacks itself.
        error_discard(report);
    }
    ctx->delivered = 1;
    send_ctx_maybe_destroy_future(env, ctx);
    send_ctx_release(env, ctx);
}

static Producer* unwrap_producer(napi_env env, napi_value handle) {
    void* data = NULL;
    if (napi_unwrap(env, handle, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "not a producer handle");
        return NULL;
    }
    Producer* p = (Producer*)data;
    // Unreachable from JS: destroy runs only from the handle object's
    // finalizer, once nothing can call through it (a failed creation never
    // hands the handle out). A guard of memory, not of the closed state.
    if (p->kp == NULL) {
        napi_throw_error(env, NULL, "the producer has been destroyed");
        return NULL;
    }
    return p;
}

// producerSend(handle, topic, partition, timestamp, key, value, headers,
// onHandover, onComplete): partition -1 and timestamp -1n (a bigint) for
// Java's null; key and
// value a Uint8Array or null, the app's (a serializer's included) or the
// client's own (an encoded string), never copied: each is kept by a reference
// until the hand-over (javascript/CLAUDE.md, rule 8); headers null or an
// array of [key, Uint8Array | null]. Throws the core's error when the record
// cannot be built (nothing is sent then).
napi_value js_producer_send(napi_env env, napi_callback_info info) {
    size_t argc = 9;
    napi_value argv[9];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 9 || !is_function(env, argv[7]) || !is_function(env, argv[8])) {
        napi_throw_type_error(
            env, NULL, "send(handle, topic, partition, timestamp, key, value, headers, onHandover, onComplete)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    int32_t partition = -1;
    int64_t timestamp = -1;
    bool lossless = false;
    NAPI_CHECK(env, napi_get_value_int32(env, argv[2], &partition), NULL);
    // A bigint src/ checked against Java's long range.
    NAPI_CHECK(env, napi_get_value_bigint_int64(env, argv[3], &timestamp, &lossless), NULL);
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
    ctx->slot = -1;
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
        send_ctx_free(env, ctx);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    // The core reads the key's and value's own memory until the hand-over:
    // a reference keeps each Buffer, and so its memory, alive until then.
    if ((!ctx->key.is_null && napi_create_reference(env, argv[4], 1, &ctx->key_ref) != napi_ok)
        || (!ctx->value.is_null && napi_create_reference(env, argv[5], 1, &ctx->value_ref) != napi_ok)
        || napi_create_reference(env, argv[7], 1, &ctx->on_handover) != napi_ok
        || napi_create_reference(env, argv[8], 1, &ctx->on_complete) != napi_ok) {
        error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecord_destroy_err(record, &ffi_err)));
        send_ctx_free(env, ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    kafka_producer_Callback_t* callback = NULL;
    kafka_common_Error_t* err = FFI_SET(callback, kafka_producer_Callback_new_err(ctx, send_on_completion, &ffi_err));
    if (err != NULL) {
        // Never registered: neither completion can fire.
        error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecord_destroy_err(record, &ffi_err)));
        send_ctx_free(env, ctx);
        error_throw_owned(env, err);
        return NULL;
    }
    // Everything the completions read is in place: the hand-over completion can
    // fire inside this call.
    kafka_producer_Producer_send_with_callback_cb(p->view, record, callback, send_on_handover, ctx);
    // The call consumed the callback and copied the record; the key and value
    // stay borrowed until the hand-over completion (send_on_handover).
    error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecord_destroy_err(record, &ffi_err)));
    return NULL;
}

// ---- flush ------------------------------------------------------------------

static void flush_on_complete(kafka_common_Error_t* error, void* opaque) {
    FlushCtx* ctx = (FlushCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    if (!p->queue.finalizing) {
        napi_value arg = error_take_to_js(env, error);
        error = NULL;
        if (arg != NULL) call_ref(env, ctx->done, 1, &arg, NULL);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
}

// producerFlush(handle, done): Producer.flush() through flush_cb;
// `done(error | null)` runs once every record sent before it completed and its
// callback reported.
napi_value js_producer_flush(napi_env env, napi_callback_info info) {
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
    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);
    return NULL;
}

// ---- partitionsFor ----------------------------------------------------------

// A node's fields as src/common/node.ts's constructor takes them: { id, host,
// port, rack, isFenced }, or null for a NULL node.
static napi_value node_to_js(napi_env env, const kafka_common_Node_t* n) {
    napi_value obj, v;
    if (n == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &obj), NULL);
        return obj;
    }
    kafka_common_Error_t* ffi_err = NULL;
    int32_t id = -1, port = -1;
    const char* host = NULL;
    const char* rack = NULL;
    int8_t has_rack = 0, fenced = 0;
    error_warn(env, 1, FFI_SET(id, kafka_common_Node_id_err(n, &ffi_err)));
    error_warn(env, 1, FFI_SET(host, kafka_common_Node_host_err(n, &ffi_err)));
    error_warn(env, 1, FFI_SET(port, kafka_common_Node_port_err(n, &ffi_err)));
    error_warn(env, 1, FFI_SET(has_rack, kafka_common_Node_has_rack_err(n, &ffi_err)));
    if (has_rack) error_warn(env, 1, FFI_SET(rack, kafka_common_Node_rack_err(n, &ffi_err)));
    error_warn(env, 1, FFI_SET(fenced, kafka_common_Node_is_fenced_err(n, &ffi_err)));
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_int32(env, id, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "id", v), NULL);
    if (host != NULL) NAPI_CHECK(env, napi_create_string_utf8(env, host, NAPI_AUTO_LENGTH, &v), NULL);
    else NAPI_CHECK(env, napi_get_null(env, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "host", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, port, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "port", v), NULL);
    if (rack != NULL) NAPI_CHECK(env, napi_create_string_utf8(env, rack, NAPI_AUTO_LENGTH, &v), NULL);
    else NAPI_CHECK(env, napi_get_null(env, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "rack", v), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, fenced != 0, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "isFenced", v), NULL);
    return obj;
}

// An owned kafka_List_t of owned kafka_common_Node_t * as an array of node
// fields, the list freed; a NULL list is null.
static napi_value nodes_take_to_js(napi_env env, kafka_List_t* list) {
    napi_value arr;
    if (list == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &arr), NULL);
        return arr;
    }
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    error_warn(env, 1, FFI_SET(size, kafka_List_size_err(list, &ffi_err)));
    napi_value result = NULL;
    if (napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok) {
        result = arr;
        for (int32_t i = 0; i < size && result != NULL; i++) {
            const kafka_common_Node_t* n = NULL;
            error_warn(env, 1, FFI_SET(n, (const kafka_common_Node_t*)kafka_List_get_err(list, i, &ffi_err)));
            napi_value v = node_to_js(env, n);
            if (v == NULL || napi_set_element(env, arr, (uint32_t)i, v) != napi_ok) result = NULL;
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    return result;
}

// A partition's fields as src/common/partition_info.ts's constructor takes
// them: { topic, partition, leader, replicas, inSyncReplicas,
// offlineReplicas }, each node as node_to_js gives it.
static napi_value partition_info_to_js(napi_env env, const kafka_common_PartitionInfo_t* info) {
    kafka_common_Error_t* ffi_err = NULL;
    const char* topic = NULL;
    int32_t partition = -1;
    const kafka_common_Node_t* leader = NULL;
    kafka_List_t* replicas = NULL;
    kafka_List_t* in_sync = NULL;
    kafka_List_t* offline = NULL;
    error_warn(env, 1, FFI_SET(topic, kafka_common_PartitionInfo_topic_err(info, &ffi_err)));
    error_warn(env, 1, FFI_SET(partition, kafka_common_PartitionInfo_partition_err(info, &ffi_err)));
    error_warn(env, 1, FFI_SET(leader, kafka_common_PartitionInfo_leader_err(info, &ffi_err)));
    error_warn(env, 1, FFI_SET(replicas, kafka_common_PartitionInfo_replicas_err(info, &ffi_err)));
    error_warn(env, 1, FFI_SET(in_sync, kafka_common_PartitionInfo_in_sync_replicas_err(info, &ffi_err)));
    error_warn(env, 1, FFI_SET(offline, kafka_common_PartitionInfo_offline_replicas_err(info, &ffi_err)));
    // Each owned list is freed, whatever happens to the others.
    napi_value r = nodes_take_to_js(env, replicas);
    napi_value s = nodes_take_to_js(env, in_sync);
    napi_value o = nodes_take_to_js(env, offline);
    napi_value l = node_to_js(env, leader);
    if (r == NULL || s == NULL || o == NULL || l == NULL) return NULL;
    napi_value obj, v;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, topic != NULL ? topic : "", NAPI_AUTO_LENGTH, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "topic", v), NULL);
    NAPI_CHECK(env, napi_create_int32(env, partition, &v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "partition", v), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "leader", l), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "replicas", r), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "inSyncReplicas", s), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "offlineReplicas", o), NULL);
    return obj;
}

// The owned list of owned kafka_common_PartitionInfo_t * partitions_for_cb
// delivers, as an array of partition fields; the list (and its elements) freed.
static napi_value partitions_take_to_js(napi_env env, kafka_List_t* list) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    error_warn(env, 1, FFI_SET(size, kafka_List_size_err(list, &ffi_err)));
    napi_value arr;
    napi_value result = NULL;
    if (napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok) {
        result = arr;
        for (int32_t i = 0; i < size && result != NULL; i++) {
            const kafka_common_PartitionInfo_t* info = NULL;
            error_warn(env, 1, FFI_SET(info, (const kafka_common_PartitionInfo_t*)kafka_List_get_err(list, i, &ffi_err)));
            napi_value v = info == NULL ? NULL : partition_info_to_js(env, info);
            if (v == NULL || napi_set_element(env, arr, (uint32_t)i, v) != napi_ok) result = NULL;
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    return result;
}

// The partitions_for_cb completion, on the JS thread: `done(error, null)` or
// `done(null, partitions)`.
static void partitions_for_on_complete(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    PartitionsForCtx* ctx = (PartitionsForCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    if (!p->queue.finalizing) {
        napi_value argv[2];
        if (error != NULL) {
            argv[0] = error_take_to_js(env, error);
            error = NULL;
            if (argv[0] != NULL && napi_get_null(env, &argv[1]) == napi_ok) call_ref(env, ctx->done, 2, argv, NULL);
            if (value != NULL) {
                kafka_common_Error_t* ffi_err = NULL;
                error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(value, &ffi_err)));
            }
        } else {
            argv[1] = value == NULL ? NULL : partitions_take_to_js(env, value);
            value = NULL;
            if (argv[1] == NULL) js_warn_thrown(env);
            else if (napi_get_null(env, &argv[0]) == napi_ok) call_ref(env, ctx->done, 2, argv, NULL);
        }
    } else if (value != NULL) {
        kafka_common_Error_t* ffi_err = NULL;
        error_discard(FFI_ERR(kafka_List_destroy_err(value, &ffi_err)));
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
}

// producerPartitionsFor(handle, topic, done): Producer.partitionsFor(topic)
// through partitions_for_cb; `done(error | null, partitions | null)` runs
// with the partitions' fields once the core answered.
napi_value js_producer_partitions_for(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "partitionsFor(handle, topic, done)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    // Java's null is NULL, which the core refuses with Java's
    // NullPointerException ("topic cannot be null").
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    char* topic = NULL;
    if (type != napi_null) {
        topic = js_string_dup(env, argv[1], "topic must be a string");
        if (topic == NULL) return NULL;
    }
    PartitionsForCtx* ctx = (PartitionsForCtx*)calloc(1, sizeof(PartitionsForCtx));
    if (ctx == NULL) {
        free(topic);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    ctx->p = p;
    if (napi_create_reference(env, argv[2], 1, &ctx->done) != napi_ok) {
        free(topic);
        free(ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    kafka_producer_Producer_partitions_for_cb(p->view, topic, partitions_for_on_complete, ctx);
    free(topic);
    return NULL;
}

// ---- metrics ----------------------------------------------------------------

// A metric value as JS: a double or an int a number, a long a bigint (Java's
// long, root CLAUDE.md, JavaScript Binding Conventions, Types), a string a
// string; the value freed.
static napi_value metric_value_take_to_js(napi_env env, kafka_common_MetricValue_t* value) {
    kafka_common_Error_t* ffi_err = NULL;
    napi_value v = NULL;
    if (value == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &v), NULL);
        return v;
    }
    int kind = kafka_common_MetricValue_DOUBLE;
    error_warn(env, 1, FFI_SET(kind, (int)kafka_common_MetricValue__enum_err(value, &ffi_err)));
    napi_status status = napi_ok;
    if (kind == kafka_common_MetricValue_STRING) {
        const char* text = NULL;
        error_warn(env, 1, FFI_SET(text, kafka_common_MetricValue_as_string_err(value, &ffi_err)));
        status = napi_create_string_utf8(env, text != NULL ? text : "", NAPI_AUTO_LENGTH, &v);
    } else if (kind == kafka_common_MetricValue_LONG) {
        int64_t n = 0;
        error_warn(env, 1, FFI_SET(n, kafka_common_MetricValue_as_long_err(value, &ffi_err)));
        status = napi_create_bigint_int64(env, n, &v);
    } else if (kind == kafka_common_MetricValue_INT) {
        int32_t n = 0;
        error_warn(env, 1, FFI_SET(n, kafka_common_MetricValue_as_int_err(value, &ffi_err)));
        status = napi_create_int32(env, n, &v);
    } else {
        double d = 0;
        error_warn(env, 1, FFI_SET(d, kafka_common_MetricValue_as_double_err(value, &ffi_err)));
        status = napi_create_double(env, d, &v);
    }
    error_warn(env, 1, FFI_ERR(kafka_common_MetricValue_destroy_err(value, &ffi_err)));
    return status == napi_ok ? v : NULL;
}

// A metric name's tags, the map of char * to char * sorted by key, as an array
// of [key, value] pairs in that order; the map freed.
static napi_value tags_take_to_js(napi_env env, kafka_Map_t* tags) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t size = 0;
    if (tags != NULL) error_warn(env, 1, FFI_SET(size, kafka_Map_size_err(tags, &ffi_err)));
    napi_value arr;
    napi_value result = NULL;
    if (napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok) {
        result = arr;
        for (int32_t i = 0; i < size && result != NULL; i++) {
            const char* key = NULL;
            const char* value = NULL;
            error_warn(env, 1, FFI_SET(key, (const char*)kafka_Map_key_err(tags, i, &ffi_err)));
            error_warn(env, 1, FFI_SET(value, (const char*)kafka_Map_value_err(tags, i, &ffi_err)));
            napi_value pair, k, v;
            if (napi_create_array_with_length(env, 2, &pair) != napi_ok
                || napi_create_string_utf8(env, key != NULL ? key : "", NAPI_AUTO_LENGTH, &k) != napi_ok
                || napi_create_string_utf8(env, value != NULL ? value : "", NAPI_AUTO_LENGTH, &v) != napi_ok
                || napi_set_element(env, pair, 0, k) != napi_ok || napi_set_element(env, pair, 1, v) != napi_ok
                || napi_set_element(env, arr, (uint32_t)i, pair) != napi_ok)
                result = NULL;
        }
    }
    if (tags != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(tags, &ffi_err)));
    return result;
}

// One metric as JS: { name, group, description, tags, value, measurable },
// read when metrics() runs.
static napi_value metric_to_js(napi_env env, const kafka_common_MetricName_t* name,
                               const kafka_common_metrics_KafkaMetric_t* metric) {
    kafka_common_Error_t* ffi_err = NULL;
    const char* n = NULL;
    const char* g = NULL;
    const char* d = NULL;
    kafka_Map_t* tags = NULL;
    const kafka_common_Metric_t* as_metric = NULL;
    kafka_common_MetricValue_t* value = NULL;
    int8_t measurable = 0;
    error_warn(env, 1, FFI_SET(n, kafka_common_MetricName_name_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(g, kafka_common_MetricName_group_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(d, kafka_common_MetricName_description_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(tags, kafka_common_MetricName_tags_err(name, &ffi_err)));
    error_warn(env, 1, FFI_SET(as_metric, kafka_common_metrics_KafkaMetric__as_Metric_err(metric, &ffi_err)));
    if (as_metric != NULL) error_warn(env, 1, FFI_SET(value, kafka_common_Metric_metric_value_err(as_metric, &ffi_err)));
    error_warn(env, 1, FFI_SET(measurable, kafka_common_metrics_KafkaMetric_is_measurable_err(metric, &ffi_err)));
    // Each owned value is freed, whatever happens to the others.
    napi_value t = tags_take_to_js(env, tags);
    napi_value v = metric_value_take_to_js(env, value);
    if (t == NULL || v == NULL) return NULL;
    napi_value obj, x;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, n != NULL ? n : "", NAPI_AUTO_LENGTH, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "name", x), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, g != NULL ? g : "", NAPI_AUTO_LENGTH, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "group", x), NULL);
    NAPI_CHECK(env, napi_create_string_utf8(env, d != NULL ? d : "", NAPI_AUTO_LENGTH, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "description", x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "tags", t), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "value", v), NULL);
    NAPI_CHECK(env, napi_get_boolean(env, measurable != 0, &x), NULL);
    NAPI_CHECK(env, napi_set_named_property(env, obj, "measurable", x), NULL);
    return obj;
}

// producerMetrics(handle): Producer.metrics() through kafka_producer_Producer_metrics,
// synchronously (it has no _cb twin): every metric's fields and value, read
// now, as an array (src/producer/kafka_producer.ts builds the snapshot).
napi_value js_producer_metrics(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "metrics(handle)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_metrics_MetricsView_t* view = NULL;
    kafka_common_Error_t* err = FFI_SET(view, kafka_producer_Producer_metrics_err(p->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    kafka_Iterator_t* it = NULL;
    err = FFI_SET(it, kafka_common_metrics_MetricsView_iterator_err(view, &ffi_err));
    error_warn(env, 1, FFI_ERR(kafka_common_metrics_MetricsView_destroy_err(view, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value arr;
    napi_value result = NULL;
    if (napi_create_array(env, &arr) == napi_ok) {
        result = arr;
        uint32_t i = 0;
        for (;;) {
            int8_t more = 0;
            error_warn(env, 1, FFI_SET(more, kafka_Iterator_has_next_err(it, &ffi_err)));
            if (!more) break;
            const kafka_Map_Entry_t* entry = NULL;
            err = FFI_SET(entry, (const kafka_Map_Entry_t*)kafka_Iterator_next_err(it, &ffi_err));
            if (err != NULL || entry == NULL) {
                error_warn(env, 1, err);
                break;
            }
            const kafka_common_MetricName_t* name = NULL;
            const kafka_common_metrics_KafkaMetric_t* metric = NULL;
            error_warn(env, 1, FFI_SET(name, (const kafka_common_MetricName_t*)kafka_Map_Entry_key_err(entry, &ffi_err)));
            error_warn(env, 1, FFI_SET(metric, (const kafka_common_metrics_KafkaMetric_t*)kafka_Map_Entry_value_err(entry, &ffi_err)));
            if (name == NULL || metric == NULL) continue;
            napi_value v = metric_to_js(env, name, metric);
            if (v == NULL || napi_set_element(env, arr, i++, v) != napi_ok) {
                result = NULL;
                break;
            }
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_Iterator_destroy_err(it, &ffi_err)));
    return result;
}

// ---- close and destroy ------------------------------------------------------

static void free_orphans(napi_env env, Producer* p) {
    while (p->orphans != NULL) {
        SendCtx* ctx = p->orphans;
        p->orphans = ctx->next;
        // An awaited callback's function still owns it: freed by its finalizer.
        if (ctx->settle_fn) ctx->orphaned = 0;
        else send_ctx_free(env, ctx);
    }
}

static void noop_notify(void* opaque) {
    (void)opaque;
}

// Destroys the class handle of a closed producer, on the JS thread, from the
// handle object's finalizer, outside any drain: the core runs the
// still-pending callbacks here, inline, once each, running no JS. Then
// releases the tsfn.
static void producer_destroy(napi_env env, Producer* p) {
    kafka_common_Error_t* ffi_err = NULL;
    p->queue.stopped = 1;
    // A callback's Promise still pending is reported as returned before
    // destroy begins, after which no report may be made: no JS will settle it.
    send_report_awaiting(env, p);
    p->destroying = 1;
    error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(p->kp, &ffi_err)));
    p->kp = NULL;
    p->view = NULL;
    // No callback can fire past this point.
    free_orphans(env, p);
    cbq_release(env, &p->queue);
}

// The close completion (close_cb / close_with_timeout_cb), on the JS thread.
// A timeout the core rejected (Java's IllegalArgumentException) closed
// nothing: the producer stays open and keeps Node running. Any other outcome
// closed it: Node may exit and the handle object be collected (cbq_closed).
// `done(error | null, closedNow)`, closedNow true for the close that closed
// the producer, false for a rejected one or a later close.
static void close_on_complete(kafka_common_Error_t* error, void* opaque) {
    CloseCtx* ctx = (CloseCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    int rejected = 0;
    if (error != NULL) {
        int code = 0;
        error_warn(env, !p->queue.finalizing, FFI_SET(code, (int)kafka_common_Error__enum_err(error, &ffi_err)));
        rejected = code == kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT;
    }
    int closed_now = rejected || p->queue.finalizing ? 0 : cbq_closed(env, &p->queue);
    // The close completed every record the accumulator held, so no orphan's
    // delivery callback can fire any more: their JS functions, whose closures
    // hold the KafkaProducer, are dropped, so the closed producer can be
    // collected and destroyed. Each context stays on the list until destroy.
    if (closed_now)
        for (SendCtx* o = p->orphans; o != NULL; o = o->next) delete_ref(env, &o->on_complete);
    if (!p->queue.finalizing) {
        napi_value argv[2];
        argv[0] = error_take_to_js(env, error);
        error = NULL;
        if (argv[0] != NULL && napi_get_boolean(env, closed_now != 0, &argv[1]) == napi_ok)
            call_ref(env, ctx->done, 2, argv, NULL);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
}

// producerClose(handle, timeoutMs, done): Producer.close() for a timeoutMs of
// null (the form without a timeout), else close(Duration.ofMillis(timeoutMs)),
// through the `_cb` twins: the form called picks the call, never the value. A
// negative timeoutMs goes to the core as it is, which rejects it with Java's
// IllegalArgumentException. timeoutMs is a number, or a bigint for a value a
// number does not hold exactly (Long.MAX_VALUE, the no-limit timeout of
// src/internal/duration.ts). `done(error | null, closedNow)` runs once the
// close completed (close_on_complete). A close of a producer already closed
// goes to the core too.
napi_value js_producer_close(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "close(handle, timeoutMs, done)");
        return NULL;
    }
    Producer* p = unwrap_producer(env, argv[0]);
    if (p == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    int timed = type != napi_null;
    int64_t timeout = 0;
    if (!timed) {
        // Java's close(): no timeout to read.
    } else if (type == napi_bigint) {
        bool lossless = false;
        NAPI_CHECK(env, napi_get_value_bigint_int64(env, argv[1], &timeout, &lossless), NULL);
        if (!lossless) {
            napi_throw_range_error(env, NULL, "close timeoutMs must fit a signed 64-bit integer");
            return NULL;
        }
    } else {
        NAPI_CHECK(env, napi_get_value_int64(env, argv[1], &timeout), NULL);
    }
    CloseCtx* ctx = (CloseCtx*)calloc(1, sizeof(CloseCtx));
    if (ctx == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    ctx->p = p;
    if (napi_create_reference(env, argv[2], 1, &ctx->done) != napi_ok) {
        free(ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    if (!timed) kafka_producer_Producer_close_cb(p->view, close_on_complete, ctx);
    else kafka_producer_Producer_close_with_timeout_cb(p->view, timeout, close_on_complete, ctx);
    return NULL;
}

// ---- the pump ---------------------------------------------------------------

// The queue's execute: the producer's __execute_callbacks.
static int32_t producer_execute(void* client, kafka_common_Error_t** out_err) {
    return kafka_producer_Producer__execute_callbacks_err(((Producer*)client)->view, out_err);
}

// producerPump(handle): the queue's drain, run by send() on entry every so
// many sends the producer took at once in a row (javascript/CLAUDE.md, rule
// 3): such sends never return to the event loop, so the tsfn's call_js could
// not run between them. A no-op inside a drain.
napi_value js_producer_pump(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    void* data = NULL;
    if (argc != 1 || napi_unwrap(env, argv[0], &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "pump(handle)");
        return NULL;
    }
    Producer* p = (Producer*)data;
    if (p->kp != NULL) cbq_drain(env, &p->queue);
    return NULL;
}

// ---- finalization -----------------------------------------------------------

// The queue's on_tsfn_finalize: the tsfn's ownership of the struct ends.
static void producer_tsfn_finalized(napi_env env, void* client) {
    (void)env;
    Producer* p = (Producer*)client;
    if (p->kp != NULL && !p->destroying) {
        // Environment teardown finalized the tsfn before the handle object: a
        // later hook call need not even take the lock.
        kafka_common_Error_t* ffi_err = NULL;
        FFI_DISCARD(kafka_producer_Producer__set_callbacks_notify_err(p->view, noop_notify, NULL, &ffi_err));
    }
    producer_release(p);
}

// The handle object's finalizer: the garbage collection of a closed
// producer, or the environment's teardown (a worker ending, the process
// exiting its event loop). A closed producer is destroyed here, running no
// JS. An open one is never closed or destroyed by a finalizer
// (javascript/CLAUDE.md, rule 10): only a teardown finalizes one, since the
// handle object is held strongly until the close, and it is left to the core,
// with this struct, which the core's callbacks may still reach.
static void producer_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    Producer* p = (Producer*)data;
    p->queue.finalizing = 1;
    int open = p->kp != NULL && !cbq_is_closed(&p->queue);
    if (p->kp != NULL && cbq_is_closed(&p->queue)) producer_destroy(env, p);
    cbq_detach(env, &p->queue);
    if (!open) producer_release(p);
}

// ---- construction -----------------------------------------------------------

// The class handle KafkaProducer(configs) with NULL serializers (key and value
// cross as kafka_Bytes_t), from `entries`, an array of [key, value] pairs, the
// bytes of each string (encodeLikeJava's).
// Returns NULL with a JS exception pending on failure.
static kafka_producer_KafkaProducer_t* new_kafka_producer(napi_env env, napi_value entries) {
    uint32_t count = 0;
    NAPI_CHECK(env, napi_get_array_length(env, entries, &count), NULL);
    char** strings = (char**)calloc((size_t)count * 2 + 1, sizeof(char*));
    if (strings == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* props = NULL;
    kafka_producer_ProducerConfig_t* config = NULL;
    kafka_producer_KafkaProducer_t* kp = NULL;
    int thrown = 0;
    // A C-built kafka_Map_t borrows its char * keys and values: they are freed
    // after the map.
    kafka_common_Error_t* err = FFI_SET(props, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && !thrown && i < count; i++) {
        napi_value entry, k, v;
        if (napi_get_element(env, entries, i, &entry) != napi_ok || napi_get_element(env, entry, 0, &k) != napi_ok
            || napi_get_element(env, entry, 1, &v) != napi_ok) {
            js_throw_last_napi_error(env, "reading a config entry");
            thrown = 1;
            break;
        }
        // Bytes encoded by encodeLikeJava (src/internal/config.ts).
        strings[2 * i] = js_bytes_cstring(env, k, "config keys must be strings");
        strings[2 * i + 1] = strings[2 * i] != NULL ? js_bytes_cstring(env, v, "config values must be strings") : NULL;
        if (strings[2 * i + 1] == NULL) {
            thrown = 1;
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err));
    }
    // kafka_producer_ProducerConfig_new and _KafkaProducer_new take an out_err
    // of their own (no `_err` twin).
    if (err == NULL && !thrown) err = FFI_SET(config, kafka_producer_ProducerConfig_new(props, &ffi_err));
    if (props != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(props, &ffi_err)));
    for (uint32_t i = 0; i < count * 2; i++) free(strings[i]);
    free(strings);
    if (err == NULL && !thrown) err = FFI_SET(kp, kafka_producer_KafkaProducer_new(config, NULL, NULL, &ffi_err));
    if (config != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_ProducerConfig_destroy_err(config, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return thrown ? NULL : kp;
}

// producerCreate(entries): a native producer handle over
// KafkaProducer(configs), `entries` an array of [key, value] byte pairs. Throws a
// KafkaError when the configuration is rejected or the producer cannot be
// built.
napi_value js_producer_create(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool is_array = false;
    if (argc != 1 || napi_is_array(env, argv[0], &is_array) != napi_ok || !is_array) {
        napi_throw_type_error(env, NULL, "create(entries)");
        return NULL;
    }
    kafka_producer_KafkaProducer_t* kp = new_kafka_producer(env, argv[0]);
    if (kp == NULL) return NULL;

    kafka_common_Error_t* ffi_err = NULL;
    Producer* p = (Producer*)calloc(1, sizeof(Producer));
    if (p == NULL) {
        error_warn(env, 1, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err)));
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    p->env = env;
    p->kp = kp;
    p->free_head = -1;
    kafka_common_Error_t* err = FFI_SET(p->view, kafka_producer_KafkaProducer__as_Producer_err(kp, &ffi_err));
    if (err == NULL
        && cbq_init(env, &p->queue, "confluent-kafka producer callbacks", p, producer_execute, producer_tsfn_finalized)
               != 0) {
        error_warn(env, 1, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err)));
        free(p);
        return NULL;
    }
    if (err != NULL) {
        error_warn(env, 1, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err)));
        free(p);
        error_throw_owned(env, err);
        return NULL;
    }
    p->owners = 1;  // the tsfn
    napi_value result = NULL;
    int thrown = 0;
    if (napi_create_object(env, &result) != napi_ok || napi_wrap(env, result, p, producer_finalize, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "napi_wrap");
        thrown = 1;
    } else {
        p->owners = 2;  // and the handle object
        if (cbq_attach(env, &p->queue, result) != 0) thrown = 1;
    }
    if (!thrown)
        err = FFI_ERR(kafka_producer_Producer__set_callbacks_notify_err(p->view, cbq_notify, &p->queue, &ffi_err));
    if (err != NULL || thrown) {
        error_warn(env, 1, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err)));
        p->kp = NULL;
        p->destroying = 1;
        // Releases the tsfn, whose finalizer frees p when it is the only owner.
        cbq_release(env, &p->queue);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return result;
}

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
// flush_cb, partitions_for_cb, close_cb / close_with_timeout_cb), whose
// completion, like every delivery callback, the core QUEUES on the producer's
// callback vector; the producer's CallbackQueue (callback_queue.c) drains it
// on the JS thread, from the event loop or from send()'s `pump`. So every
// completion runs on the JS thread and builds its JS values there. A send's
// hand-over completion may also fire inside send_with_callback_cb itself, on
// the JS thread (the record taken or refused at once), so a send's whole
// state is in place before that call.
//
// Each request has its own context (SendCtx, FlushCtx, CloseCtx, FutureWait,
// PartitionsForCtx), whose pointer is what the core receives as the
// completion's opaque. A send that fails at hand-over parks its context on
// the orphan list, freed only after destroy. A completion whose answer can't
// be built still calls its JS function, with the failure (js_take_failure), so
// its call settles, and so does a delivery callback (send_on_completion).
//
// Every `_cb` call is counted on the producer's queue (javascript/CLAUDE.md,
// "Threads and liveness"): `cbq_call_begin` as the statement right before the
// call, after every early return of the function making it, and
// `cbq_call_end` as the last statement of its completion (send_on_handover,
// flush_on_complete, partitions_for_on_complete, close_on_complete,
// future_on_wait), on every path: the drain, inline inside the call, and the
// destroy; nowhere else (not in the delivery callback). So a call made on a
// closed producer holds Node until its answer ran, as Java's call returns,
// and so does a second close made while the first runs, whose answer comes
// after the first's. A send's hand-over and a wait on a record's future are
// counted the same way, so one rule covers every call whatever order the core
// answers in, although every answer of theirs seen so far came before the
// close's completion. A MockProducer makes the same calls, through the same
// view and the same sites, so its calls are counted the same way: its own
// calls (history, clear, completeNext, errorNext, the error setters) are
// `_err` calls, answered inside the call, with no completion to count.
//
// A delivery callback that returns a Promise is awaited: its report through
// `__set_callback_result` waits until the Promise settled (send_on_settled),
// so the record's future, `flush` and `close` wait for it as for a callback
// that returned. The callback id stays in C, on the context, meanwhile; the
// context and the producer struct stay allocated until the function the
// Promise calls back is finalized. Callbacks run one at a time, in completion
// order, as Java's run one after another on its I/O thread: until an awaited
// Promise settled, the callbacks of the records that completed after it wait
// for their turn in the run queue (send_on_completion, send_run_queued).
//
// Closed state and teardown (javascript/CLAUDE.md, rules 1 and 10): the
// binding keeps no closed flag and refuses no call; a call after close() goes
// to the FFI, whose answer is the core's. close() does not free the native
// handle: its completion only lets Node exit once no call made on the
// producer waits for its answer, and lets the handle object be collected
// (cbq_closed). The handle object's finalizer then destroys the
// closed producer (outside any drain, running no JS), and releases the
// threadsafe function, whose finalizer frees this struct together with the
// handle object's finalizer (two owners). `KafkaProducer_destroy_err` runs the
// callbacks still pending, inline, exactly once each. No finalizer closes or
// destroys a producer still open: one finalized open (only an environment
// teardown can, as the handle object is held strongly until the close) is
// left to the core, and its memory to the process.

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

typedef struct SendCtx SendCtx;
typedef struct MockRecord MockRecord;
typedef struct InjectedError InjectedError;

typedef struct {
    napi_env env;                              // the environment that created it (JS thread)
    kafka_producer_KafkaProducer_t* kp;        // owned class handle of a KafkaProducer, NULL once destroyed
    kafka_producer_MockProducer_t* mock;       // owned class handle of a MockProducer, NULL once destroyed
    const kafka_producer_Producer_t* view;     // borrowed __as_Producer view of kp or mock
    CallbackQueue queue;                       // notify hook, tsfn, drain, liveness
    int destroying;                            // KafkaProducer_destroy_err began: no report from now on
    SendCtx* orphans;                          // contexts of sends that failed at hand-over
    SendCtx* awaiting;                         // contexts whose callback's Promise has not settled
    SendCtx* current;                          // the context whose callback runs, or whose Promise is awaited
    SendCtx* run_head;                         // the callbacks waiting for their turn, in completion order
    SendCtx* run_tail;
    int running_queue;                         // send_run_queued's loop runs
    int owners;                                // the handle object, the tsfn, each awaited callback's function
    // The send contexts whose record's future JS may wait on, by the slot
    // number JS holds (a small integer, so no object is made per record);
    // a free slot holds NULL and is chained through `free_next`.
    SendCtx** slots;
    int32_t* free_next;
    int32_t slot_count;
    int32_t free_head;                         // -1: none free
    MockRecord* kept;                          // a mock's records taken by its hand-over, held until clear() or destroy
    InjectedError* injected;                   // the errors JS gave a mock, held while the core holds a copy
    SendCtx* mock_pending;                     // a mock's sends taken and not delivered yet: freed at destroy
} Producer;

// Whether the class handle is alive: not destroyed (only the handle object's
// finalizer destroys it).
static int producer_alive(const Producer* p) {
    return p->kp != NULL || p->mock != NULL;
}

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
    napi_ref q_args;                           // a callback waiting for its turn: its onComplete arguments
    int queued;                                // on p->run_head's list
    int turn;                                  // send_run_queued runs the callback in its turn
    SendCtx* run_next;
    kafka_common_KafkaFuture_t* future;        // the record's future (owned), from the hand-over until destroyed
    int waits;                                 // the waits on `future` still pending (each one also holds a ref)
    int js_holds;                              // JS was handed the future's slot number (`slot`)
    int32_t slot;                              // its slot in p->slots while js_holds, else -1
    int js_settled;                            // JS recorded the delivery: it reads the future no more
    MockRecord* mock_record;                   // a mock's send: the record it keeps, until the hand-over
    // A mock's send taken and not delivered yet, on p->mock_pending: its
    // completion may never come (Java's mock completes no send at close, and
    // clear() drops the waiting ones), so destroy frees it. Its `on_complete`
    // is weak: the MockProducer holds the function (src/producer/mock_producer.ts
    // `#pending`), so an unreferenced closed mock is collected and destroyed;
    // once its callback waits for its turn, it is strong (send_enqueue).
    int mock_pending;
    SendCtx* pend_prev;
    SendCtx* pend_next;
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

// The counts producerStats() reads, for the memory tests (test/memory/):
// each native object and unit made, and each freed or given back. Test-only.
static atomic_long stat_producers_made;
static atomic_long stat_producers_freed;
static atomic_long stat_owner_units_taken;
static atomic_long stat_owner_units_released;
static atomic_long stat_contexts_made;
static atomic_long stat_contexts_freed;
static atomic_long stat_slots_taken;
static atomic_long stat_slots_returned;
static atomic_long stat_futures_held;
static atomic_long stat_futures_destroyed;
static atomic_long stat_waits_started;
static atomic_long stat_waits_completed;
static atomic_long stat_deferrals_made;
static atomic_long stat_deferrals_reported;
static atomic_long stat_deferrals_released;
static atomic_long stat_buffer_refs_taken;
static atomic_long stat_buffer_refs_released;
static atomic_long stat_mock_records_made;
static atomic_long stat_mock_records_freed;
static atomic_long stat_injected_made;
static atomic_long stat_injected_freed;

static void producer_release(Producer* p) {
    atomic_fetch_add(&stat_owner_units_released, 1);
    if (--p->owners != 0) return;
    cbq_dispose(&p->queue);
    free(p->slots);
    free(p->free_next);
    free(p);
    atomic_fetch_add(&stat_producers_freed, 1);
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
    atomic_fetch_add(&stat_slots_taken, 1);
    return i;
}

static void slot_give_back(Producer* p, int32_t i) {
    p->slots[i] = NULL;
    p->free_next[i] = p->free_head;
    p->free_head = i;
    atomic_fetch_add(&stat_slots_returned, 1);
}

// ---- a mock's records and errors --------------------------------------------

// A record a MockProducer keeps (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Implementation over the FFI): its key and value cross as
// the `void *` addresses of `key_tag` and `value_tag`, which the FFI's mock
// keeps without reading, and `history()` finds the record again from the key
// it hands back. The reference holds the app's ProducerRecord, and so its key
// and value, from the send until clear() (for a record its hand-over took)
// or the destroy.
struct MockRecord {
    napi_ref record;
    char key_tag;
    char value_tag;
    MockRecord* next;
};

static void mock_record_free(napi_env env, MockRecord* r) {
    delete_ref(env, &r->record);
    free(r);
    atomic_fetch_add(&stat_mock_records_freed, 1);
}

static void mock_records_free(napi_env env, MockRecord** list) {
    while (*list != NULL) {
        MockRecord* r = *list;
        *list = r->next;
        mock_record_free(env, r);
    }
}

// The record whose key crossed as `key`, or NULL for a pointer no mock's
// send made (none: the FFI's mock hands back the pointers it was given).
static MockRecord* mock_record_of(const void* key) {
    if (key == NULL) return NULL;
    return (MockRecord*)((const char*)key - offsetof(MockRecord, key_tag));
}

// An error JS gave a mock (setSendException, errorNext, ...): the core
// carries it as a LocalCallbackError whose opaque is this struct, and
// error.c hands the very same JS value back (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Errors). Two owners: the core's error, which
// releases it from any thread once its last copy is dropped
// (injected_error_release), and the producer struct, which deletes the
// reference on the JS thread once the core's copies are gone (the sweep of
// every mock call, injected_errors_sweep) or at the destroy
// (injected_errors_release); the struct is freed by the last of them, and a
// value read after the destroy is the core's plain error.
struct InjectedError {
    uint32_t magic;
    atomic_int owners;
    napi_env env;
    napi_ref value;
    InjectedError* next;
};

#define INJECTED_ERROR_MAGIC 0x4a53494eu

static void injected_error_release(void* opaque) {
    InjectedError* e = (InjectedError*)opaque;
    if (atomic_fetch_sub(&e->owners, 1) == 1) {
        free(e);
        atomic_fetch_add(&stat_injected_freed, 1);
    }
}

static void injected_errors_release(napi_env env, Producer* p) {
    while (p->injected != NULL) {
        InjectedError* e = p->injected;
        p->injected = e->next;
        delete_ref(env, &e->value);
        injected_error_release(e);
    }
}

// The errors the core holds no copy of any more (the producer struct is their
// last owner): their references are deleted, on the JS thread, so a value JS
// gave the mock is collectable once the core is done with it. Run by every
// mock call (unwrap_mock).
static void injected_errors_sweep(napi_env env, Producer* p) {
    InjectedError** link = &p->injected;
    while (*link != NULL) {
        InjectedError* e = *link;
        if (atomic_load(&e->owners) == 1) {
            *link = e->next;
            delete_ref(env, &e->value);
            injected_error_release(e);
        } else {
            link = &e->next;
        }
    }
}

napi_value injected_error_value(napi_env env, void* opaque) {
    InjectedError* e = (InjectedError*)opaque;
    napi_value value = NULL;
    if (e == NULL || e->magic != INJECTED_ERROR_MAGIC || e->env != env || e->value == NULL) return NULL;
    if (napi_get_reference_value(env, e->value, &value) != napi_ok) return NULL;
    return value;
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
    // Java's long: a bigint (javascript/CLAUDE.md, JavaScript Binding Conventions, Types).
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
    // faultNext('metadata'): the factory's call fails.
    return fault_due(FAULT_METADATA) ? NULL : call_ref(env, data->metadata_factory, 1, &obj, NULL);
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
    atomic_fetch_add(&stat_buffer_refs_released, (ctx->key_ref != NULL) + (ctx->value_ref != NULL));
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
    atomic_fetch_add(&stat_futures_destroyed, 1);
    ctx->future = NULL;
}

// The record's future is no longer needed once its callback reported, no
// wait on it is pending, and JS reads it no more: JS recorded the delivery,
// or was never handed it. It is destroyed then, on the JS thread, with no
// finalizer of its own.
static void send_ctx_maybe_destroy_future(napi_env env, SendCtx* ctx) {
    if (ctx->delivered && ctx->waits == 0 && (ctx->js_settled || !ctx->js_holds)) send_ctx_destroy_future(env, ctx);
}

static void mock_record_free(napi_env env, MockRecord* r);

// Takes a mock's send off p->mock_pending, once.
static void mock_pending_unlink(SendCtx* ctx) {
    if (!ctx->mock_pending) return;
    Producer* p = ctx->p;
    if (ctx->pend_prev != NULL) ctx->pend_prev->pend_next = ctx->pend_next;
    else p->mock_pending = ctx->pend_next;
    if (ctx->pend_next != NULL) ctx->pend_next->pend_prev = ctx->pend_prev;
    ctx->pend_prev = ctx->pend_next = NULL;
    ctx->mock_pending = 0;
}

static void send_ctx_free(napi_env env, SendCtx* ctx) {
    mock_pending_unlink(ctx);
    send_ctx_release_buffers(env, ctx);
    if (ctx->mock_record != NULL) {
        mock_record_free(env, ctx->mock_record);
        ctx->mock_record = NULL;
    }
    delete_ref(env, &ctx->on_handover);
    delete_ref(env, &ctx->on_complete);
    delete_ref(env, &ctx->q_args);
    // A future JS still holds and never recorded the delivery of (its
    // callback reached no JS: an environment teardown) stays readable through
    // the handle JS has, so its context is kept. A callback Promise the
    // destroy reported as returned is not such a case, although a completed
    // close does not rule it out (a close() made from the callback before its
    // first await closes at once, the Promise still pending): JS holds none of
    // those futures any more (send_report_awaiting).
    if (ctx->future != NULL && ctx->js_holds && !ctx->js_settled) return;
    send_ctx_destroy_future(env, ctx);
    free(ctx);
    atomic_fetch_add(&stat_contexts_freed, 1);
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
// until its completion, and is one of the producer's counted calls.
typedef struct {
    SendCtx* ctx;
    Producer* p;
    int timed;
    napi_ref done;                             // JS (metadata | null, error, timedOut, failed) => void
} FutureWait;

// The completion of a wait (kafka_common_KafkaFuture_get_cb_t and
// kafka_common_KafkaFuture_get_with_timeout_cb_t): run by the producer's
// drain, inline inside the wait's call once the producer is destroyed, or by
// its destroy, on the JS thread. `value` is the record's metadata, borrowed;
// `error` is owned. A completion the destroy runs, from the handle object's
// finalizer, reaches no JS, and its JS wait never settles: nothing in the FFI
// orders it before the close's completion, and no other wait is known to
// stay pending past a completed close. An answer that can't be built
// (`failed`) fails the future only while it is still pending: the core
// completes the native future only once the delivery's callback reported, so
// its delivery reached no JS. Otherwise the failure is a warning, and a timed
// wait that timed out ends only its get() with it, or warns it once that
// get() settled (src/common/kafka_future.ts).
static void future_on_wait(void* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    FutureWait* w = (FutureWait*)opaque;
    Producer* p = w->p;
    SendCtx* ctx = w->ctx;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (!(p->queue.finalizing && producer_alive(p))) {
        int timed_out = 0;
        if (w->timed && error != NULL) {
            int code = 0;
            error_warn(env, 1, FFI_SET(code, (int)kafka_common_Error__enum_err(error, &ffi_err)));
            // The wait timed out (Java's java.util.concurrent.TimeoutException),
            // not the record.
            timed_out = code == kafka_common_Error_LOCAL_TIMEOUT;
        }
        napi_value argv[4];
        argv[0] = metadata_to_js(env, (const kafka_producer_RecordMetadata_t*)value);
        argv[1] = NULL;
        if (argv[0] != NULL) {
            argv[1] = error_take_to_js(env, error);
            error = NULL;
        }
        int failed = argv[1] == NULL;
        if (failed && napi_get_null(env, &argv[0]) != napi_ok) argv[0] = NULL;
        if (napi_get_boolean(env, timed_out != 0, &argv[2]) != napi_ok || napi_get_boolean(env, failed != 0, &argv[3]) != napi_ok)
            argv[0] = NULL;
        if (failed && argv[0] != NULL) argv[1] = js_take_failure(env, "converting a record future's answer");
        if (argv[0] != NULL && argv[1] != NULL) call_ref(env, w->done, 4, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &w->done);
    free(w);
    // The value is borrowed from the future until this returns: only now may
    // the last wait let the context destroy it.
    ctx->waits--;
    atomic_fetch_add(&stat_waits_completed, 1);
    send_ctx_maybe_destroy_future(env, ctx);
    send_ctx_release(env, ctx);
    cbq_call_end(env, &p->queue);
    // The wait's unit of the struct, which holds the queue: released last.
    producer_release(p);
}

// recordFutureGet(handle, future, timeoutMs, done): a wait on a record's future
// through its `_cb` twins, never the blocking get (javascript/CLAUDE.md, rule
// 5): `get_cb` for a timeoutMs of null, else `get_with_timeout_cb`, a
// negative timeoutMs passed on as it is (the FFI waits no time then).
// `future` is the slot number the hand-over gave JS for the record, which JS
// calls this with, and the producer's handle, only until it records the
// delivery. `done(metadata | null, error | null,
// timedOut, failed)` runs once it completed (future_on_wait), from the drain,
// or inside this call once the producer is destroyed; `failed`: the answer
// could not be built, `error` being the failure.
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
    atomic_fetch_add(&stat_waits_started, 1);
    atomic_fetch_add(&stat_owner_units_taken, 1);
    if (!w->timed) {
        cbq_call_begin(env, &p->queue);
        kafka_common_KafkaFuture_get_cb(ctx->future, future_on_wait, w);
    } else {
        cbq_call_begin(env, &p->queue);
        kafka_common_KafkaFuture_get_with_timeout_cb(ctx->future, timeout, future_on_wait, w);
    }
    return NULL;
}

// The send's hand-over completion (kafka_producer_Producer_send_with_callback_cb_t):
// inside send_with_callback_cb for a record taken or refused at once, else from
// the drain, or from destroy; always on the JS thread. `value`, the record's
// future, is kept on the context, and JS gets the context's slot number to
// wait on it with (the record's future section above).
static void send_on_handover(kafka_common_KafkaFuture_t* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
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
    if (value != NULL) atomic_fetch_add(&stat_futures_held, 1);
    send_ctx_release_buffers(env, ctx);
    if (!p->queue.finalizing && ctx->on_handover != NULL) {
        napi_value argv[2];
        argv[0] = error_take_to_js(env, error);
        error = NULL;
        argv[1] = NULL;
        int holds = 0;
        if (value == NULL) {
            // A refused record whose error can't be built: send() rejects
            // with the failure.
            if (napi_get_null(env, &argv[1]) != napi_ok) argv[1] = NULL;
            if (argv[0] == NULL && argv[1] != NULL) argv[0] = js_take_failure(env, "converting a send's answer");
        } else if (argv[0] != NULL) {
            ctx->slot = slot_take(p, ctx);
            // faultNext('slot'): the slot taken can't be made a number.
            if (ctx->slot >= 0 && !fault_due(FAULT_SLOT) && napi_create_int32(env, ctx->slot, &argv[1]) == napi_ok)
                holds = 1;
            // A record taken whose slot JS can't get: send() resolves with a
            // future the delivery completes. A slot taken goes back once that
            // future is destroyed (`js_holds` 0).
            else if (napi_get_null(env, &argv[1]) != napi_ok)
                argv[1] = NULL;
        }
        if (argv[0] != NULL && argv[1] != NULL) {
            ctx->js_holds = holds;
            call_ref(env, ctx->on_handover, 2, argv, NULL);
        } else {
            js_warn_thrown(env);
        }
    }
    // A mock keeps a record its send took, and never one it refused: the
    // record is held from now on until clear() or the destroy.
    if (ctx->mock_record != NULL && !ctx->handover_failed) {
        ctx->mock_record->next = p->kept;
        p->kept = ctx->mock_record;
        ctx->mock_record = NULL;
    }
    // A mock's refused send: the core's mock refuses before it keeps the
    // record or makes a completion (MockProducer::send_with_callback's
    // verify_can_send), and drops the delivery callback uncalled, so no
    // delivery can follow: the context, and the app's record with it, are
    // freed now.
    if (p->mock != NULL && ctx->handover_failed) {
        send_ctx_maybe_destroy_future(env, ctx);
        error_discard(error);
        send_ctx_free(env, ctx);
    } else {
        // A delivery that came first has already reported.
        send_ctx_maybe_destroy_future(env, ctx);
        error_discard(error);
        delete_ref(env, &ctx->on_handover);
        if (ctx->handover_failed && --ctx->refs > 0) {
            // The delivery callback may still fire, once (the record had
            // reached the accumulator): keep the context until destroy.
            ctx->orphaned = 1;
            ctx->next = p->orphans;
            p->orphans = ctx;
            // Refused once the core reported the close: no delivery callback
            // can fire for it any more, so its JS function is dropped now, as
            // close_on_complete drops the others'. Its closure holds the
            // KafkaProducer, which would otherwise never be collected, nor
            // the closed producer destroyed. One that fired and waits for its
            // turn keeps it: it still runs.
            if (cbq_is_closed(&p->queue) && !ctx->queued) delete_ref(env, &ctx->on_complete);
        } else if (ctx->handover_failed) {
            send_ctx_free(env, ctx);
        } else {
            send_ctx_release(env, ctx);
        }
    }
    cbq_call_end(env, &p->queue);
}

// The owned error a callback's failure is reported with: the core logs it, as
// Java logs a throwing onCompletion. `thrown` is the text src/ returned.
static kafka_common_Error_t* callback_error(const char* thrown) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    kafka_common_Error_t* panic = FFI_SET(report, kafka_common_Error_local_callback_err(thrown, NULL, NULL, &ffi_err));
    if (report == NULL) return panic;
    error_discard(panic);
    error_stat_made(report);
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
    atomic_fetch_add(&stat_deferrals_reported, 1);
    kafka_common_Error_t* report = thrown != NULL ? callback_error(thrown) : NULL;
    kafka_common_Error_t* ffi_err = NULL;
    if (!p->destroying && p->view != NULL) {
        error_stat_given(report);
        error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_Producer__set_callback_result_err(p->view, ctx->callback_id, NULL, report, &ffi_err)));
    } else {
        error_discard(report);
    }
    ctx->delivered = 1;
    send_ctx_maybe_destroy_future(env, ctx);
    // Its turn ends with its report: the callbacks waiting behind it may run
    // (send_run_queued, which its caller runs).
    if (p->current == ctx) p->current = NULL;
    // The function may be collected from now on; its finalizer releases.
    if (ctx->settle_ref != NULL) {
        napi_delete_reference(env, ctx->settle_ref);
        ctx->settle_ref = NULL;
    }
}

// Reports every awaited callback as returned, before the producer is
// destroyed (producer_destroy, from the handle object's finalizer): no JS
// will settle them, and destroy refuses a report once it began. Nor does JS
// hold their futures any more: a KafkaFuture holding a slot holds the handle
// object, which is being finalized. So each future is destroyed, and its slot
// given back, as a recorded one is, once its last wait completed; kept, it
// would stay with its context and slot for good.
static void send_report_awaiting(napi_env env, Producer* p) {
    while (p->awaiting != NULL) {
        SendCtx* ctx = p->awaiting;
        ctx->js_holds = 0;
        send_awaited_report(env, ctx, NULL);
    }
}

static void send_run_queued(Producer* p);

// The function a callback's Promise calls once settled, as both its handlers
// (src/ resolves it with undefined, or with the text of what the callback
// threw or rejected with; it never rejects, but a rejection, whatever its
// reason, is still reported once, as a failure). Then the callbacks that
// waited behind it take their turn.
static napi_value js_send_on_settled(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1] = {NULL};
    void* data = NULL;
    if (napi_get_cb_info(env, info, &argc, argv, NULL, &data) != napi_ok || data == NULL) return NULL;
    SendCtx* ctx = (SendCtx*)data;
    Producer* p = ctx->p;
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
    send_run_queued(p);
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
    atomic_fetch_add(&stat_deferrals_released, 1);
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
    atomic_fetch_add(&stat_owner_units_taken, 1);
    atomic_fetch_add(&stat_deferrals_made, 1);
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

// Puts a delivered callback at the end of the run queue, with the `argc`
// arguments it will receive (`argv`: send_on_completion's), kept in an array
// the context references until it runs. A mock's function, weak while src/
// holds it (SendCtx.mock_pending), is held strongly from now on: its record
// completed, so it runs in its turn even once src/ let it go (clear() drops
// the functions of the sends still waiting), as Java's MockProducer ran the
// callback of every record it completed. Returns 0, or -1 when a Node-API
// call failed (a JS exception may be pending).
static int send_enqueue(napi_env env, SendCtx* ctx, size_t argc, napi_value* argv, int64_t callback_id) {
    Producer* p = ctx->p;
    napi_value args;
    if (fault_due(FAULT_ENQUEUE) || napi_create_array_with_length(env, argc, &args) != napi_ok) return -1;
    for (size_t i = 0; i < argc; i++)
        if (napi_set_element(env, args, (uint32_t)i, argv[i]) != napi_ok) return -1;
    if (napi_create_reference(env, args, 1, &ctx->q_args) != napi_ok) return -1;
    if (p->mock != NULL && napi_reference_ref(env, ctx->on_complete, NULL) != napi_ok) {
        delete_ref(env, &ctx->q_args);
        return -1;
    }
    ctx->callback_id = callback_id;
    ctx->queued = 1;
    ctx->run_next = NULL;
    if (p->run_tail != NULL) p->run_tail->run_next = ctx;
    else p->run_head = ctx;
    p->run_tail = ctx;
    return 0;
}

// The first callback waiting for its turn, taken off the run queue; the
// values it was queued with stay on it (q_args) for its turn.
static SendCtx* send_dequeue(Producer* p) {
    SendCtx* ctx = p->run_head;
    p->run_head = ctx->run_next;
    if (p->run_head == NULL) p->run_tail = NULL;
    ctx->run_next = NULL;
    ctx->queued = 0;
    return ctx;
}

// The arguments a callback was queued with, into `argv`, their count into
// `*argc`, and the reference on them dropped. Returns 1, or 0 when they
// cannot be read.
static int send_queued_args(napi_env env, SendCtx* ctx, size_t* argc, napi_value* argv) {
    napi_value args;
    uint32_t n = 0;
    int have = ctx->q_args != NULL && napi_get_reference_value(env, ctx->q_args, &args) == napi_ok && args != NULL
               && napi_get_array_length(env, args, &n) == napi_ok && n >= 2 && n <= 3;
    for (uint32_t i = 0; have && i < n; i++) have = napi_get_element(env, args, i, &argv[i]) == napi_ok;
    if (have) *argc = n;
    delete_ref(env, &ctx->q_args);
    return have;
}

// The values of a delivery whose metadata or error can't be built, so that
// its callback still runs once: the metadata when it was built, else null,
// which src/ makes Java's metadata of a failed record, and the failure
// (js_take_failure) as its error. For a null metadata, the partition of the
// core's (`metadata`, read in C) goes as a third argument, so that metadata
// keeps the partition the core gave the record; with no handle, src/ uses
// the record's. Returns 1, or 0 when even the failure can't be made (a JS
// exception may be pending then).
static int send_failure_args(napi_env env, size_t* argc, napi_value* argv, const kafka_producer_RecordMetadata_t* metadata) {
    int built = argv[0] != NULL;
    if (!built && napi_get_null(env, &argv[0]) != napi_ok) argv[0] = NULL;
    argv[1] = argv[0] != NULL ? js_take_failure(env, "converting a record's delivery") : NULL;
    if (argv[1] == NULL) return 0;
    if (!built && metadata != NULL) {
        kafka_common_Error_t* ffi_err = NULL;
        int32_t partition = -1;
        kafka_common_Error_t* failed = FFI_SET(partition, kafka_producer_RecordMetadata_partition_err(metadata, &ffi_err));
        if (failed != NULL) error_warn(env, 1, failed);
        else if (napi_create_int32(env, partition, &argv[2]) == napi_ok) *argc = 3;
    }
    return 1;
}

// Java's Callback.onCompletion (kafka_producer_Callback_on_completion_fn_t),
// run by the drain or by destroy on the JS thread. Both arguments are
// BORROWED: they are made into the callback's JS values here. Order
// (javascript/CLAUDE.md, rule 6): the JS side runs the user callback and then
// completes the KafkaFuture; then the result is reported, once.
//
// What runs inside the drain here: the user's Java-style Callback, the
// one piece of user code called synchronously, and the settling of the
// record's Promises (KafkaFuture.get()'s), whose reactions run as microtasks
// once the drain has returned to JS, never inside it.
//
// The JS side returns undefined (reported now, as returned), a string (what
// the callback threw: reported now with it) or a Promise (the callback
// returned one: the future completes and the report is made once it settled,
// send_defer).
//
// One callback at a time, in completion order, as Java runs them one after
// another on its I/O thread: while another callback runs or its Promise is
// awaited (p->current), or callbacks already wait for their turn, a callback
// the core delivers waits at the end of the run queue (send_enqueue), with
// its values, and send_run_queued calls this again for it once its turn
// came (ctx->turn), with the values it was queued with; it reports then. So
// a later record's callback starts only once an earlier callback's Promise
// settled; the record's future still completes after its own callback, and a
// flush() or close() covering it still waits for its report. This is no send
// queue (rule 7): the core hands the records over and delivers them as
// before; only the callbacks wait for their turn, as Python's AsyncProducer
// keeps them. A callback the drain runs leaves none waiting behind it: the
// core queues every delivery, and drains never nest, so none is delivered
// while it runs. Those delivered during a turn (a send() whose pump runs the
// core's queue) wait for send_run_queued's loop, which runs them next.
//
// Values that can't be built (send_failure_args) still run the callback once,
// with the failure as its error, so the future fails with it and a flush()
// covering the record finds it done, as Java runs every record's callback.
// Only when even the failure can't be made is the delivery a warning, and
// reported as returned. A callback the run queue can't take (send_enqueue)
// runs at once, out of turn, rather than never.
//
// `callback_id` stays in C: it is an int64_t the core may number past
// 2^53, which a JS number cannot hold exactly, so it is never handed to JS. It
// is used only here, in the one __set_callback_result report, or kept on the
// context for an awaited or queued callback's.
static void send_on_completion(void* self_, const kafka_producer_RecordMetadata_t* metadata,
                               const kafka_common_Error_t* error, int64_t callback_id) {
    SendCtx* ctx = (SendCtx*)self_;
    Producer* p = ctx->p;
    napi_env env = p->env;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    int deferred = 0;
    // In its turn (send_run_queued), it runs with the values it was queued with.
    int turn = ctx->turn;
    ctx->turn = 0;
    mock_pending_unlink(ctx);
    // A mock's function is held weakly (SendCtx.mock_pending), until it
    // waits for its turn (send_enqueue): once its MockProducer is gone,
    // nothing runs.
    napi_value on_complete_fn = NULL;
    if (ctx->on_complete != NULL && napi_get_reference_value(env, ctx->on_complete, &on_complete_fn) != napi_ok) on_complete_fn = NULL;
    if (!p->queue.finalizing && on_complete_fn != NULL) {
        napi_value argv[3] = {NULL, NULL, NULL};
        size_t argc = 2;
        int have;
        if (turn) {
            have = send_queued_args(env, ctx, &argc, argv);
        } else {
            argv[0] = metadata_to_js(env, metadata);
            argv[1] = argv[0] != NULL ? error_to_js(env, error) : NULL;
            have = argv[0] != NULL && argv[1] != NULL;
        }
        if (!have) have = send_failure_args(env, &argc, argv, metadata);
        SendCtx* holder = NULL;
        int out_of_turn = 0;
        if (have && !turn && (p->current != NULL || p->run_head != NULL)) {
            // Not its turn: it waits behind the others.
            if (send_enqueue(env, ctx, argc, argv, callback_id) == 0) return;
            // One that cannot wait runs now, out of turn, instead of never;
            // the callback whose turn it is keeps it.
            js_warn_thrown(env);
            holder = p->current;
            out_of_turn = 1;
        }
        if (have) {
            char* thrown = NULL;
            // Its turn: a callback the core delivers meanwhile waits behind it.
            p->current = ctx;
            napi_value r = call_ref(env, ctx->on_complete, argc, argv, &thrown);
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
            if (thrown != NULL) {
                report = callback_error(thrown);
                free(thrown);
            }
            // The turn ends with the callback, or, for an awaited one, with
            // its report (send_awaited_report).
            if (!deferred && p->current == ctx) p->current = NULL;
            if (out_of_turn) p->current = holder;
        } else {
            js_warn_thrown(env);
        }
    }
    delete_ref(env, &ctx->on_complete);
    // A turn that ran no JS (its function dropped) leaves its values unread.
    delete_ref(env, &ctx->q_args);
    // An awaited callback reports once its Promise settled (send_on_settled);
    // its function owns the context from now on.
    if (deferred) return;
    // The report the core takes over, counted for errorStats() (test-only).
    if (!p->destroying) error_stat_given(report);
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

// Runs the callbacks waiting for their turn, in completion order, while no
// callback runs or is awaited: one that returns a Promise stops the run until
// it settled (js_send_on_settled runs this again). Never while the handle
// object is finalized or once the destroy began: send_report_queued reports
// those callbacks then.
static void send_run_queued(Producer* p) {
    if (p->running_queue) return;
    p->running_queue = 1;
    while (p->current == NULL && p->run_head != NULL && !p->queue.finalizing && !p->destroying) {
        SendCtx* ctx = send_dequeue(p);
        ctx->turn = 1;
        send_on_completion(ctx, NULL, NULL, ctx->callback_id);
    }
    p->running_queue = 0;
}

// Reports every callback still waiting for its turn as returned, before the
// producer is destroyed (producer_destroy, after send_report_awaiting): the
// finalizer runs no JS, so they never run, and destroy refuses a report once
// it began. JS holds none of their futures any more, as for the awaited ones.
static void send_report_queued(napi_env env, Producer* p) {
    while (p->run_head != NULL) {
        SendCtx* ctx = send_dequeue(p);
        delete_ref(env, &ctx->q_args);
        ctx->js_holds = 0;
        ctx->turn = 1;
        send_on_completion(ctx, NULL, NULL, ctx->callback_id);
    }
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
    if (!producer_alive(p)) {
        napi_throw_error(env, NULL, "the producer has been destroyed");
        return NULL;
    }
    return p;
}

static void send_hand_over(napi_env env, Producer* p, SendCtx* ctx, kafka_producer_ProducerRecord_t* record,
                           napi_value key, napi_value value, napi_value on_handover, napi_value on_complete);

// producerSend(handle, topic, partition, timestamp, key, value, headers,
// onHandover, onComplete): partition -1 and timestamp -1n (a bigint) for
// Java's null; key and
// value a Uint8Array or null, the app's (a serializer's included) or the
// client's own (an encoded string), never copied: each is kept by a reference
// until the hand-over (javascript/CLAUDE.md, rule 8); headers null or the
// record's headers, each key's bytes then its value, copied into the record
// (native/headers.c).
// Throws the core's error when the record
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
    atomic_fetch_add(&stat_contexts_made, 1);
    ctx->p = p;
    ctx->refs = 2;
    ctx->slot = -1;
    if (js_bytes(env, argv[4], "key must be a Buffer, a Uint8Array, a string or null", &ctx->key)
        || js_bytes(env, argv[5], "value must be a Buffer, a Uint8Array, a string or null", &ctx->value)) {
        free(topic);
        free(ctx);
        atomic_fetch_add(&stat_contexts_freed, 1);
        return NULL;
    }
    kafka_producer_ProducerRecord_t* record =
        record_build(env, topic, partition, timestamp, &ctx->key, &ctx->value, argv[6]);
    free(topic);
    if (record == NULL) {
        send_ctx_free(env, ctx);
        return NULL;
    }
    record_spy_capture(env, record, argv[4], argv[5]);
    send_hand_over(env, p, ctx, record, argv[4], argv[5], argv[7], argv[8]);
    return NULL;
}

// The rest of a send, once its record is built: the references taken, the
// completions registered and the record handed over through
// send_with_callback_cb, which consumes the callback and copies the record
// (the record is destroyed here). `key` and `value` are the JS values of the
// context's borrowed bytes (a mock's send has none: its key and value cross
// as tags). On a failure before the hand-over the context is freed and a JS
// error is pending.
static void send_hand_over(napi_env env, Producer* p, SendCtx* ctx, kafka_producer_ProducerRecord_t* record,
                           napi_value key, napi_value value, napi_value on_handover, napi_value on_complete) {
    kafka_common_Error_t* ffi_err = NULL;
    // The core reads the key's and value's own memory until the hand-over:
    // a reference keeps each Buffer, and so its memory, alive until then.
    int failed = (!ctx->key.is_null && napi_create_reference(env, key, 1, &ctx->key_ref) != napi_ok)
                 || (!ctx->value.is_null && napi_create_reference(env, value, 1, &ctx->value_ref) != napi_ok)
                 || napi_create_reference(env, on_handover, 1, &ctx->on_handover) != napi_ok
                 || napi_create_reference(env, on_complete, p->mock != NULL ? 0 : 1, &ctx->on_complete) != napi_ok;
    atomic_fetch_add(&stat_buffer_refs_taken, (ctx->key_ref != NULL) + (ctx->value_ref != NULL));
    if (failed) {
        error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecord_destroy_err(record, &ffi_err)));
        send_ctx_free(env, ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return;
    }
    kafka_producer_Callback_t* callback = NULL;
    kafka_common_Error_t* err = FFI_SET(callback, kafka_producer_Callback_new_err(ctx, send_on_completion, &ffi_err));
    if (err != NULL) {
        // Never registered: neither completion can fire.
        error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecord_destroy_err(record, &ffi_err)));
        send_ctx_free(env, ctx);
        error_throw_owned(env, err);
        return;
    }
    // Everything the completions read is in place: the hand-over completion can
    // fire inside this call.
    cbq_call_begin(env, &p->queue);
    kafka_producer_Producer_send_with_callback_cb(p->view, record, callback, send_on_handover, ctx);
    // The call consumed the callback and copied the record; the key and value
    // stay borrowed until the hand-over completion (send_on_handover).
    error_warn(env, 1, FFI_ERR(kafka_producer_ProducerRecord_destroy_err(record, &ffi_err)));
}

// ---- flush ------------------------------------------------------------------

static void flush_on_complete(kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    FlushCtx* ctx = (FlushCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    if (!p->queue.finalizing) {
        napi_value arg = error_take_to_js(env, error);
        error = NULL;
        // An answer that can't be built rejects the flush with the failure.
        if (arg == NULL) arg = js_take_failure(env, "converting a flush's answer");
        if (arg != NULL) call_ref(env, ctx->done, 1, &arg, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &p->queue);
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
    cbq_call_begin(env, &p->queue);
    kafka_producer_Producer_flush_cb(p->view, flush_on_complete, ctx);
    return NULL;
}

// ---- partitionsFor ----------------------------------------------------------

// A node's fields as src/common/node.ts's constructor takes them: { id, host,
// port, rack, isFenced }, or null for a NULL node.
napi_value node_to_js(napi_env env, const kafka_common_Node_t* n) {
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
    // faultNext('partitions'): the array can't be made.
    if (!fault_due(FAULT_PARTITIONS) && napi_create_array_with_length(env, (size_t)size, &arr) == napi_ok) {
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
// `done(null, partitions)`, or `done(failure, null)` for an answer that can't
// be built, then the end of the call's count (a call made on a closed
// producer holds Node until then).
static void partitions_for_on_complete(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    PartitionsForCtx* ctx = (PartitionsForCtx*)opaque;
    Producer* p = ctx->p;
    napi_env env = p->env;
    if (!p->queue.finalizing) {
        napi_value argv[2];
        if (error != NULL) {
            argv[0] = error_take_to_js(env, error);
            error = NULL;
            if (napi_get_null(env, &argv[1]) != napi_ok) argv[1] = NULL;
            if (argv[0] == NULL && argv[1] != NULL) argv[0] = js_take_failure(env, "converting a partitionsFor answer");
            if (argv[0] != NULL && argv[1] != NULL) call_ref(env, ctx->done, 2, argv, NULL);
            else js_warn_thrown(env);
            if (value != NULL) {
                kafka_common_Error_t* ffi_err = NULL;
                error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(value, &ffi_err)));
            }
        } else {
            argv[1] = value == NULL ? NULL : partitions_take_to_js(env, value);
            value = NULL;
            if (napi_get_null(env, &argv[0]) != napi_ok) argv[0] = NULL;
            // No partitions built (or no list, which the FFI rules out).
            if (argv[1] == NULL && argv[0] != NULL) {
                argv[1] = argv[0];
                argv[0] = js_take_failure(env, "converting a partitionsFor answer");
            }
            if (argv[0] != NULL && argv[1] != NULL) call_ref(env, ctx->done, 2, argv, NULL);
            else js_warn_thrown(env);
        }
    } else if (value != NULL) {
        kafka_common_Error_t* ffi_err = NULL;
        error_discard(FFI_ERR(kafka_List_destroy_err(value, &ffi_err)));
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &p->queue);
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
    cbq_call_begin(env, &p->queue);
    kafka_producer_Producer_partitions_for_cb(p->view, topic, partitions_for_on_complete, ctx);
    free(topic);
    return NULL;
}

// ---- metrics ----------------------------------------------------------------

// producerMetrics(handle): Producer.metrics() through kafka_producer_Producer_metrics,
// synchronously (it has no _cb twin): the live view, as a handle object that
// owns it (native/metrics.c). The view reads the registry at each call and
// stays valid after the producer is destroyed, so the handle object holds no
// reference on the producer.
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
    return metrics_view_take_to_js(env, view);
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
    // So are the callbacks waiting behind it for their turn, which never run.
    send_report_awaiting(env, p);
    send_report_queued(env, p);
    p->destroying = 1;
    if (p->kp != NULL) error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(p->kp, &ffi_err)));
    else error_warn(env, !p->queue.finalizing, FFI_ERR(kafka_producer_MockProducer_destroy_err(p->mock, &ffi_err)));
    p->kp = NULL;
    p->mock = NULL;
    p->view = NULL;
    // No callback can fire past this point, and a mock keeps no record and
    // no error any more. A mock's send whose completion never came (pending
    // at its close, or dropped by clear()) is freed: no JS can reach its
    // future, the handle object being collected.
    while (p->mock_pending != NULL) {
        SendCtx* ctx = p->mock_pending;
        mock_pending_unlink(ctx);
        ctx->js_holds = 0;
        send_ctx_free(env, ctx);
    }
    free_orphans(env, p);
    mock_records_free(env, &p->kept);
    injected_errors_release(env, p);
    cbq_release(env, &p->queue);
}

// The close completion (close_cb / close_with_timeout_cb), on the JS thread.
// A timeout the core rejected (Java's IllegalArgumentException) closed
// nothing: the producer stays open and keeps Node running. Any other outcome
// closed it: the handle object may be collected (cbq_closed), and Node may
// exit once no call made on the producer waits for its answer, this close
// included until its cbq_call_end, after `done` ran. `done(error | null,
// closedNow)`, closedNow true for the close that closed the producer, false
// for a rejected one or a later close.
static void close_on_complete(kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
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
    // A mock's close may fail and leave it open (Java's closeException): it
    // closed only when the mock says so.
    if (!rejected && p->mock != NULL) {
        int8_t shut = 0;
        error_warn(env, !p->queue.finalizing, FFI_SET(shut, kafka_producer_MockProducer_closed_err(p->mock, &ffi_err)));
        rejected = !shut;
    }
    int closed_now = rejected || p->queue.finalizing ? 0 : cbq_closed(env, &p->queue);
    // The close completed every record the accumulator held, so no orphan's
    // delivery callback can fire any more: their JS functions, whose closures
    // hold the KafkaProducer, are dropped, so the closed producer can be
    // collected and destroyed. One whose callback fired and waits for its turn
    // keeps its function: it still runs. Each context stays on the list until
    // destroy.
    if (closed_now)
        for (SendCtx* o = p->orphans; o != NULL; o = o->next)
            if (!o->queued) delete_ref(env, &o->on_complete);
    if (!p->queue.finalizing) {
        napi_value argv[2];
        argv[0] = error_take_to_js(env, error);
        error = NULL;
        // faultNext('close'): the answer can't be built.
        if (fault_due(FAULT_CLOSE)) argv[0] = NULL;
        if (napi_get_boolean(env, closed_now != 0, &argv[1]) != napi_ok) argv[1] = NULL;
        // An answer that can't be built rejects the close with the failure,
        // `closedNow` as the core's answer made it.
        if (argv[0] == NULL && argv[1] != NULL) argv[0] = js_take_failure(env, "converting a close's answer");
        if (argv[0] != NULL && argv[1] != NULL) call_ref(env, ctx->done, 2, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &p->queue);
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
    if (!timed) {
        cbq_call_begin(env, &p->queue);
        kafka_producer_Producer_close_cb(p->view, close_on_complete, ctx);
    } else {
        cbq_call_begin(env, &p->queue);
        kafka_producer_Producer_close_with_timeout_cb(p->view, timeout, close_on_complete, ctx);
    }
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
    if (producer_alive(p)) cbq_drain(env, &p->queue);
    return NULL;
}

// ---- finalization -----------------------------------------------------------

// The queue's on_tsfn_finalize: the tsfn's ownership of the struct ends.
static void producer_tsfn_finalized(napi_env env, void* client) {
    (void)env;
    Producer* p = (Producer*)client;
    if (producer_alive(p) && !p->destroying) {
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
    int open = producer_alive(p) && !cbq_is_closed(&p->queue);
    if (producer_alive(p) && cbq_is_closed(&p->queue)) producer_destroy(env, p);
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

// Destroys a class handle on a construction's failure path, before a handle
// object, and so a finalizer, exists.
static void class_destroy(napi_env env, kafka_producer_KafkaProducer_t* kp, kafka_producer_MockProducer_t* mock) {
    kafka_common_Error_t* ffi_err = NULL;
    if (kp != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_KafkaProducer_destroy_err(kp, &ffi_err)));
    if (mock != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_MockProducer_destroy_err(mock, &ffi_err)));
}

// The native handle over class handle `kp` or `mock` (one is NULL), which it
// owns from now on: its view, callback queue and handle object. Throws, and
// destroys the class handle, on failure.
static napi_value producer_handle_new(napi_env env, kafka_producer_KafkaProducer_t* kp,
                                      kafka_producer_MockProducer_t* mock) {
    kafka_common_Error_t* ffi_err = NULL;
    Producer* p = (Producer*)calloc(1, sizeof(Producer));
    if (p == NULL) {
        class_destroy(env, kp, mock);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    atomic_fetch_add(&stat_producers_made, 1);
    p->env = env;
    p->kp = kp;
    p->mock = mock;
    p->free_head = -1;
    kafka_common_Error_t* err = kp != NULL
        ? FFI_SET(p->view, kafka_producer_KafkaProducer__as_Producer_err(kp, &ffi_err))
        : FFI_SET(p->view, kafka_producer_MockProducer__as_Producer_err(mock, &ffi_err));
    if (err == NULL
        && cbq_init(env, &p->queue, "confluent-kafka producer callbacks", p, producer_execute, producer_tsfn_finalized)
               != 0) {
        class_destroy(env, kp, mock);
        free(p);
        atomic_fetch_add(&stat_producers_freed, 1);
        return NULL;
    }
    if (err != NULL) {
        class_destroy(env, kp, mock);
        free(p);
        atomic_fetch_add(&stat_producers_freed, 1);
        error_throw_owned(env, err);
        return NULL;
    }
    p->owners = 1;  // the tsfn
    atomic_fetch_add(&stat_owner_units_taken, 1);
    napi_value result = NULL;
    int thrown = 0;
    if (napi_create_object(env, &result) != napi_ok || napi_wrap(env, result, p, producer_finalize, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "napi_wrap");
        thrown = 1;
    } else {
        p->owners = 2;  // and the handle object
        atomic_fetch_add(&stat_owner_units_taken, 1);
        if (cbq_attach(env, &p->queue, result) != 0) thrown = 1;
    }
    // A test's faultNext('notify') fails the registration through the FFI's
    // own error path, as a panic the FFI caught would: a NULL view.
    if (!thrown)
        err = FFI_ERR(kafka_producer_Producer__set_callbacks_notify_err(fault_due(FAULT_NOTIFY) ? NULL : p->view, cbq_notify,
                                                                       &p->queue, &ffi_err));
    if (err != NULL || thrown) {
        class_destroy(env, kp, mock);
        p->kp = NULL;
        p->mock = NULL;
        p->destroying = 1;
        // The handle object, never handed out, may be collected: its
        // finalizer releases its unit of p, a destroyed client's.
        cbq_detach(env, &p->queue);
        // Releases the tsfn, whose finalizer frees p when it is the only owner.
        cbq_release(env, &p->queue);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return result;
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
    return producer_handle_new(env, kp, NULL);
}

// ---- MockProducer -------------------------------------------------------------

// The mock behind a native handle, or NULL with a TypeError pending for a
// handle that is no mock's.
static Producer* unwrap_mock(napi_env env, napi_value handle) {
    Producer* p = unwrap_producer(env, handle);
    if (p == NULL) return NULL;
    if (p->mock == NULL) {
        napi_throw_type_error(env, NULL, "not a mock producer handle");
        return NULL;
    }
    injected_errors_sweep(env, p);
    return p;
}

// mockProducerCreate(autoComplete): a native producer handle over the FFI's
// MockProducer, built through MockProducerOptionsBuilder (the cluster left
// unset, Cluster.empty(); no partitioner and no serializers: the binding
// runs the serializers, and the key and value cross as `void *`), as
// `new MockProducer(autoComplete, null, null, null)`.
napi_value js_mock_producer_create(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool auto_complete = false;
    if (argc != 1 || napi_get_value_bool(env, argv[0], &auto_complete) != napi_ok) {
        napi_throw_type_error(env, NULL, "mockProducerCreate(autoComplete)");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_producer_MockProducerOptionsBuilder_t* builder = NULL;
    kafka_producer_MockProducerOptions_t* options = NULL;
    kafka_producer_MockProducer_t* mock = NULL;
    kafka_common_Error_t* err = FFI_SET(builder, kafka_producer_MockProducerOptionsBuilder_new_err(&ffi_err));
    if (err == NULL)
        err = FFI_ERR(kafka_producer_MockProducerOptionsBuilder_set_auto_complete_err(builder, auto_complete ? 1 : 0, &ffi_err));
    // _build takes an out_err of its own.
    if (err == NULL) err = FFI_SET(options, kafka_producer_MockProducerOptionsBuilder_build(builder, &ffi_err));
    if (err == NULL) err = FFI_SET(mock, kafka_producer_MockProducer_with_options_err(options, &ffi_err));
    if (options != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_MockProducerOptions_destroy_err(options, &ffi_err)));
    if (builder != NULL) error_warn(env, 1, FFI_ERR(kafka_producer_MockProducerOptionsBuilder_destroy_err(builder, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return producer_handle_new(env, NULL, mock);
}

// mockProducerSend(handle, topic, partition, timestamp, record, onHandover,
// onComplete): a mock's send of the app's ProducerRecord `record`, whose
// key and value cross as a MockRecord's `void *` tags, which the FFI's mock
// keeps without reading; the record is held until clear() or the destroy
// (refused at the hand-over: released then, with the send's context). No
// header crosses: the mock keeps the app's own record, headers included. The
// mock's `onComplete` is held weakly: src/ keeps it until it runs.
napi_value js_mock_producer_send(napi_env env, napi_callback_info info) {
    size_t argc = 7;
    napi_value argv[7];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 7 || !is_function(env, argv[5]) || !is_function(env, argv[6])) {
        napi_throw_type_error(env, NULL, "mockProducerSend(handle, topic, partition, timestamp, record, onHandover, onComplete)");
        return NULL;
    }
    Producer* p = unwrap_mock(env, argv[0]);
    if (p == NULL) return NULL;
    int32_t partition = -1;
    int64_t timestamp = -1;
    bool lossless = false;
    NAPI_CHECK(env, napi_get_value_int32(env, argv[2], &partition), NULL);
    NAPI_CHECK(env, napi_get_value_bigint_int64(env, argv[3], &timestamp, &lossless), NULL);
    char* topic = js_string_dup(env, argv[1], "topic must be a string");
    if (topic == NULL) return NULL;
    SendCtx* ctx = (SendCtx*)calloc(1, sizeof(SendCtx));
    MockRecord* kept = (MockRecord*)calloc(1, sizeof(MockRecord));
    if (ctx == NULL || kept == NULL) {
        free(topic);
        free(ctx);
        free(kept);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    atomic_fetch_add(&stat_contexts_made, 1);
    atomic_fetch_add(&stat_mock_records_made, 1);
    ctx->p = p;
    ctx->refs = 2;
    ctx->slot = -1;
    ctx->key.is_null = 1;
    ctx->value.is_null = 1;
    if (napi_create_reference(env, argv[4], 1, &kept->record) != napi_ok) {
        free(topic);
        free(ctx);
        free(kept);
        atomic_fetch_add(&stat_contexts_freed, 1);
        atomic_fetch_add(&stat_mock_records_freed, 1);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    ctx->mock_record = kept;
    // Pending until its delivery, or the destroy (SendCtx.mock_pending).
    ctx->mock_pending = 1;
    ctx->pend_next = p->mock_pending;
    if (p->mock_pending != NULL) p->mock_pending->pend_prev = ctx;
    p->mock_pending = ctx;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_producer_ProducerRecord_t* record = NULL;
    // _with_partition_timestamp_key takes an out_err of its own.
    kafka_common_Error_t* err = FFI_SET(record, kafka_producer_ProducerRecord_with_partition_timestamp_key(
                                                    topic, partition, timestamp, &kept->key_tag, &kept->value_tag, &ffi_err));
    free(topic);
    if (err != NULL) {
        send_ctx_free(env, ctx);
        error_throw_owned(env, err);
        return NULL;
    }
    send_hand_over(env, p, ctx, record, NULL, NULL, argv[5], argv[6]);
    return NULL;
}

// mockProducerHistory(handle): the records the mock holds since the last
// clear(), in send order, each the app's own ProducerRecord (Java's history()).
napi_value js_mock_producer_history(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "mockProducerHistory(handle)");
        return NULL;
    }
    Producer* p = unwrap_mock(env, argv[0]);
    if (p == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_producer_MockProducer_history_err(p->mock, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    int32_t size = 0;
    error_warn(env, 1, FFI_SET(size, kafka_List_size_err(list, &ffi_err)));
    napi_value arr;
    napi_value result = NULL;
    if (napi_create_array_with_length(env, (size_t)(size > 0 ? size : 0), &arr) == napi_ok) {
        result = arr;
        uint32_t n = 0;
        for (int32_t i = 0; i < size; i++) {
            const kafka_producer_ProducerRecord_t* rec = NULL;
            error_warn(env, 1, FFI_SET(rec, (const kafka_producer_ProducerRecord_t*)kafka_List_get_err(list, i, &ffi_err)));
            const void* key = NULL;
            if (rec != NULL) error_warn(env, 1, FFI_SET(key, kafka_producer_ProducerRecord_key_err(rec, &ffi_err)));
            MockRecord* kept = mock_record_of(key);
            napi_value v;
            if (kept == NULL || kept->record == NULL || napi_get_reference_value(env, kept->record, &v) != napi_ok) continue;
            if (napi_set_element(env, arr, n++, v) != napi_ok) {
                result = NULL;
                break;
            }
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    if (result == NULL) js_throw_last_napi_error(env, "reading the mock's history");
    return result;
}

// mockProducerClear(handle): Java's clear(). The records the mock's
// hand-overs had taken before it are no longer held by the mock, so their
// references are dropped; a hand-over still to come drops its own at the next
// clear() or the destroy.
napi_value js_mock_producer_clear(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "mockProducerClear(handle)");
        return NULL;
    }
    Producer* p = unwrap_mock(env, argv[0]);
    if (p == NULL) return NULL;
    MockRecord* taken = p->kept;
    p->kept = NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_producer_MockProducer_clear_err(p->mock, &ffi_err));
    if (err != NULL) {
        // Nothing was cleared: the records stay held.
        p->kept = taken;
        error_throw_owned(env, err);
        return NULL;
    }
    mock_records_free(env, &taken);
    return NULL;
}

// The core's error for JS value `value`: NULL for `null`, else a
// LocalCallbackError carrying it (InjectedError), which the producer struct
// holds until the destroy. Throws and returns -1 on failure.
static int injected_error_new(napi_env env, Producer* p, napi_value value, kafka_common_Error_t** out) {
    *out = NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, value, &type), -1);
    if (type == napi_null) return 0;
    InjectedError* e = (InjectedError*)calloc(1, sizeof(InjectedError));
    if (e == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    e->magic = INJECTED_ERROR_MAGIC;
    e->env = env;
    atomic_init(&e->owners, 2);
    if (napi_create_reference(env, value, 1, &e->value) != napi_ok) {
        free(e);
        js_throw_last_napi_error(env, "napi_create_reference");
        return -1;
    }
    // The text the core logs: the error's String(), as Java's toString().
    napi_value text;
    char* message = NULL;
    if (napi_coerce_to_string(env, value, &text) == napi_ok) message = js_string_dup(env, text, "the error's text");
    if (message == NULL) {
        napi_value ignored;
        napi_get_and_clear_last_exception(env, &ignored);
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err =
        FFI_SET(*out, kafka_common_Error_local_callback_err(message, e, injected_error_release, &ffi_err));
    free(message);
    if (err != NULL) {
        delete_ref(env, &e->value);
        free(e);
        error_throw_owned(env, err);
        return -1;
    }
    e->next = p->injected;
    p->injected = e;
    atomic_fetch_add(&stat_injected_made, 1);
    error_stat_made(*out);
    return 0;
}

// mockProducerSetError(handle, field, error): Java's public error field
// `field` ("send", "flush", "partitionsFor" or "close") of the mock set to
// `error`, or cleared by `null`, through MockProducer_set_<field>_error; the
// mock throws the very same value.
napi_value js_mock_producer_set_error(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    char* field = argc == 3 ? js_string_dup(env, argv[1], "the field must be a string") : NULL;
    if (field == NULL) {
        if (argc != 3) napi_throw_type_error(env, NULL, "mockProducerSetError(handle, field, error)");
        return NULL;
    }
    Producer* p = unwrap_mock(env, argv[0]);
    kafka_common_Error_t* error = NULL;
    if (p == NULL || injected_error_new(env, p, argv[2], &error) != 0) {
        free(field);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = NULL;
    if (strcmp(field, "send") == 0 || strcmp(field, "flush") == 0 || strcmp(field, "partitionsFor") == 0
        || strcmp(field, "close") == 0)
        error_stat_given(error);
    if (strcmp(field, "send") == 0) err = FFI_ERR(kafka_producer_MockProducer_set_send_error_err(p->mock, error, &ffi_err));
    else if (strcmp(field, "flush") == 0) err = FFI_ERR(kafka_producer_MockProducer_set_flush_error_err(p->mock, error, &ffi_err));
    else if (strcmp(field, "partitionsFor") == 0)
        err = FFI_ERR(kafka_producer_MockProducer_set_partitions_for_error_err(p->mock, error, &ffi_err));
    else if (strcmp(field, "close") == 0) err = FFI_ERR(kafka_producer_MockProducer_set_close_error_err(p->mock, error, &ffi_err));
    else {
        error_discard(error);
        free(field);
        napi_throw_type_error(env, NULL, "the field must be send, flush, partitionsFor or close");
        return NULL;
    }
    free(field);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockProducerCompleteNext(handle, error): Java's errorNext(error), or
// completeNext() for a `null` error, through MockProducer_error_next (which
// completes successfully for NULL): true when a send was waiting. The
// record's callback is queued; src/ drains it (the pump) before it returns.
napi_value js_mock_producer_complete_next(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2) {
        napi_throw_type_error(env, NULL, "mockProducerCompleteNext(handle, error)");
        return NULL;
    }
    Producer* p = unwrap_mock(env, argv[0]);
    kafka_common_Error_t* error = NULL;
    if (p == NULL || injected_error_new(env, p, argv[1], &error) != 0) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    int8_t completed = 0;
    error_stat_given(error);
    kafka_common_Error_t* err = FFI_SET(completed, kafka_producer_MockProducer_error_next_err(p->mock, error, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, completed != 0, &result), NULL);
    return result;
}

// mockProducerState(handle, which): Java's closed() for "closed", flushed()
// for "flushed".
napi_value js_mock_producer_state(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    char* which = argc == 2 ? js_string_dup(env, argv[1], "the state must be a string") : NULL;
    if (which == NULL) {
        if (argc != 2) napi_throw_type_error(env, NULL, "mockProducerState(handle, which)");
        return NULL;
    }
    Producer* p = unwrap_mock(env, argv[0]);
    if (p == NULL) {
        free(which);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = NULL;
    int8_t state = 0;
    if (strcmp(which, "closed") == 0) err = FFI_SET(state, kafka_producer_MockProducer_closed_err(p->mock, &ffi_err));
    else if (strcmp(which, "flushed") == 0) err = FFI_SET(state, kafka_producer_MockProducer_flushed_err(p->mock, &ffi_err));
    else {
        free(which);
        napi_throw_type_error(env, NULL, "the state must be closed or flushed");
        return NULL;
    }
    free(which);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value result;
    NAPI_CHECK(env, napi_get_boolean(env, state != 0, &result), NULL);
    return result;
}

// ---- the counts, for the tests ------------------------------------------------

static int stat_put(napi_env env, napi_value obj, const char* name, atomic_long* count) {
    napi_value v;
    return napi_create_int64(env, (int64_t)atomic_load(count), &v) == napi_ok
                   && napi_set_named_property(env, obj, name, v) == napi_ok
               ? 0
               : -1;
}

// producerStats(): the counts of the native producers, their units of
// ownership, the send contexts, the future slots, the record futures, the
// waits on them, the callback deferrals, the key and value Buffer
// references, a mock's records and the errors JS gave a mock, each made and
// freed. Test-only.
napi_value js_producer_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (stat_put(env, obj, "producersMade", &stat_producers_made) || stat_put(env, obj, "producersFreed", &stat_producers_freed)
        || stat_put(env, obj, "ownerUnitsTaken", &stat_owner_units_taken)
        || stat_put(env, obj, "ownerUnitsReleased", &stat_owner_units_released)
        || stat_put(env, obj, "contextsMade", &stat_contexts_made) || stat_put(env, obj, "contextsFreed", &stat_contexts_freed)
        || stat_put(env, obj, "slotsTaken", &stat_slots_taken) || stat_put(env, obj, "slotsReturned", &stat_slots_returned)
        || stat_put(env, obj, "futuresHeld", &stat_futures_held)
        || stat_put(env, obj, "futuresDestroyed", &stat_futures_destroyed)
        || stat_put(env, obj, "waitsStarted", &stat_waits_started)
        || stat_put(env, obj, "waitsCompleted", &stat_waits_completed)
        || stat_put(env, obj, "deferralsMade", &stat_deferrals_made)
        || stat_put(env, obj, "deferralsReported", &stat_deferrals_reported)
        || stat_put(env, obj, "deferralsReleased", &stat_deferrals_released)
        || stat_put(env, obj, "bufferRefsTaken", &stat_buffer_refs_taken)
        || stat_put(env, obj, "bufferRefsReleased", &stat_buffer_refs_released)
        || stat_put(env, obj, "mockRecordsMade", &stat_mock_records_made)
        || stat_put(env, obj, "mockRecordsFreed", &stat_mock_records_freed)
        || stat_put(env, obj, "injectedMade", &stat_injected_made)
        || stat_put(env, obj, "injectedFreed", &stat_injected_freed)) {
        js_throw_last_napi_error(env, "producerStats");
        return NULL;
    }
    return obj;
}

// Copyright 2026 Confluent Inc.
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

// The KafkaConsumer handle: a marshaling-only bridge to the Rust consumer FFI
// (javascript/CLAUDE.md), built with NULL deserializers, so keys and values
// cross as kafka_Bytes_t. It runs no thread of its own and holds no Kafka
// logic. A MockConsumer is the same handle over the FFI's mock, whose records
// are the app's own (native/mock_values.c, MockRecord): a poll of it returns
// them and makes no external buffer.
//
// Every waiting call uses its `_cb` twin, whose completion the core queues on
// the consumer's callback vector; the consumer's CallbackQueue
// (callback_queue.c) drains it on the JS thread. Each `_cb` call is counted
// on the queue (cbq_call_begin right before it, cbq_call_end as the last
// statement of its completion, on every path), so a call made on a closed
// consumer holds Node until its answer ran. The three getters,
// groupMetadata(), wakeup() and metrics() have no `_cb` twin and go through
// their fallible or `_err` form.
//
// The records of a poll (Batch): the ConsumerRecords handle the completion
// receives owns every record and the bytes behind their keys and values, and
// nothing of the consumer, so it may outlive it. Each key and value is a
// Buffer over those bytes, made with napi_create_external_buffer, and each
// record's Headers holds a small external (HeaderSource) until it copied its
// headers out; each of them counts one reference on the batch, and the batch
// is destroyed when the last one is collected. Where Node-API refuses external
// buffers (napi_no_external_buffers_allowed, as Electron does, or the test
// switch consumerRefuseExternalBuffers), each key and value is copied instead,
// and a record's headers are looked at, so a record without headers holds
// nothing of the batch.
//
// Closed state and teardown (javascript/CLAUDE.md, rules 1 and 10): the
// binding keeps no closed flag; a call after close() goes to the FFI. close()
// does not free the native handle: its completion lets Node exit once no call
// waits and lets the handle object be collected (cbq_closed). The handle
// object's finalizer destroys only a closed consumer, outside any drain,
// running no JS.
//
// The listeners, the commit callbacks and the window: a rebalance listener's
// methods (Listener) and an OffsetCommitCallback (CommitCallback) run inside
// the drain of the waiting call that delivers them, and each reports once,
// through __set_callback_result, at once or once its Promise settled
// (Deferred). While a report is pending (`reports`), every call the
// ConsumerHandle has goes through the handle, which the core admits while it
// runs a callback; every other call goes to the consumer, whose single-owner
// flag refuses it. A pending report holds the handle object, so the finalizer
// cannot run before it is made; one the destroy runs is made at once, running
// no JS, and none once the destroy has returned (`destroyed`).

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "util.h"

typedef struct Batch Batch;
typedef struct Listener Listener;
typedef struct CommitCallback CommitCallback;
typedef struct Deferred Deferred;

typedef struct {
    napi_env env;                              // the environment that created it (JS thread)
    kafka_consumer_KafkaConsumer_t* kc;        // owned class handle of a KafkaConsumer, NULL once destroyed
    kafka_consumer_MockConsumer_t* mock;       // owned class handle of a MockConsumer, NULL once destroyed
    kafka_consumer_Consumer_t* view;           // borrowed __as_Consumer view of kc or mock
    kafka_consumer_ConsumerHandle_t* handle;   // owned reentrancy handle, destroyed after the class handle
    MockRecord* kept;                          // the records added to a mock, held until the destroy
    InjectedError* injected;                   // the JS values the core carries, held while it holds a copy
    Listener* listener;                        // the listener the core holds, NULL for none
    CommitCallback* commits;                   // the commit callbacks given the core, until each reported
    Deferred* deferred;                        // the reports waiting for a callback's Promise
    int reports;                               // the listener and commit callback reports not made yet
    int destroyed;                             // the class handle's destroy returned: no report from now on
    CallbackQueue queue;                       // notify hook, tsfn, drain, liveness
    int owners;                                // the handle object, the tsfn and each Deferred
} Consumer;

// A rebalance listener's context, the `self_` of its FFI registration: the
// functions src/ built for its three methods (src/internal/consumer_calls.ts,
// listenerRuns), null for a missing required one, which the FFI refused.
// Held by its subscribe until the core answered it, then by the consumer
// while the core holds it (Consumer.listener), and by each report pending.
struct Listener {
    Consumer* c;
    napi_ref runs[3];                          // onPartitionsRevoked's, onPartitionsAssigned's, onPartitionsLost's
    int refs;
};

// An OffsetCommitCallback's context, the `self_` of its FFI registration: the
// function src/ built for it (src/internal/consumer_calls.ts, commitRun), held
// until it reported (its Deferred's report, for one that returned a Promise),
// or until the destroy.
struct CommitCallback {
    Consumer* c;
    napi_ref run;
    CommitCallback* prev;
    CommitCallback* next;
};

// A report waiting for the Promise a callback returned: its two handlers, each
// owning it until finalized, held strongly until the report, and the handle
// object, held until the report.
struct Deferred {
    Consumer* c;
    int64_t callback_id;                       // stays in C: the core may number past 2^53
    Listener* listener;                        // the listener whose method it is, held until the report
    CommitCallback* commit;                    // or the commit callback, freed by the report
    napi_ref handlers[2];                      // on fulfilled, on rejected
    napi_ref hold;                             // the consumer's handle object
    int functions;                             // the handlers not yet finalized
    int reported;
    Deferred* prev;
    Deferred* next;
};

// The context of a subscribe: its `done`, and the listener it gives the core,
// kept as the consumer's once the core answers that the subscribe registered
// it.
typedef struct {
    Consumer* c;
    napi_ref done;
    Listener* listener;                        // NULL for a subscribe without one
    int registers;                             // a success replaces the consumer's listener
} SubscribeCtx;

// A record's headers until its Headers copied them out: what the batch's
// HeaderSource externals point at.
typedef struct {
    Batch* batch;
    const kafka_consumer_ConsumerRecord_t* record;  // borrowed from the batch's records
} HeaderSource;

// One poll's records, owned until nothing needs them.
struct Batch {
    kafka_consumer_ConsumerRecords_t* records;  // owned
    HeaderSource* sources;                      // one per record that may need its headers
    int32_t source_count;
    int refs;                                   // the build, each external buffer and HeaderSource alive
    int mock;                                   // a mock's poll: its records are the app's own
};

// The context of a call answered by a completion that gives an error or
// nothing, `done(error | null)` (void_on_complete), or an error or a value,
// `done(error | null, value | null)` (value_settle).
typedef struct {
    Consumer* c;
    napi_ref done;
} CallCtx;

typedef struct {
    Consumer* c;
    napi_ref done;                             // JS (error | null, partitions, nextOffsets) => void
    napi_ref factory;                          // JS ConsumerRecord factory
} PollCtx;

typedef struct {
    Consumer* c;
    napi_ref done;                             // JS (error | null, closedNow) => void
} CloseCtx;

// The type tag of a HeaderSource external, so no other value is read as one.
static const napi_type_tag SOURCE_TAG = {0x6b61666b61687372ULL, 0x6373726365303031ULL};

// Test-only (consumerRefuseExternalBuffers): act as if Node-API refused
// external buffers.
static atomic_int refuse_external;

// The counts consumerStats() reads, for the memory tests. Test-only.
static atomic_long stat_consumers_made;
static atomic_long stat_consumers_freed;
static atomic_long stat_batches_made;
static atomic_long stat_batches_destroyed;
static atomic_long stat_buffers_made;
static atomic_long stat_buffers_freed;
static atomic_long stat_sources_made;
static atomic_long stat_sources_freed;
static atomic_long stat_copies;
static atomic_long stat_listeners_made;
static atomic_long stat_listeners_freed;
static atomic_long stat_commit_callbacks_made;
static atomic_long stat_commit_callbacks_freed;
static atomic_long stat_deferrals_made;
static atomic_long stat_deferrals_freed;

static void consumer_release(Consumer* c) {
    if (--c->owners != 0) return;
    cbq_dispose(&c->queue);
    free(c);
    atomic_fetch_add(&stat_consumers_freed, 1);
}

static int consumer_alive(const Consumer* c) {
    return c->kc != NULL || c->mock != NULL;
}

static int is_function(napi_env env, napi_value v) {
    napi_valuetype type;
    return napi_typeof(env, v, &type) == napi_ok && type == napi_function;
}

static Consumer* unwrap_consumer(napi_env env, napi_value handle) {
    void* data = NULL;
    if (napi_unwrap(env, handle, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "not a consumer handle");
        return NULL;
    }
    Consumer* c = (Consumer*)data;
    // Unreachable from JS: only the handle object's finalizer destroys the
    // consumer. A guard of memory, not of the closed state.
    if (!consumer_alive(c)) {
        napi_throw_error(env, NULL, "the consumer has been destroyed");
        return NULL;
    }
    return c;
}

// The mock behind `c`, which a mock's call takes, or NULL with a TypeError
// pending for a handle that is no mock's. Each mock call first lets go of the
// errors JS gave the mock that the core holds no copy of any more.
static kafka_consumer_MockConsumer_t* mock_of(napi_env env, Consumer* c) {
    if (c == NULL) return NULL;
    if (c->mock == NULL) {
        napi_throw_type_error(env, NULL, "not a mock consumer handle");
        return NULL;
    }
    injected_errors_sweep(env, &c->injected);
    return c->mock;
}

// ---- the listeners' and commit callbacks' reports ---------------------------

static void listener_release(napi_env env, Listener* l) {
    if (l == NULL || --l->refs != 0) return;
    for (int i = 0; i < 3; i++) delete_ref(env, &l->runs[i]);
    free(l);
    atomic_fetch_add(&stat_listeners_freed, 1);
}

// Frees a commit callback's context: once it reported, or at the destroy.
static void commit_callback_free(napi_env env, CommitCallback* cb) {
    if (cb == NULL) return;
    Consumer* c = cb->c;
    if (cb->prev != NULL) cb->prev->next = cb->next;
    else c->commits = cb->next;
    if (cb->next != NULL) cb->next->prev = cb->prev;
    delete_ref(env, &cb->run);
    free(cb);
    atomic_fetch_add(&stat_commit_callbacks_freed, 1);
}

// The reentrancy handle a call goes through while a listener's or commit
// callback's report is pending (the window), or NULL: then the call goes to
// the consumer.
static const kafka_consumer_ConsumerHandle_t* window_of(const Consumer* c) {
    return c->reports > 0 ? c->handle : NULL;
}

// The owned error a callback's failure the binding could not carry is
// reported with (`text` what failed).
static kafka_common_Error_t* callback_failure(const char* text) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    kafka_common_Error_t* panic = FFI_SET(report, kafka_common_Error_local_callback_err(text, NULL, NULL, &ffi_err));
    if (report == NULL) return panic;
    error_discard(panic);
    error_stat_made(report);
    return report;
}

// The owned error a callback's thrown or rejected `value` is reported with: a
// LocalCallbackError carrying it (native/mock_values.c), so the value comes
// back as the same object wherever the core reports it.
static kafka_common_Error_t* callback_value_error(napi_env env, Consumer* c, napi_value value) {
    kafka_common_Error_t* report = NULL;
    if (injected_error_carry(env, &c->injected, value, &report) == 0) return report;
    js_warn_thrown(env);
    return callback_failure("the callback's error could not be kept");
}

static void deferred_unlink(Deferred* d) {
    Consumer* c = d->c;
    if (d->prev != NULL) d->prev->next = d->next;
    else c->deferred = d->next;
    if (d->next != NULL) d->next->prev = d->prev;
    d->prev = NULL;
    d->next = NULL;
}

// The one report of a callback whose Promise settled: `rejected` with the
// value `reason`, or fulfilled. A no-op once reported, and dropped once the
// destroy has returned (JS conventions, Threads and callbacks). `js` is false
// in a finalizer, where no JS runs: the report is a fulfilled one, and the
// handlers' references are the runtime's to drop.
static void deferred_report(napi_env env, Deferred* d, int rejected, napi_value reason, int js) {
    if (d->reported) return;
    d->reported = 1;
    deferred_unlink(d);
    Consumer* c = d->c;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = js && rejected && !c->destroyed ? callback_value_error(env, c, reason) : NULL;
    if (!c->destroyed) {
        error_stat_given(report);
        error_warn(env, js, FFI_ERR(kafka_consumer_Consumer__set_callback_result_err(c->view, d->callback_id, NULL, report, &ffi_err)));
    } else {
        error_discard(report);
    }
    c->reports--;
    if (js) {
        delete_ref(env, &d->handlers[0]);
        delete_ref(env, &d->handlers[1]);
        delete_ref(env, &d->hold);
    }
    listener_release(env, d->listener);
    d->listener = NULL;
    // A commit callback is invoked once: it reported.
    commit_callback_free(env, d->commit);
    d->commit = NULL;
}

// Reports every callback still waiting for its Promise as fulfilled, before
// the destroy, which waits for their reports: no JS will settle them once the
// handle object is finalized.
static void deferred_report_all(napi_env env, Consumer* c) {
    while (c->deferred != NULL) deferred_report(env, c->deferred, 0, NULL, 0);
}

static napi_value js_callback_fulfilled(napi_env env, napi_callback_info info) {
    void* data = NULL;
    if (napi_get_cb_info(env, info, NULL, NULL, NULL, &data) == napi_ok && data != NULL)
        deferred_report(env, (Deferred*)data, 0, NULL, 1);
    return NULL;
}

static napi_value js_callback_rejected(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1] = {NULL};
    void* data = NULL;
    if (napi_get_cb_info(env, info, &argc, argv, NULL, &data) != napi_ok || data == NULL) return NULL;
    if (argv[0] == NULL && napi_get_undefined(env, &argv[0]) != napi_ok) return NULL;
    deferred_report(env, (Deferred*)data, 1, argv[0], 1);
    return NULL;
}

// A handler's finalizer. It is held strongly until the report, so it runs
// before one only at an environment teardown, when no JS will settle the
// Promise: the report is then made as fulfilled, so the call waiting for it
// ends. The last one frees the context and its unit of the consumer struct.
static void deferred_finalize(napi_env env, void* data, void* hint) {
    Deferred* d = (Deferred*)data;
    // The reference to this very handler is the runtime's to drop.
    d->handlers[(intptr_t)hint] = NULL;
    deferred_report(env, d, 0, NULL, 0);
    if (--d->functions != 0) return;
    Consumer* c = d->c;
    free(d);
    atomic_fetch_add(&stat_deferrals_freed, 1);
    consumer_release(c);
}

// Defers the report of the callback whose JS side returned `promise`, a
// Promise src/ made (Promise.resolve, or Promise.reject for a throw), until it
// settles: `promise.then(onFulfilled, onRejected)`, at once, so no rejection
// is left unhandled. Returns -1, nothing deferred, when a Node-API call failed
// before a handler was registered; the report is then made at once.
static int deferred_new(napi_env env, Consumer* c, napi_value promise, int64_t callback_id, Listener* l, CommitCallback* cb) {
    Deferred* d = (Deferred*)calloc(1, sizeof(Deferred));
    napi_value then, fns[2], obj, ignored;
    if (d == NULL || napi_get_named_property(env, promise, "then", &then) != napi_ok
        || napi_create_function(env, "onFulfilled", NAPI_AUTO_LENGTH, js_callback_fulfilled, d, &fns[0]) != napi_ok
        || napi_create_function(env, "onRejected", NAPI_AUTO_LENGTH, js_callback_rejected, d, &fns[1]) != napi_ok
        || napi_get_reference_value(env, c->queue.self_ref, &obj) != napi_ok || obj == NULL
        || napi_create_reference(env, obj, 1, &d->hold) != napi_ok
        || napi_create_reference(env, fns[0], 1, &d->handlers[0]) != napi_ok
        || napi_create_reference(env, fns[1], 1, &d->handlers[1]) != napi_ok) {
        if (d != NULL) {
            delete_ref(env, &d->hold);
            delete_ref(env, &d->handlers[0]);
            delete_ref(env, &d->handlers[1]);
            free(d);
        }
        js_warn_thrown(env);
        return -1;
    }
    d->c = c;
    d->callback_id = callback_id;
    d->listener = l;
    if (l != NULL) l->refs++;
    d->commit = cb;
    d->next = c->deferred;
    if (c->deferred != NULL) c->deferred->prev = d;
    c->deferred = d;
    c->owners++;
    atomic_fetch_add(&stat_deferrals_made, 1);
    for (intptr_t i = 0; i < 2; i++) {
        if (napi_add_finalizer(env, fns[i], d, deferred_finalize, (void*)i, NULL) == napi_ok) {
            d->functions++;
        } else {
            // This handler frees nothing: its reference goes.
            delete_ref(env, &d->handlers[i]);
            js_warn_thrown(env);
        }
    }
    if (d->functions == 0 || napi_call_function(env, promise, then, 2, fns, &ignored) != napi_ok) {
        // Never called back: reported now as a failure.
        js_warn_thrown(env);
        napi_value reason = NULL;
        napi_get_undefined(env, &reason);
        deferred_report(env, d, 1, reason, 1);
    }
    if (d->functions == 0) {
        free(d);
        atomic_fetch_add(&stat_deferrals_freed, 1);
        c->owners--;
    }
    return 0;
}

// What a listener method's or commit callback's JS side answered, `result`
// from call_ref (NULL when src/ failed, `thrown` its text): 1 when its report
// waits for the Promise it returned (deferred_new), else 0, `*report` the
// error to report, NULL for none.
static int callback_answer(napi_env env, Consumer* c, napi_value result, char* thrown, int64_t callback_id, Listener* l,
                           CommitCallback* cb, kafka_common_Error_t** report) {
    *report = NULL;
    if (result == NULL) {
        *report = callback_failure(thrown != NULL ? thrown : "the callback could not be run");
        free(thrown);
        return 0;
    }
    free(thrown);
    bool is_promise = false;
    if (napi_is_promise(env, result, &is_promise) != napi_ok || !is_promise) return 0;
    if (deferred_new(env, c, result, callback_id, l, cb) == 0) return 1;
    *report = callback_failure("the callback's Promise could not be awaited");
    return 0;
}

// The run the listener's three methods hand their invocation to: it runs the
// method's JS side once, with the partitions as [topic, partition] pairs read
// from the borrowed, sorted list, then reports once, at once or once the
// Promise it returned settled. A run the destroy makes runs no JS and reports
// at once.
static void listener_invoke(Listener* l, int method, const kafka_List_t* partitions, int64_t callback_id) {
    Consumer* c = l->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    // The window opens: a call made until the report goes through the handle.
    c->reports++;
    if (!c->queue.finalizing) {
        injected_errors_sweep(env, &c->injected);
        char* thrown = NULL;
        napi_value pairs = tp_list_to_js(env, partitions);
        napi_value result = pairs != NULL ? call_ref(env, l->runs[method], 1, &pairs, &thrown) : NULL;
        if (pairs == NULL) js_warn_thrown(env);
        if (callback_answer(env, c, result, thrown, callback_id, l, NULL, &report) == 1) return;
    }
    if (!c->destroyed) {
        error_stat_given(report);
        error_warn(env, !c->queue.finalizing,
                   FFI_ERR(kafka_consumer_Consumer__set_callback_result_err(c->view, callback_id, NULL, report, &ffi_err)));
    } else {
        error_discard(report);
    }
    c->reports--;
}

static void listener_on_revoked(void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    listener_invoke((Listener*)self_, 0, partitions, callback_id);
}

static void listener_on_assigned(void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    listener_invoke((Listener*)self_, 1, partitions, callback_id);
}

static void listener_on_lost(void* self_, const kafka_List_t* partitions, int64_t callback_id) {
    listener_invoke((Listener*)self_, 2, partitions, callback_id);
}

// The listener `runs` (src/'s three functions, the first two null for a
// missing required method) as a context and its FFI registration, *handle,
// which the subscribe copies: a missing required method goes as NULL, which
// the FFI refuses with its LocalNullPointerError, thrown as a TypeError.
// NULL with a JS exception pending on failure.
static Listener* listener_new(napi_env env, Consumer* c, napi_value runs, kafka_consumer_ConsumerRebalanceListener_t** handle) {
    *handle = NULL;
    Listener* l = (Listener*)calloc(1, sizeof(Listener));
    if (l == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    l->c = c;
    l->refs = 1;
    atomic_fetch_add(&stat_listeners_made, 1);
    for (uint32_t i = 0; i < 3; i++) {
        napi_value fn;
        if (napi_get_element(env, runs, i, &fn) != napi_ok || (is_function(env, fn) && napi_create_reference(env, fn, 1, &l->runs[i]) != napi_ok)) {
            listener_release(env, l);
            js_throw_last_napi_error(env, "reading the listener");
            return NULL;
        }
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_SET(*handle, kafka_consumer_ConsumerRebalanceListener_new_err(
                                                     l, l->runs[0] != NULL ? listener_on_revoked : NULL,
                                                     l->runs[1] != NULL ? listener_on_assigned : NULL, listener_on_lost, &ffi_err));
    if (err != NULL) {
        listener_release(env, l);
        error_throw_owned(env, err);
        return NULL;
    }
    return l;
}

// Destroys the FFI registration of a listener once the subscribe copied it.
static void listener_handle_free(napi_env env, kafka_consumer_ConsumerRebalanceListener_t* handle) {
    kafka_common_Error_t* ffi_err = NULL;
    if (handle != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerRebalanceListener_destroy_err(handle, &ffi_err)));
}

// An OffsetCommitCallback's on_complete: it runs the callback's JS side once,
// with the offsets' entries as offset_map_to_js reads the borrowed map and the
// borrowed error as JS, or null, then reports once, at once or once the
// Promise it returned settled, and the context is freed with the report. A
// run the destroy makes runs no JS and reports at once.
static void commit_on_complete(void* self_, const kafka_Map_t* offsets, const kafka_common_Error_t* error, int64_t callback_id) {
    CommitCallback* cb = (CommitCallback*)self_;
    Consumer* c = cb->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* report = NULL;
    // The window opens: a call made until the report goes through the handle.
    c->reports++;
    if (!c->queue.finalizing) {
        injected_errors_sweep(env, &c->injected);
        char* thrown = NULL;
        napi_value argv[2] = {NULL, NULL};
        if (offsets != NULL) argv[0] = offset_map_to_js(env, offsets);
        else napi_get_null(env, &argv[0]);
        if (argv[0] != NULL) argv[1] = error != NULL ? error_to_js(env, error) : NULL;
        if (argv[0] != NULL && error == NULL) napi_get_null(env, &argv[1]);
        napi_value result = argv[0] != NULL && argv[1] != NULL ? call_ref(env, cb->run, 2, argv, &thrown) : NULL;
        if (argv[0] == NULL || argv[1] == NULL) js_warn_thrown(env);
        if (callback_answer(env, c, result, thrown, callback_id, NULL, cb, &report) == 1) return;
    }
    if (!c->destroyed) {
        error_stat_given(report);
        error_warn(env, !c->queue.finalizing,
                   FFI_ERR(kafka_consumer_Consumer__set_callback_result_err(c->view, callback_id, NULL, report, &ffi_err)));
    } else {
        error_discard(report);
    }
    c->reports--;
    commit_callback_free(env, cb);
}

// The commit callback `run` (src/'s function) as a context, on the
// consumer's list, and its FFI registration, *handle, which the commit
// copies. NULL with a JS exception pending on failure.
static CommitCallback* commit_callback_new(napi_env env, Consumer* c, napi_value run, kafka_consumer_OffsetCommitCallback_t** handle) {
    *handle = NULL;
    CommitCallback* cb = (CommitCallback*)calloc(1, sizeof(CommitCallback));
    if (cb == NULL || napi_create_reference(env, run, 1, &cb->run) != napi_ok) {
        free(cb);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    cb->c = c;
    cb->next = c->commits;
    if (c->commits != NULL) c->commits->prev = cb;
    c->commits = cb;
    atomic_fetch_add(&stat_commit_callbacks_made, 1);
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_SET(*handle, kafka_consumer_OffsetCommitCallback_new_err(cb, commit_on_complete, &ffi_err));
    if (err != NULL) {
        commit_callback_free(env, cb);
        error_throw_owned(env, err);
        return NULL;
    }
    return cb;
}

// ---- the batch --------------------------------------------------------------

static void batch_unref(Batch* b) {
    if (--b->refs != 0) return;
    kafka_common_Error_t* ffi_err = NULL;
    // A finalizer may run this, where no JS can run: an error is only freed.
    error_discard(FFI_ERR(kafka_consumer_ConsumerRecords_destroy_err(b->records, &ffi_err)));
    free(b->sources);
    free(b);
    atomic_fetch_add(&stat_batches_destroyed, 1);
}

static void buffer_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)data;
    atomic_fetch_add(&stat_buffers_freed, 1);
    batch_unref((Batch*)hint);
}

static void source_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)data;
    atomic_fetch_add(&stat_sources_freed, 1);
    batch_unref((Batch*)hint);
}

// A key or value as JS: null for Java's null, a Buffer over the batch's bytes,
// or a copy where external buffers are refused (*copied set). NULL with a JS
// exception pending on failure.
static napi_value bytes_to_js(napi_env env, Batch* b, const kafka_Bytes_t* bytes, int* copied) {
    napi_value v = NULL;
    if (bytes == NULL || bytes->data == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &v), NULL);
        return v;
    }
    size_t len = (size_t)bytes->len;
    // An empty key or value holds nothing of the batch: it is a new Buffer.
    if (len > 0 && !*copied) {
        napi_status status = napi_create_external_buffer(env, len, (void*)bytes->data, buffer_finalize, b, &v);
        if (status == napi_ok) {
            b->refs++;
            atomic_fetch_add(&stat_buffers_made, 1);
            return v;
        }
        if (status != napi_no_external_buffers_allowed) {
            js_throw_last_napi_error(env, "napi_create_external_buffer");
            return NULL;
        }
        // Refused once, refused for every record of the batch.
        *copied = 1;
    }
    void* ignored = NULL;
    NAPI_CHECK(env, napi_create_buffer_copy(env, len, bytes->data, &ignored, &v), NULL);
    if (len > 0) atomic_fetch_add(&stat_copies, 1);
    return v;
}

// Whether a record has headers, read through the C API (the FFI has no
// count): only the copy path asks.
static int record_has_headers(const kafka_consumer_ConsumerRecord_t* record) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_header_internals_RecordHeaders_t* headers = NULL;
    error_discard(FFI_SET(headers, kafka_consumer_ConsumerRecord_headers_err(record, &ffi_err)));
    return headers != NULL && headers_count(headers) != 0;
}

// The HeaderSource external of `record`, or null when `copied` and
// `record` has no headers. NULL with a JS exception pending on failure.
static napi_value source_to_js(napi_env env, Batch* b, const kafka_consumer_ConsumerRecord_t* record, int copied) {
    napi_value v = NULL;
    if (copied && !record_has_headers(record)) {
        NAPI_CHECK(env, napi_get_null(env, &v), NULL);
        return v;
    }
    HeaderSource* s = &b->sources[b->source_count];
    s->batch = b;
    s->record = record;
    NAPI_CHECK(env, napi_create_external(env, s, source_finalize, b, &v), NULL);
    b->source_count++;
    b->refs++;
    atomic_fetch_add(&stat_sources_made, 1);
    NAPI_CHECK(env, napi_type_tag_object(env, v, &SOURCE_TAG), NULL);
    return v;
}

// A mock's record as JS: the app's own ConsumerRecord, which the MockRecord
// whose tag the record's key points at holds (js_mock_consumer_add_record).
// NULL with a JS exception pending on failure.
static napi_value mock_record_to_js(napi_env env, const kafka_consumer_ConsumerRecord_t* r) {
    kafka_common_Error_t* ffi_err = NULL;
    const void* key = NULL;
    kafka_common_Error_t* err = FFI_SET(key, kafka_consumer_ConsumerRecord_key_err(r, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    MockRecord* kept = mock_record_of(key);
    napi_value v = NULL;
    if (kept == NULL || kept->record == NULL || napi_get_reference_value(env, kept->record, &v) != napi_ok || v == NULL) {
        napi_throw_error(env, NULL, "a mock's record without the record it was given");
        return NULL;
    }
    return v;
}

// A record as the factory builds it: (topic, partition, offset, timestamp,
// timestampTypeId, serializedKeySize, serializedValueSize, headers, key,
// value, leaderEpoch, deliveryCount), `topic` the partition's one string.
static napi_value record_to_js(napi_env env, Batch* b, napi_value factory, napi_value topic,
                               const kafka_consumer_ConsumerRecord_t* r, int* copied) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t partition = -1, key_size = -1, value_size = -1, leader_epoch = -1, type_id = -1;
    int16_t delivery_count = -1;
    int64_t offset = -1, timestamp = -1;
    const kafka_common_record_TimestampType_t* type = NULL;
    const kafka_Bytes_t* key = NULL;
    const kafka_Bytes_t* value = NULL;
    kafka_common_Error_t* err = FFI_SET(partition, kafka_consumer_ConsumerRecord_partition_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(offset, kafka_consumer_ConsumerRecord_offset_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(timestamp, kafka_consumer_ConsumerRecord_timestamp_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(type, kafka_consumer_ConsumerRecord_timestamp_type_err(r, &ffi_err));
    if (err == NULL && type != NULL) err = FFI_SET(type_id, kafka_common_record_TimestampType_id_err(type, &ffi_err));
    if (err == NULL) err = FFI_SET(key_size, kafka_consumer_ConsumerRecord_serialized_key_size_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(value_size, kafka_consumer_ConsumerRecord_serialized_value_size_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(key, (const kafka_Bytes_t*)kafka_consumer_ConsumerRecord_key_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(value, (const kafka_Bytes_t*)kafka_consumer_ConsumerRecord_value_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(leader_epoch, kafka_consumer_ConsumerRecord_leader_epoch_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(delivery_count, kafka_consumer_ConsumerRecord_delivery_count_err(r, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value argv[12];
    argv[0] = topic;
    // The key and value first: the copy path, once taken, decides whether the
    // headers are looked at.
    if ((argv[8] = bytes_to_js(env, b, key, copied)) == NULL || (argv[9] = bytes_to_js(env, b, value, copied)) == NULL
        || (argv[7] = source_to_js(env, b, r, *copied)) == NULL)
        return NULL;
    if (napi_create_int32(env, partition, &argv[1]) != napi_ok || napi_create_bigint_int64(env, offset, &argv[2]) != napi_ok
        || napi_create_bigint_int64(env, timestamp, &argv[3]) != napi_ok || napi_create_int32(env, type_id, &argv[4]) != napi_ok
        || napi_create_int32(env, key_size, &argv[5]) != napi_ok || napi_create_int32(env, value_size, &argv[6]) != napi_ok
        || napi_create_int32(env, leader_epoch, &argv[10]) != napi_ok
        || napi_create_int32(env, delivery_count, &argv[11]) != napi_ok) {
        js_throw_last_napi_error(env, "reading a record");
        return NULL;
    }
    napi_value global, record;
    NAPI_CHECK(env, napi_get_global(env, &global), NULL);
    if (napi_call_function(env, global, factory, 12, argv, &record) != napi_ok) return NULL;
    return record;
}

// A partition's records as [topic, partition, records], each record built by
// `factory`. NULL with a JS exception pending on failure.
static napi_value partition_to_js(napi_env env, Batch* b, napi_value factory, const kafka_common_TopicPartition_t* tp,
                                  int* copied) {
    kafka_common_Error_t* ffi_err = NULL;
    napi_value pair = topic_partition_to_js(env, tp);
    if (pair == NULL) return NULL;
    napi_value topic, records;
    NAPI_CHECK(env, napi_get_element(env, pair, 0, &topic), NULL);
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_consumer_ConsumerRecords_records_with_partition_err(b->records, tp, &ffi_err));
    int32_t n = 0;
    if (err == NULL) err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    int failed = err != NULL;
    if (!failed && napi_create_array_with_length(env, (size_t)n, &records) != napi_ok) {
        js_throw_last_napi_error(env, "napi_create_array_with_length");
        failed = 1;
    }
    for (int32_t i = 0; !failed && i < n; i++) {
        const kafka_consumer_ConsumerRecord_t* r = NULL;
        err = FFI_SET(r, (const kafka_consumer_ConsumerRecord_t*)kafka_List_get_err(list, i, &ffi_err));
        napi_value record = NULL;
        if (err == NULL) record = b->mock ? mock_record_to_js(env, r) : record_to_js(env, b, factory, topic, r, copied);
        if (record == NULL || napi_set_element(env, records, (uint32_t)i, record) != napi_ok) failed = 1;
    }
    // The list borrows its records from the batch.
    if (list != NULL) error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    if (failed) return NULL;
    NAPI_CHECK(env, napi_set_element(env, pair, 2, records), NULL);
    return pair;
}

// The poll's answer as JS, argv[1] the partitions and argv[2] the next
// offsets; the batch now held by what needs it. A mock's batch (`mock`)
// holds the app's own records, so nothing needs it once read: it makes no
// external buffer and no HeaderSource, and is destroyed here. -1 with a JS
// exception pending on failure.
static int batch_to_js(napi_env env, kafka_consumer_ConsumerRecords_t* records, napi_value factory, int mock, napi_value* argv) {
    kafka_common_Error_t* ffi_err = NULL;
    Batch* b = (Batch*)calloc(1, sizeof(Batch));
    int32_t count = 0;
    kafka_common_Error_t* err = b == NULL ? NULL : FFI_SET(count, kafka_consumer_ConsumerRecords_count_err(records, &ffi_err));
    if (b != NULL && err == NULL && count > 0 && !mock) {
        b->sources = (HeaderSource*)calloc((size_t)count, sizeof(HeaderSource));
        if (b->sources == NULL) {
            free(b);
            b = NULL;
        }
    }
    if (b == NULL || err != NULL) {
        free(b);
        error_discard(FFI_ERR(kafka_consumer_ConsumerRecords_destroy_err(records, &ffi_err)));
        if (err != NULL) error_throw_owned(env, err);
        else napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    atomic_fetch_add(&stat_batches_made, 1);
    b->records = records;
    b->mock = mock;
    // The build holds the batch until every record is made.
    b->refs = 1;
    // The test switch takes the copy path from the first record.
    int copied = atomic_load(&refuse_external);
    kafka_List_t* partitions = NULL;
    kafka_Map_t* next = NULL;
    err = FFI_SET(partitions, kafka_consumer_ConsumerRecords_partitions_err(records, &ffi_err));
    if (err == NULL) err = FFI_SET(next, kafka_consumer_ConsumerRecords_next_offsets_err(records, &ffi_err));
    int32_t n = 0;
    if (err == NULL) err = FFI_SET(n, kafka_List_size_err(partitions, &ffi_err));
    int failed = err != NULL;
    if (!failed && napi_create_array_with_length(env, (size_t)n, &argv[1]) != napi_ok) {
        js_throw_last_napi_error(env, "napi_create_array_with_length");
        failed = 1;
    }
    for (int32_t i = 0; !failed && i < n; i++) {
        const kafka_common_TopicPartition_t* tp = NULL;
        err = FFI_SET(tp, (const kafka_common_TopicPartition_t*)kafka_List_get_err(partitions, i, &ffi_err));
        napi_value p = err == NULL ? partition_to_js(env, b, factory, tp, &copied) : NULL;
        if (p == NULL || napi_set_element(env, argv[1], (uint32_t)i, p) != napi_ok) failed = 1;
    }
    // The list owns its TopicPartitions: destroying it frees them.
    if (partitions != NULL) error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(partitions, &ffi_err)));
    if (next != NULL) {
        // [topic, partition, offset, leaderEpoch, metadata] entries.
        argv[2] = failed ? NULL : offset_map_to_js(env, next);
        if (argv[2] == NULL && !failed) {
            js_throw_last_napi_error(env, "reading the next offsets");
            failed = 1;
        }
        error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(next, &ffi_err)));
    }
    if (err != NULL) error_throw_owned(env, err);
    batch_unref(b);
    return failed ? -1 : 0;
}

static void poll_on_complete(kafka_consumer_ConsumerRecords_t* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    PollCtx* ctx = (PollCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    if (c->queue.finalizing) {
        // The destroy runs this: no JS. The records are freed.
        kafka_common_Error_t* ffi_err = NULL;
        if (value != NULL) error_discard(FFI_ERR(kafka_consumer_ConsumerRecords_destroy_err(value, &ffi_err)));
    } else {
        napi_value argv[3] = {NULL, NULL, NULL};
        napi_value factory = NULL;
        if (error != NULL || value == NULL) {
            // A failed poll has no records; any it was given are freed.
            kafka_common_Error_t* ffi_err = NULL;
            if (value != NULL) error_discard(FFI_ERR(kafka_consumer_ConsumerRecords_destroy_err(value, &ffi_err)));
            argv[0] = error != NULL ? error_take_to_js(env, error) : js_take_failure(env, "a poll without records");
            error = NULL;
            if (argv[0] != NULL && (napi_get_null(env, &argv[1]) != napi_ok || napi_get_null(env, &argv[2]) != napi_ok))
                argv[0] = NULL;
        } else if (napi_get_reference_value(env, ctx->factory, &factory) != napi_ok || factory == NULL
                   || batch_to_js(env, value, factory, c->mock != NULL, argv) != 0 || napi_get_null(env, &argv[0]) != napi_ok) {
            // An answer that can't be built rejects the poll with the failure.
            argv[0] = js_take_failure(env, "building a poll's records");
            if (argv[0] != NULL && (napi_get_null(env, &argv[1]) != napi_ok || napi_get_null(env, &argv[2]) != napi_ok))
                argv[0] = NULL;
        }
        if (argv[0] != NULL) call_ref(env, ctx->done, 3, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    delete_ref(env, &ctx->factory);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// consumerPoll(handle, timeoutMs, factory, done): Java's poll(Duration)
// through poll_cb. timeoutMs is a number, or a bigint for a value a number
// does not hold exactly; a negative one goes to the FFI, which rejects it.
// `done(error | null, partitions, nextOffsets)`.
napi_value js_consumer_poll(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4 || !is_function(env, argv[2]) || !is_function(env, argv[3])) {
        napi_throw_type_error(env, NULL, "poll(handle, timeoutMs, factory, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int64_t timeout = 0;
    if (js_int64(env, argv[1], "poll timeoutMs", &timeout) != 0) return NULL;
    PollCtx* ctx = (PollCtx*)calloc(1, sizeof(PollCtx));
    if (ctx == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    ctx->c = c;
    if (napi_create_reference(env, argv[3], 1, &ctx->done) != napi_ok
        || napi_create_reference(env, argv[2], 1, &ctx->factory) != napi_ok) {
        delete_ref(env, &ctx->done);
        free(ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    cbq_call_begin(env, &c->queue);
    kafka_consumer_Consumer_poll_cb(c->view, timeout, poll_on_complete, ctx);
    return NULL;
}

// ---- calls answered with an error or nothing --------------------------------

static void void_on_complete(kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    if (!c->queue.finalizing) {
        napi_value arg = error_take_to_js(env, error);
        error = NULL;
        if (arg == NULL) arg = js_take_failure(env, "converting a consumer call's answer");
        if (arg != NULL) call_ref(env, ctx->done, 1, &arg, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// The context of a call, its `done` the last argument; NULL with a JS
// exception pending.
static CallCtx* call_ctx_new(napi_env env, Consumer* c, napi_value done) {
    CallCtx* ctx = (CallCtx*)calloc(1, sizeof(CallCtx));
    if (ctx == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    ctx->c = c;
    if (napi_create_reference(env, done, 1, &ctx->done) != napi_ok) {
        free(ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    return ctx;
}

// Reads (handle, list, done) of a call taking a list: `list` an array, or
// null, which the FFI reports. NULL with a JS exception pending.
static Consumer* list_call_args(napi_env env, napi_callback_info info, const char* shape, napi_value* argv, int* is_null) {
    size_t argc = 3;
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, shape);
        return NULL;
    }
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    *is_null = type == napi_null;
    return unwrap_consumer(env, argv[0]);
}

// An array of strings (a null element crosses as NULL) as a list borrowing
// the copies in `strings`, which the caller frees after the call.
static kafka_List_t* string_list_from_js(napi_env env, napi_value array, char*** strings, uint32_t* count) {
    uint32_t n = 0;
    *strings = NULL;
    *count = 0;
    NAPI_CHECK(env, napi_get_array_length(env, array, &n), NULL);
    *strings = (char**)calloc((size_t)n + 1, sizeof(char*));
    if (*strings == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    *count = n;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_List_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && i < n; i++) {
        napi_value v;
        napi_valuetype type = napi_undefined;
        if (napi_get_element(env, array, i, &v) != napi_ok || napi_typeof(env, v, &type) != napi_ok) {
            js_throw_last_napi_error(env, "napi_get_element");
            list_free(env, list, NULL);
            return NULL;
        }
        if (type != napi_null && ((*strings)[i] = js_string_dup(env, v, "topics must be strings")) == NULL) {
            list_free(env, list, NULL);
            return NULL;
        }
        err = FFI_ERR(kafka_List_add_err(list, (*strings)[i], &ffi_err));
    }
    if (err != NULL) {
        list_free(env, list, NULL);
        error_throw_owned(env, err);
        return NULL;
    }
    return list;
}

static void strings_free(char** strings, uint32_t count) {
    if (strings == NULL) return;
    for (uint32_t i = 0; i < count; i++) free(strings[i]);
    free(strings);
}

// The subscribe completion: a success the core registered replaces the
// consumer's listener with the subscribe's (none for a subscribe without
// one); a failure, and a KafkaConsumer's empty topic list (unsubscribe()),
// keep the consumer's and let go of the subscribe's, as the FFI's subscribe
// docs give their lifetimes.
static void subscribe_on_complete(kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    SubscribeCtx* ctx = (SubscribeCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    if (error == NULL && ctx->registers) {
        listener_release(env, c->listener);
        c->listener = ctx->listener;
        ctx->listener = NULL;
    }
    listener_release(env, ctx->listener);
    if (!c->queue.finalizing) {
        napi_value arg = error_take_to_js(env, error);
        error = NULL;
        if (arg == NULL) arg = js_take_failure(env, "converting a subscribe's answer");
        if (arg != NULL) call_ref(env, ctx->done, 1, &arg, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// What both subscribes read: (handle, ..., withListener, runs, done), `runs`
// at argv[runs_at], as a subscribe context, its listener (none without
// `withListener`) and the listener's FFI registration, NULL for a null
// listener, which the FFI refuses. NULL with a JS exception pending.
static SubscribeCtx* subscribe_ctx_new(napi_env env, Consumer* c, napi_value* argv, size_t runs_at,
                                       kafka_consumer_ConsumerRebalanceListener_t** handle) {
    bool with_listener = false;
    *handle = NULL;
    NAPI_CHECK(env, napi_get_value_bool(env, argv[runs_at - 1], &with_listener), NULL);
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[runs_at], &type), NULL);
    Listener* l = NULL;
    if (with_listener && type != napi_null && (l = listener_new(env, c, argv[runs_at], handle)) == NULL) return NULL;
    SubscribeCtx* ctx = (SubscribeCtx*)calloc(1, sizeof(SubscribeCtx));
    if (ctx == NULL || napi_create_reference(env, argv[runs_at + 1], 1, &ctx->done) != napi_ok) {
        free(ctx);
        listener_release(env, l);
        listener_handle_free(env, *handle);
        *handle = NULL;
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    ctx->c = c;
    ctx->listener = l;
    ctx->registers = 1;
    return ctx;
}

// consumerSubscribeTopics(handle, topics, withListener, runs, done): Java's
// subscribe(Collection) through subscribe_with_topics_cb, or with
// `withListener` subscribe(Collection, ConsumerRebalanceListener) through
// subscribe_with_topics_listener_cb; `topics` an array of strings, or null;
// `runs` the listener's three methods (listener_new), or null for a null
// listener.
napi_value js_consumer_subscribe_topics(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value argv[5];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 5 || !is_function(env, argv[4])) {
        napi_throw_type_error(env, NULL, "subscribeTopics(handle, topics, withListener, runs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    int is_null = type == napi_null;
    char** strings = NULL;
    uint32_t count = 0;
    kafka_List_t* topics = is_null ? NULL : string_list_from_js(env, argv[1], &strings, &count);
    if (!is_null && topics == NULL) {
        strings_free(strings, count);
        return NULL;
    }
    kafka_consumer_ConsumerRebalanceListener_t* listener = NULL;
    SubscribeCtx* ctx = subscribe_ctx_new(env, c, argv, 3, &listener);
    if (ctx != NULL) {
        // A KafkaConsumer's empty topic list is unsubscribe(): it registers no listener.
        ctx->registers = !(c->kc != NULL && topics != NULL && count == 0);
        bool with_listener = false;
        napi_get_value_bool(env, argv[2], &with_listener);
        if (!with_listener) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_subscribe_with_topics_cb(c->view, topics, subscribe_on_complete, ctx);
        } else {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_subscribe_with_topics_listener_cb(c->view, topics, listener, subscribe_on_complete, ctx);
        }
    }
    // The call copied the listener's registration, and the list borrows the
    // strings, which it copied too.
    listener_handle_free(env, listener);
    list_free(env, topics, NULL);
    strings_free(strings, count);
    return NULL;
}

// consumerSubscribePattern(handle, given, pattern, withListener, runs, done):
// Java's subscribe(SubscriptionPattern) through subscribe_with_pattern_cb, or
// with `withListener` subscribe(SubscriptionPattern,
// ConsumerRebalanceListener) through subscribe_with_pattern_listener_cb:
// `given` false for a null SubscriptionPattern, which crosses as NULL, else
// `pattern` its string, a null one crossing as NULL to
// SubscriptionPattern_new; `runs` as for consumerSubscribeTopics.
napi_value js_consumer_subscribe_pattern(napi_env env, napi_callback_info info) {
    size_t argc = 6;
    napi_value argv[6];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool given = false;
    bool with_listener = false;
    if (argc != 6 || napi_get_value_bool(env, argv[1], &given) != napi_ok || napi_get_value_bool(env, argv[3], &with_listener) != napi_ok
        || !is_function(env, argv[5])) {
        napi_throw_type_error(env, NULL, "subscribePattern(handle, given, pattern, withListener, runs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_consumer_SubscriptionPattern_t* pattern = NULL;
    if (given) {
        napi_valuetype type;
        NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
        char* text = type == napi_null ? NULL : js_string_dup(env, argv[2], "a pattern must be a string");
        if (type != napi_null && text == NULL) return NULL;
        kafka_common_Error_t* err = FFI_SET(pattern, kafka_consumer_SubscriptionPattern_new_err(text, &ffi_err));
        free(text);
        if (err != NULL) {
            error_throw_owned(env, err);
            return NULL;
        }
    }
    kafka_consumer_ConsumerRebalanceListener_t* listener = NULL;
    SubscribeCtx* ctx = subscribe_ctx_new(env, c, argv, 4, &listener);
    if (ctx != NULL && !with_listener) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_subscribe_with_pattern_cb(c->view, pattern, subscribe_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_subscribe_with_pattern_listener_cb(c->view, pattern, listener, subscribe_on_complete, ctx);
    }
    // The call copied the pattern and the listener's registration.
    listener_handle_free(env, listener);
    if (pattern != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_SubscriptionPattern_destroy_err(pattern, &ffi_err)));
    return NULL;
}

typedef enum {
    TP_ASSIGN,
    TP_PAUSE,
    TP_RESUME,
    TP_SEEK_TO_BEGINNING,
    TP_SEEK_TO_END,
    TP_MOCK_REBALANCE,
    TP_MOCK_LOSE_PARTITIONS
} TpCall;

// The calls taking partitions and answered with nothing: an array of [topic,
// partition] (a null element crosses as NULL), or null. The list borrows its
// TopicPartitions, which are destroyed after the call.
static napi_value tp_call(napi_env env, napi_callback_info info, TpCall which, const char* shape) {
    napi_value argv[3];
    int is_null = 0;
    Consumer* c = list_call_args(env, info, shape, argv, &is_null);
    if (c == NULL) return NULL;
    kafka_consumer_MockConsumer_t* mock = NULL;
    if ((which == TP_MOCK_REBALANCE || which == TP_MOCK_LOSE_PARTITIONS) && (mock = mock_of(env, c)) == NULL) return NULL;
    kafka_List_t* partitions = is_null ? NULL : tp_list_from_js(env, argv[1]);
    if (!is_null && partitions == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[2]);
    // assign and the mock's own calls are not the handle's: they go to the consumer.
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && which == TP_PAUSE) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_pause_cb(h, partitions, void_on_complete, ctx);
    } else if (ctx != NULL && h != NULL && which == TP_RESUME) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_resume_cb(h, partitions, void_on_complete, ctx);
    } else if (ctx != NULL && h != NULL && which == TP_SEEK_TO_BEGINNING) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_seek_to_beginning_cb(h, partitions, void_on_complete, ctx);
    } else if (ctx != NULL && h != NULL && which == TP_SEEK_TO_END) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_seek_to_end_cb(h, partitions, void_on_complete, ctx);
    } else if (ctx != NULL) {
        if (which == TP_ASSIGN) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_assign_cb(c->view, partitions, void_on_complete, ctx);
        } else if (which == TP_PAUSE) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_pause_cb(c->view, partitions, void_on_complete, ctx);
        } else if (which == TP_SEEK_TO_BEGINNING) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_seek_to_beginning_cb(c->view, partitions, void_on_complete, ctx);
        } else if (which == TP_SEEK_TO_END) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_seek_to_end_cb(c->view, partitions, void_on_complete, ctx);
        } else if (which == TP_MOCK_REBALANCE) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_MockConsumer_rebalance_cb(mock, partitions, void_on_complete, ctx);
        } else if (which == TP_MOCK_LOSE_PARTITIONS) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_MockConsumer_lose_partitions_cb(mock, partitions, void_on_complete, ctx);
        } else {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_resume_cb(c->view, partitions, void_on_complete, ctx);
        }
    }
    list_free(env, partitions, tp_item_destroy);
    return NULL;
}

// consumerAssign(handle, partitions, done): Java's assign(Collection).
napi_value js_consumer_assign(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_ASSIGN, "assign(handle, partitions, done)");
}

// consumerPause(handle, partitions, done): Java's pause(Collection).
napi_value js_consumer_pause(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_PAUSE, "pause(handle, partitions, done)");
}

// consumerResume(handle, partitions, done): Java's resume(Collection).
napi_value js_consumer_resume(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_RESUME, "resume(handle, partitions, done)");
}

// consumerSeekToBeginning(handle, partitions, done): Java's
// seekToBeginning(Collection).
napi_value js_consumer_seek_to_beginning(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_SEEK_TO_BEGINNING, "seekToBeginning(handle, partitions, done)");
}

// consumerSeekToEnd(handle, partitions, done): Java's seekToEnd(Collection).
napi_value js_consumer_seek_to_end(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_SEEK_TO_END, "seekToEnd(handle, partitions, done)");
}

// mockConsumerRebalance(handle, partitions, done): Java's
// MockConsumer#rebalance(Collection), through rebalance_cb.
napi_value js_mock_consumer_rebalance(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_MOCK_REBALANCE, "mockConsumerRebalance(handle, partitions, done)");
}

// mockConsumerLosePartitions(handle, partitions, done): Java's
// MockConsumer#losePartitions(Collection), through lose_partitions_cb.
napi_value js_mock_consumer_lose_partitions(napi_env env, napi_callback_info info) {
    return tp_call(env, info, TP_MOCK_LOSE_PARTITIONS, "mockConsumerLosePartitions(handle, partitions, done)");
}

// consumerUnsubscribe(handle, done): Java's unsubscribe().
napi_value js_consumer_unsubscribe(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2 || !is_function(env, argv[1])) {
        napi_throw_type_error(env, NULL, "unsubscribe(handle, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[1]);
    if (ctx == NULL) return NULL;
    cbq_call_begin(env, &c->queue);
    kafka_consumer_Consumer_unsubscribe_cb(c->view, void_on_complete, ctx);
    return NULL;
}

// ---- the getters, wakeup() and metrics() ------------------------------------

static Consumer* handle_arg(napi_env env, napi_callback_info info, const char* shape) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, shape);
        return NULL;
    }
    return unwrap_consumer(env, argv[0]);
}

// consumerAssignment(handle): Java's assignment(), as [topic, partition]
// pairs.
napi_value js_consumer_assignment(napi_env env, napi_callback_info info) {
    Consumer* c = handle_arg(env, info, "assignment(handle)");
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    kafka_common_Error_t* err = h != NULL ? FFI_SET(list, kafka_consumer_ConsumerHandle_assignment_err(h, &ffi_err))
                                          : FFI_SET(list, kafka_consumer_Consumer_assignment(c->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return tp_list_take_to_js(env, list);
}

// consumerPaused(handle): Java's paused(), as [topic, partition] pairs.
napi_value js_consumer_paused(napi_env env, napi_callback_info info) {
    Consumer* c = handle_arg(env, info, "paused(handle)");
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    kafka_common_Error_t* err = h != NULL ? FFI_SET(list, kafka_consumer_ConsumerHandle_paused_err(h, &ffi_err))
                                          : FFI_SET(list, kafka_consumer_Consumer_paused(c->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return tp_list_take_to_js(env, list);
}

// consumerSubscription(handle): Java's subscription(), as an array of
// strings.
napi_value js_consumer_subscription(napi_env env, napi_callback_info info) {
    Consumer* c = handle_arg(env, info, "subscription(handle)");
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_consumer_Consumer_subscription(c->view, &ffi_err));
    int32_t n = 0;
    if (err == NULL) err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    napi_value arr = NULL;
    if (err == NULL && napi_create_array_with_length(env, (size_t)n, &arr) != napi_ok) {
        js_throw_last_napi_error(env, "napi_create_array_with_length");
        arr = NULL;
    }
    for (int32_t i = 0; err == NULL && arr != NULL && i < n; i++) {
        const char* topic = NULL;
        napi_value v;
        err = FFI_SET(topic, (const char*)kafka_List_get_err(list, i, &ffi_err));
        if (err == NULL
            && (napi_create_string_utf8(env, topic != NULL ? topic : "", NAPI_AUTO_LENGTH, &v) != napi_ok
                || napi_set_element(env, arr, (uint32_t)i, v) != napi_ok)) {
            js_throw_last_napi_error(env, "napi_set_element");
            arr = NULL;
        }
    }
    // The list owns its strings: destroying it frees them.
    list_free(env, list, NULL);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return arr;
}

// consumerWakeup(handle): Java's wakeup().
napi_value js_consumer_wakeup(napi_env env, napi_callback_info info) {
    Consumer* c = handle_arg(env, info, "wakeup(handle)");
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    kafka_common_Error_t* err = h != NULL ? FFI_ERR(kafka_consumer_ConsumerHandle_wakeup_err(h, &ffi_err))
                                          : FFI_ERR(kafka_consumer_Consumer_wakeup_err(c->view, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// consumerMetrics(handle): Java's metrics(), the live view, which stays valid
// after the consumer is destroyed.
napi_value js_consumer_metrics(napi_env env, napi_callback_info info) {
    Consumer* c = handle_arg(env, info, "metrics(handle)");
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_metrics_MetricsView_t* view = NULL;
    kafka_common_Error_t* err = FFI_SET(view, kafka_consumer_Consumer_metrics_err(c->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return metrics_view_take_to_js(env, view);
}

// ---- commits, committed offsets and the group metadata ---------------------

// Reads the optional timeout of a timed form: null for the form without one,
// else its milliseconds (a negative one goes to the FFI, which rejects it).
// 0 with *timed set, or -1 with a JS exception pending.
static int timeout_arg(napi_env env, napi_value value, const char* what, int* timed, int64_t* out) {
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, value, &type), -1);
    *timed = type != napi_null;
    *out = 0;
    return *timed ? js_int64(env, value, what, out) : 0;
}

// consumerCommitSync(handle, withOffsets, offsets, timeoutMs, done): Java's
// four commitSync forms through their `_cb` twins: commitSync() and
// commitSync(Duration) when `withOffsets` is false, else the forms taking
// `offsets`, an array of [[topic, partition], [offset, leaderEpoch (-1 for
// none), metadata] | null] entries, or null, which the FFI reports. timeoutMs
// null for the forms without a timeout.
napi_value js_consumer_commit_sync(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value argv[5];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool with_offsets = false;
    if (argc != 5 || napi_get_value_bool(env, argv[1], &with_offsets) != napi_ok || !is_function(env, argv[4])) {
        napi_throw_type_error(env, NULL, "commitSync(handle, withOffsets, offsets, timeoutMs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[3], "commitSync timeoutMs", &timed, &timeout) != 0) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    kafka_Map_t* offsets = NULL;
    if (with_offsets && type != napi_null && (offsets = offset_map_from_js(env, argv[2])) == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[4]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL) {
        if (!with_offsets && !timed) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_sync_cb(h, void_on_complete, ctx);
        } else if (!with_offsets) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_sync_with_timeout_cb(h, timeout, void_on_complete, ctx);
        } else if (!timed) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_sync_with_offsets_cb(h, offsets, void_on_complete, ctx);
        } else {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_sync_with_offsets_timeout_cb(h, offsets, timeout, void_on_complete, ctx);
        }
    } else if (ctx != NULL) {
        if (!with_offsets && !timed) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_sync_cb(c->view, void_on_complete, ctx);
        } else if (!with_offsets) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_sync_with_timeout_cb(c->view, timeout, void_on_complete, ctx);
        } else if (!timed) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_sync_with_offsets_cb(c->view, offsets, void_on_complete, ctx);
        } else {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_sync_with_offsets_timeout_cb(c->view, offsets, timeout, void_on_complete, ctx);
        }
    }
    // The call copied the offsets.
    offset_map_free(env, offsets);
    return NULL;
}

// consumerCommitAsync(handle, form, offsets, run, done): Java's
// commitAsync() (form 0) through commit_async_cb, commitAsync(callback) (form
// 1) through commit_async_with_callback_cb, and commitAsync(offsets, callback)
// (form 2) through commit_async_with_offsets_callback_cb, settling once the
// core took the commit: `offsets` the entries commitSync takes, or null,
// which the FFI reports; `run` src/'s function for the callback, or null for
// form 2's null callback, which the FFI refuses.
napi_value js_consumer_commit_async(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value argv[5];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    int32_t form = 0;
    if (argc != 5 || napi_get_value_int32(env, argv[1], &form) != napi_ok || form < 0 || form > 2 || !is_function(env, argv[4])) {
        napi_throw_type_error(env, NULL, "commitAsync(handle, form, offsets, run, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    kafka_Map_t* offsets = NULL;
    if (form == 2 && type != napi_null && (offsets = offset_map_from_js(env, argv[2])) == NULL) return NULL;
    kafka_consumer_OffsetCommitCallback_t* callback = NULL;
    if (form != 0 && is_function(env, argv[3]) && commit_callback_new(env, c, argv[3], &callback) == NULL) {
        offset_map_free(env, offsets);
        return NULL;
    }
    CallCtx* ctx = call_ctx_new(env, c, argv[4]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL) {
        if (form == 0) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_async_cb(h, void_on_complete, ctx);
        } else if (form == 1) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_async_with_callback_cb(h, callback, void_on_complete, ctx);
        } else {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_ConsumerHandle_commit_async_with_offsets_callback_cb(h, offsets, callback, void_on_complete, ctx);
        }
    } else if (ctx != NULL) {
        if (form == 0) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_async_cb(c->view, void_on_complete, ctx);
        } else if (form == 1) {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_async_with_callback_cb(c->view, callback, void_on_complete, ctx);
        } else {
            cbq_call_begin(env, &c->queue);
            kafka_consumer_Consumer_commit_async_with_offsets_callback_cb(c->view, offsets, callback, void_on_complete, ctx);
        }
    }
    // The call copied the offsets and the callback's registration; the
    // callback's context stays until it reported, or the destroy.
    kafka_common_Error_t* ffi_err = NULL;
    if (callback != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_OffsetCommitCallback_destroy_err(callback, &ffi_err)));
    offset_map_free(env, offsets);
    return NULL;
}

// Calls `done(error, null)` for an owned error, or `done(null, value)`, where
// `value` is NULL when the answer could not be built (a JS exception pending),
// which settles the call with the failure. Frees the error; the caller ends
// the call's count.
static void value_settle(napi_env env, CallCtx* ctx, kafka_common_Error_t* error, napi_value value, const char* what) {
    napi_value argv[2] = {NULL, NULL};
    if (error != NULL) {
        argv[0] = error_take_to_js(env, error);
        if (argv[0] == NULL) argv[0] = js_take_failure(env, what);
        if (argv[0] != NULL && napi_get_null(env, &argv[1]) != napi_ok) argv[0] = NULL;
    } else if (value == NULL) {
        argv[0] = js_take_failure(env, what);
        if (argv[0] != NULL && napi_get_null(env, &argv[1]) != napi_ok) argv[0] = NULL;
    } else if (napi_get_null(env, &argv[0]) == napi_ok) {
        argv[1] = value;
    }
    if (argv[0] != NULL && argv[1] != NULL) call_ref(env, ctx->done, 2, argv, NULL);
    else js_warn_thrown(env);
}

// The committed_cb completion: the owned map of TopicPartition to
// OffsetAndMetadata, a NULL value for a partition with no committed offset,
// as offset_map_to_js reads it; the map freed.
static void committed_on_complete(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (!c->queue.finalizing) {
        napi_value entries = error == NULL && value != NULL ? offset_map_to_js(env, value) : NULL;
        if (entries == NULL && error == NULL) js_throw_last_napi_error(env, "reading the committed offsets");
        value_settle(env, ctx, error, entries, "converting a committed() answer");
        error = NULL;
    }
    if (value != NULL) error_warn(env, !c->queue.finalizing, FFI_ERR(kafka_Map_destroy_err(value, &ffi_err)));
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// consumerCommitted(handle, partitions, timeoutMs, done): Java's committed(Set)
// and committed(Set, Duration); `partitions` an array of [topic, partition]
// (a null element crosses as NULL), or null, which the FFI reports.
// `done(error | null, entries | null)`, the entries as offset_map_to_js gives
// them.
napi_value js_consumer_committed(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4 || !is_function(env, argv[3])) {
        napi_throw_type_error(env, NULL, "committed(handle, partitions, timeoutMs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[2], "committed timeoutMs", &timed, &timeout) != 0) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    kafka_List_t* partitions = type == napi_null ? NULL : tp_list_from_js(env, argv[1]);
    if (type != napi_null && partitions == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[3]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_committed_cb(h, partitions, committed_on_complete, ctx);
    } else if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_committed_with_timeout_cb(h, partitions, timeout, committed_on_complete, ctx);
    } else if (ctx != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_committed_cb(c->view, partitions, committed_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_committed_with_timeout_cb(c->view, partitions, timeout, committed_on_complete, ctx);
    }
    list_free(env, partitions, tp_item_destroy);
    return NULL;
}

// consumerGroupMetadata(handle): Java's groupMetadata() through its fallible
// form, as [groupId, generationId, memberId, groupInstanceId (null for none)].
napi_value js_consumer_group_metadata(napi_env env, napi_callback_info info) {
    Consumer* c = handle_arg(env, info, "groupMetadata(handle)");
    if (c == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_consumer_ConsumerGroupMetadata_t* m = NULL;
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    kafka_common_Error_t* err = h != NULL ? FFI_SET(m, kafka_consumer_ConsumerHandle_group_metadata_err(h, &ffi_err))
                                          : FFI_SET(m, kafka_consumer_Consumer_group_metadata(c->view, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    const char* group_id = NULL;
    const char* member_id = NULL;
    const char* instance_id = NULL;
    int32_t generation = -1;
    // The strings are borrowed from the metadata, read before it is freed.
    err = FFI_SET(group_id, kafka_consumer_ConsumerGroupMetadata_group_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(generation, kafka_consumer_ConsumerGroupMetadata_generation_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(member_id, kafka_consumer_ConsumerGroupMetadata_member_id_err(m, &ffi_err));
    if (err == NULL) err = FFI_SET(instance_id, kafka_consumer_ConsumerGroupMetadata_group_instance_id_err(m, &ffi_err));
    napi_value arr = NULL, v;
    if (err == NULL) {
        if (napi_create_array_with_length(env, 4, &arr) != napi_ok
            || napi_create_string_utf8(env, group_id != NULL ? group_id : "", NAPI_AUTO_LENGTH, &v) != napi_ok
            || napi_set_element(env, arr, 0, v) != napi_ok || napi_create_int32(env, generation, &v) != napi_ok
            || napi_set_element(env, arr, 1, v) != napi_ok
            || napi_create_string_utf8(env, member_id != NULL ? member_id : "", NAPI_AUTO_LENGTH, &v) != napi_ok
            || napi_set_element(env, arr, 2, v) != napi_ok
            || (instance_id != NULL ? napi_create_string_utf8(env, instance_id, NAPI_AUTO_LENGTH, &v) : napi_get_null(env, &v)) != napi_ok
            || napi_set_element(env, arr, 3, v) != napi_ok) {
            js_throw_last_napi_error(env, "reading the group metadata");
            arr = NULL;
        }
    }
    error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerGroupMetadata_destroy_err(m, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    return arr;
}

// ---- seeks, positions and lag -----------------------------------------------

// A [topic, partition] argument as an owned TopicPartition, or NULL for null,
// which the FFI reports. 0, or -1 with a JS exception pending.
static int partition_arg(napi_env env, napi_value value, kafka_common_TopicPartition_t** out) {
    napi_valuetype type;
    *out = NULL;
    NAPI_CHECK(env, napi_typeof(env, value, &type), -1);
    if (type == napi_null) return 0;
    *out = tp_from_js(env, value);
    return *out == NULL ? -1 : 0;
}

static void partition_free(napi_env env, kafka_common_TopicPartition_t* tp) {
    kafka_common_Error_t* ffi_err = NULL;
    if (tp != NULL) error_warn(env, 1, FFI_ERR(kafka_common_TopicPartition_destroy_err(tp, &ffi_err)));
}

// consumerSeek(handle, partition, withMetadata, offset, done): Java's
// seek(TopicPartition, long) through seek_with_offset_cb when `withMetadata`
// is false, `offset` a bigint or a number, else seek(TopicPartition,
// OffsetAndMetadata) through seek_with_offset_and_metadata_cb, `offset` the
// OffsetAndMetadata's [offset, leaderEpoch (-1 for none), metadata], or
// null, which the FFI reports. A null partition crosses as NULL too.
napi_value js_consumer_seek(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value argv[5];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool with_metadata = false;
    if (argc != 5 || napi_get_value_bool(env, argv[2], &with_metadata) != napi_ok || !is_function(env, argv[4])) {
        napi_throw_type_error(env, NULL, "seek(handle, partition, withMetadata, offset, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int64_t offset = 0;
    kafka_consumer_OffsetAndMetadata_t* om = NULL;
    if (!with_metadata) {
        if (js_int64(env, argv[3], "seek offset", &offset) != 0) return NULL;
    } else {
        napi_valuetype type;
        NAPI_CHECK(env, napi_typeof(env, argv[3], &type), NULL);
        if (type != napi_null && (om = offset_and_metadata_from_js(env, argv[3])) == NULL) return NULL;
    }
    kafka_common_TopicPartition_t* tp = NULL;
    if (partition_arg(env, argv[1], &tp) != 0) {
        offset_and_metadata_free(env, om);
        return NULL;
    }
    CallCtx* ctx = call_ctx_new(env, c, argv[4]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && !with_metadata) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_seek_with_offset_cb(h, tp, offset, void_on_complete, ctx);
    } else if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_seek_with_offset_and_metadata_cb(h, tp, om, void_on_complete, ctx);
    } else if (ctx != NULL && !with_metadata) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_seek_with_offset_cb(c->view, tp, offset, void_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_seek_with_offset_and_metadata_cb(c->view, tp, om, void_on_complete, ctx);
    }
    // The call copied the partition and the offset.
    partition_free(env, tp);
    offset_and_metadata_free(env, om);
    return NULL;
}

// The position_cb completion: `done(error | null, position | null)`, the
// position a bigint.
static void position_on_complete(int64_t value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    if (!c->queue.finalizing) {
        napi_value position = NULL;
        if (error == NULL && napi_create_bigint_int64(env, value, &position) != napi_ok) {
            js_throw_last_napi_error(env, "napi_create_bigint_int64");
            position = NULL;
        }
        value_settle(env, ctx, error, position, "converting a position() answer");
        error = NULL;
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// consumerPosition(handle, partition, timeoutMs, done): Java's
// position(TopicPartition) and position(TopicPartition, Duration).
napi_value js_consumer_position(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4 || !is_function(env, argv[3])) {
        napi_throw_type_error(env, NULL, "position(handle, partition, timeoutMs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[2], "position timeoutMs", &timed, &timeout) != 0) return NULL;
    kafka_common_TopicPartition_t* tp = NULL;
    if (partition_arg(env, argv[1], &tp) != 0) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[3]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_position_cb(h, tp, position_on_complete, ctx);
    } else if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_position_with_timeout_cb(h, tp, timeout, position_on_complete, ctx);
    } else if (ctx != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_position_cb(c->view, tp, position_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_position_with_timeout_cb(c->view, tp, timeout, position_on_complete, ctx);
    }
    partition_free(env, tp);
    return NULL;
}

// The current_lag_cb completion: `done(error | null, lag)`, the lag a bigint,
// or null for Java's empty OptionalLong.
static void current_lag_on_complete(int8_t present, int64_t lag, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    if (!c->queue.finalizing) {
        napi_value value = NULL;
        if (error == NULL
            && (present ? napi_create_bigint_int64(env, lag, &value) : napi_get_null(env, &value)) != napi_ok) {
            js_throw_last_napi_error(env, "reading the lag");
            value = NULL;
        }
        value_settle(env, ctx, error, value, "converting a currentLag() answer");
        error = NULL;
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// consumerCurrentLag(handle, partition, done): Java's
// currentLag(TopicPartition) through current_lag_cb.
napi_value js_consumer_current_lag(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "currentLag(handle, partition, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    kafka_common_TopicPartition_t* tp = NULL;
    if (partition_arg(env, argv[1], &tp) != 0) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[2]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_current_lag_cb(h, tp, current_lag_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_current_lag_cb(c->view, tp, current_lag_on_complete, ctx);
    }
    partition_free(env, tp);
    return NULL;
}

// ---- the offset and metadata queries ----------------------------------------

// The partitions_for_cb completion: the owned list of PartitionInfo as
// partitions_take_to_js reads it, the list freed.
static void partitions_for_on_complete(kafka_List_t* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (!c->queue.finalizing) {
        napi_value partitions = NULL;
        if (value != NULL) {
            partitions = partitions_take_to_js(env, value);
            value = NULL;
        }
        if (partitions == NULL && error == NULL) js_throw_last_napi_error(env, "reading the partitions");
        value_settle(env, ctx, error, partitions, "converting a partitionsFor() answer");
        error = NULL;
    }
    if (value != NULL) error_discard(FFI_ERR(kafka_List_destroy_err(value, &ffi_err)));
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// consumerPartitionsFor(handle, topic, timeoutMs, done): Java's
// partitionsFor(String) and partitionsFor(String, Duration); a null topic
// crosses as NULL, which the FFI reports. `done(error | null, partitions |
// null)`, each partition's fields as partitions_to_js gives them.
napi_value js_consumer_partitions_for(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4 || !is_function(env, argv[3])) {
        napi_throw_type_error(env, NULL, "partitionsFor(handle, topic, timeoutMs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[2], "partitionsFor timeoutMs", &timed, &timeout) != 0) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    char* topic = type == napi_null ? NULL : js_string_dup(env, argv[1], "topic must be a string");
    if (type != napi_null && topic == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[3]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_partitions_for_cb(h, topic, partitions_for_on_complete, ctx);
    } else if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_partitions_for_with_timeout_cb(h, topic, timeout, partitions_for_on_complete, ctx);
    } else if (ctx != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_partitions_for_cb(c->view, topic, partitions_for_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_partitions_for_with_timeout_cb(c->view, topic, timeout, partitions_for_on_complete, ctx);
    }
    free(topic);
    return NULL;
}

// A borrowed map of topic (a C string) to a list of PartitionInfo as an array
// of [topic, partitions], NULL when Node-API failed.
static napi_value topics_to_js(napi_env env, const kafka_Map_t* map) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    error_warn(env, 1, FFI_SET(n, kafka_Map_size_err(map, &ffi_err)));
    napi_value arr;
    if (napi_create_array_with_length(env, (size_t)n, &arr) != napi_ok) return NULL;
    for (int32_t i = 0; i < n; i++) {
        const char* topic = NULL;
        const kafka_List_t* list = NULL;
        error_warn(env, 1, FFI_SET(topic, (const char*)kafka_Map_key_err(map, i, &ffi_err)));
        error_warn(env, 1, FFI_SET(list, (const kafka_List_t*)kafka_Map_value_err(map, i, &ffi_err)));
        napi_value entry, t;
        napi_value partitions = list == NULL ? NULL : partitions_to_js(env, list);
        if (topic == NULL || partitions == NULL || napi_create_array_with_length(env, 2, &entry) != napi_ok
            || napi_create_string_utf8(env, topic, NAPI_AUTO_LENGTH, &t) != napi_ok || napi_set_element(env, entry, 0, t) != napi_ok
            || napi_set_element(env, entry, 1, partitions) != napi_ok || napi_set_element(env, arr, (uint32_t)i, entry) != napi_ok)
            return NULL;
    }
    return arr;
}

// The list_topics_cb completion: the owned map of topic to its partitions, as
// topics_to_js reads it, the map (and its lists) freed.
static void list_topics_on_complete(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (!c->queue.finalizing) {
        napi_value topics = error == NULL && value != NULL ? topics_to_js(env, value) : NULL;
        if (topics == NULL && error == NULL) js_throw_last_napi_error(env, "reading the topics");
        value_settle(env, ctx, error, topics, "converting a listTopics() answer");
        error = NULL;
    }
    if (value != NULL) error_warn(env, !c->queue.finalizing, FFI_ERR(kafka_Map_destroy_err(value, &ffi_err)));
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// consumerListTopics(handle, timeoutMs, done): Java's listTopics() and
// listTopics(Duration). `done(error | null, topics | null)`, the topics as
// topics_to_js gives them.
napi_value js_consumer_list_topics(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "listTopics(handle, timeoutMs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[1], "listTopics timeoutMs", &timed, &timeout) != 0) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[2]);
    if (ctx == NULL) return NULL;
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (h != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_list_topics_cb(h, list_topics_on_complete, ctx);
    } else if (h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_list_topics_with_timeout_cb(h, timeout, list_topics_on_complete, ctx);
    } else if (!timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_list_topics_cb(c->view, list_topics_on_complete, ctx);
    } else {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_list_topics_with_timeout_cb(c->view, timeout, list_topics_on_complete, ctx);
    }
    return NULL;
}

// A borrowed map of TopicPartition to OffsetAndTimestamp as an array of
// [topic, partition, offset, timestamp, leaderEpoch (-1 for none)], or
// [topic, partition, null] for a NULL value; NULL when Node-API failed.
static napi_value timestamps_to_js(napi_env env, const kafka_Map_t* map) {
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    error_warn(env, 1, FFI_SET(n, kafka_Map_size_err(map, &ffi_err)));
    napi_value arr;
    if (napi_create_array_with_length(env, (size_t)n, &arr) != napi_ok) return NULL;
    for (int32_t i = 0; i < n; i++) {
        const kafka_common_TopicPartition_t* tp = NULL;
        const kafka_consumer_OffsetAndTimestamp_t* ot = NULL;
        error_warn(env, 1, FFI_SET(tp, (const kafka_common_TopicPartition_t*)kafka_Map_key_err(map, i, &ffi_err)));
        error_warn(env, 1, FFI_SET(ot, (const kafka_consumer_OffsetAndTimestamp_t*)kafka_Map_value_err(map, i, &ffi_err)));
        napi_value entry = tp == NULL ? NULL : topic_partition_to_js(env, tp);
        if (entry == NULL) return NULL;
        napi_value v;
        if (ot == NULL) {
            if (napi_get_null(env, &v) != napi_ok || napi_set_element(env, entry, 2, v) != napi_ok) return NULL;
        } else {
            int64_t offset = -1, timestamp = -1;
            int32_t epoch = -1;
            error_warn(env, 1, FFI_SET(offset, kafka_consumer_OffsetAndTimestamp_offset_err(ot, &ffi_err)));
            error_warn(env, 1, FFI_SET(timestamp, kafka_consumer_OffsetAndTimestamp_timestamp_err(ot, &ffi_err)));
            error_warn(env, 1, FFI_SET(epoch, kafka_consumer_OffsetAndTimestamp_leader_epoch_err(ot, &ffi_err)));
            if (napi_create_bigint_int64(env, offset, &v) != napi_ok || napi_set_element(env, entry, 2, v) != napi_ok
                || napi_create_bigint_int64(env, timestamp, &v) != napi_ok || napi_set_element(env, entry, 3, v) != napi_ok
                || napi_create_int32(env, epoch, &v) != napi_ok || napi_set_element(env, entry, 4, v) != napi_ok)
                return NULL;
        }
        if (napi_set_element(env, arr, (uint32_t)i, entry) != napi_ok) return NULL;
    }
    return arr;
}

// What the completions of the calls answered with a map of TopicPartition
// share: offsets_for_times_cb's (OffsetAndTimestamp values, read by
// timestamps_to_js) and beginning_offsets_cb's and end_offsets_cb's (int64_t
// values, read by long_map_to_js); the map freed. Each completion ends its
// call's count itself, as its last statement.
static void tp_map_on_complete(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque, int timestamps) {
    error_stat_owned(error);
    CallCtx* ctx = (CallCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (!c->queue.finalizing) {
        napi_value entries = NULL;
        if (error == NULL && value != NULL) entries = timestamps ? timestamps_to_js(env, value) : long_map_to_js(env, value);
        if (entries == NULL && error == NULL) js_throw_last_napi_error(env, "reading the offsets");
        value_settle(env, ctx, error, entries, "converting an offsets answer");
        error = NULL;
    }
    if (value != NULL) error_warn(env, !c->queue.finalizing, FFI_ERR(kafka_Map_destroy_err(value, &ffi_err)));
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
}

static void offsets_for_times_on_complete(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque) {
    Consumer* c = ((CallCtx*)opaque)->c;
    tp_map_on_complete(value, error, opaque, 1);
    cbq_call_end(c->env, &c->queue);
}

static void offsets_on_complete(kafka_Map_t* value, kafka_common_Error_t* error, void* opaque) {
    Consumer* c = ((CallCtx*)opaque)->c;
    tp_map_on_complete(value, error, opaque, 0);
    cbq_call_end(c->env, &c->queue);
}

// consumerOffsetsForTimes(handle, timestampsToSearch, timeoutMs, done): Java's
// offsetsForTimes(Map) and offsetsForTimes(Map, Duration);
// `timestampsToSearch` an array of [[topic, partition], timestamp] entries,
// or null, which the FFI reports. `done(error | null, entries | null)`, the
// entries as timestamps_to_js gives them.
napi_value js_consumer_offsets_for_times(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4 || !is_function(env, argv[3])) {
        napi_throw_type_error(env, NULL, "offsetsForTimes(handle, timestampsToSearch, timeoutMs, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[2], "offsetsForTimes timeoutMs", &timed, &timeout) != 0) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    kafka_Map_t* timestamps = type == napi_null ? NULL : tp_long_map_from_js(env, argv[1]);
    if (type != napi_null && timestamps == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[3]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_offsets_for_times_cb(h, timestamps, offsets_for_times_on_complete, ctx);
    } else if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_offsets_for_times_with_timeout_cb(h, timestamps, timeout, offsets_for_times_on_complete, ctx);
    } else if (ctx != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_offsets_for_times_cb(c->view, timestamps, offsets_for_times_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_offsets_for_times_with_timeout_cb(c->view, timestamps, timeout, offsets_for_times_on_complete, ctx);
    }
    // The call copied the timestamps.
    tp_long_map_free(env, timestamps);
    return NULL;
}

// beginningOffsets and endOffsets: (handle, partitions, timeoutMs, done), the
// partitions an array of [topic, partition] (a null element crosses as NULL),
// or null, which the FFI reports. `done(error | null, entries | null)`, the
// entries as long_map_to_js gives them.
static napi_value offsets_call(napi_env env, napi_callback_info info, int end, const char* shape) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4 || !is_function(env, argv[3])) {
        napi_throw_type_error(env, NULL, shape);
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    int timed = 0;
    int64_t timeout = 0;
    if (timeout_arg(env, argv[2], "offsets timeoutMs", &timed, &timeout) != 0) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    kafka_List_t* partitions = type == napi_null ? NULL : tp_list_from_js(env, argv[1]);
    if (type != napi_null && partitions == NULL) return NULL;
    CallCtx* ctx = call_ctx_new(env, c, argv[3]);
    const kafka_consumer_ConsumerHandle_t* h = window_of(c);
    if (ctx != NULL && h != NULL && !end && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_beginning_offsets_cb(h, partitions, offsets_on_complete, ctx);
    } else if (ctx != NULL && h != NULL && !end) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_beginning_offsets_with_timeout_cb(h, partitions, timeout, offsets_on_complete, ctx);
    } else if (ctx != NULL && h != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_end_offsets_cb(h, partitions, offsets_on_complete, ctx);
    } else if (ctx != NULL && h != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_ConsumerHandle_end_offsets_with_timeout_cb(h, partitions, timeout, offsets_on_complete, ctx);
    } else if (ctx != NULL && !end && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_beginning_offsets_cb(c->view, partitions, offsets_on_complete, ctx);
    } else if (ctx != NULL && !end) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_beginning_offsets_with_timeout_cb(c->view, partitions, timeout, offsets_on_complete, ctx);
    } else if (ctx != NULL && !timed) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_end_offsets_cb(c->view, partitions, offsets_on_complete, ctx);
    } else if (ctx != NULL) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_end_offsets_with_timeout_cb(c->view, partitions, timeout, offsets_on_complete, ctx);
    }
    list_free(env, partitions, tp_item_destroy);
    return NULL;
}

// consumerBeginningOffsets(handle, partitions, timeoutMs, done): Java's
// beginningOffsets(Collection) and beginningOffsets(Collection, Duration).
napi_value js_consumer_beginning_offsets(napi_env env, napi_callback_info info) {
    return offsets_call(env, info, 0, "beginningOffsets(handle, partitions, timeoutMs, done)");
}

// consumerEndOffsets(handle, partitions, timeoutMs, done): Java's
// endOffsets(Collection) and endOffsets(Collection, Duration).
napi_value js_consumer_end_offsets(napi_env env, napi_callback_info info) {
    return offsets_call(env, info, 1, "endOffsets(handle, partitions, timeoutMs, done)");
}

// ---- a record's headers -----------------------------------------------------

// consumerRecordHeaders(source): a consumed record's headers, copied out of
// the native record: [key, value] pairs, each value a Buffer of its own or
// null. The source keeps the batch until it is collected.
napi_value js_consumer_record_headers(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool tagged = false;
    void* data = NULL;
    if (argc != 1 || napi_check_object_type_tag(env, argv[0], &SOURCE_TAG, &tagged) != napi_ok || !tagged
        || napi_get_value_external(env, argv[0], &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, "recordHeaders(source)");
        return NULL;
    }
    HeaderSource* s = (HeaderSource*)data;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_common_header_internals_RecordHeaders_t* headers = NULL;
    kafka_common_Error_t* err = FFI_SET(headers, kafka_consumer_ConsumerRecord_headers_err(s->record, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (headers == NULL) {
        napi_value empty;
        NAPI_CHECK(env, napi_create_array(env, &empty), NULL);
        return empty;
    }
    return headers_to_js(env, headers);
}

// ---- close and destroy ------------------------------------------------------

// The close completion: any outcome but a refused call closed it, as Java's
// close runs every step and throws the first error after. A close refused
// before the core runs closed nothing: another call holds the consumer, or the
// FFI refused NULL options. `done(error | null, closedNow)`.
static void close_on_complete(kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    CloseCtx* ctx = (CloseCtx*)opaque;
    Consumer* c = ctx->c;
    napi_env env = c->env;
    kafka_common_Error_t* ffi_err = NULL;
    int refused = 0;
    if (error != NULL) {
        int code = 0;
        error_warn(env, !c->queue.finalizing, FFI_SET(code, (int)kafka_common_Error__enum_err(error, &ffi_err)));
        refused = code == kafka_common_Error_LOCAL_CONCURRENT_MODIFICATION || code == kafka_common_Error_LOCAL_NULL_POINTER;
    }
    int closed_now = refused || c->queue.finalizing ? 0 : cbq_closed(env, &c->queue);
    // A KafkaConsumer lets go of its listener once its close returns; a mock
    // keeps it until the destroy, as its rebalance() still calls it.
    if (!refused && c->kc != NULL) {
        listener_release(env, c->listener);
        c->listener = NULL;
    }
    if (!c->queue.finalizing) {
        napi_value argv[2];
        argv[0] = error_take_to_js(env, error);
        error = NULL;
        if (napi_get_boolean(env, closed_now != 0, &argv[1]) != napi_ok) argv[1] = NULL;
        if (argv[0] == NULL && argv[1] != NULL) argv[0] = js_take_failure(env, "converting a close's answer");
        if (argv[0] != NULL && argv[1] != NULL) call_ref(env, ctx->done, 2, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &c->queue);
}

// The C CloseOptions as Java builds them: with a timeout,
// CloseOptions.timeout(ms) and then the operation unless it is the default;
// without one, CloseOptions.groupMembershipOperation(op), which leaves the
// timeout unset, so close waits up to Java's default. `operation` is the
// operation's name. NULL with a JS exception pending.
static kafka_consumer_CloseOptions_t* close_options_build(napi_env env, napi_value timeout_value, napi_value operation_value) {
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, timeout_value, &type), NULL);
    int timed = type != napi_null;
    int64_t timeout = 0;
    if (timed && js_int64(env, timeout_value, "close timeoutMs", &timeout) != 0) return NULL;
    char* name = js_string_dup(env, operation_value, "a group membership operation must be a string");
    if (name == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_consumer_CloseOptions_GroupMembershipOperation_t* op = NULL;
    int is_default = strcmp(name, "DEFAULT") == 0;
    kafka_common_Error_t* err = NULL;
    if (strcmp(name, "LEAVE_GROUP") == 0)
        err = FFI_SET(op, kafka_consumer_CloseOptions_GroupMembershipOperation_leave_group_err(&ffi_err));
    else if (strcmp(name, "REMAIN_IN_GROUP") == 0)
        err = FFI_SET(op, kafka_consumer_CloseOptions_GroupMembershipOperation_remain_in_group_err(&ffi_err));
    else
        err = FFI_SET(op, kafka_consumer_CloseOptions_GroupMembershipOperation_default_err(&ffi_err));
    free(name);
    kafka_consumer_CloseOptions_t* options = NULL;
    if (err == NULL && timed) {
        err = FFI_SET(options, kafka_consumer_CloseOptions_new_with_timeout_err(timeout, &ffi_err));
        if (err == NULL && !is_default)
            err = FFI_ERR(kafka_consumer_CloseOptions_with_group_membership_operation_err(options, op, &ffi_err));
    } else if (err == NULL) {
        err = FFI_SET(options, kafka_consumer_CloseOptions_with_operation_err(op, &ffi_err));
    }
    if (err != NULL) {
        if (options != NULL) FFI_DISCARD(kafka_consumer_CloseOptions_destroy_err(options, &ffi_err));
        error_throw_owned(env, err);
        return NULL;
    }
    return options;
}

// consumerClose(handle, withOptions, timeoutMs, operation, done): Java's
// close() when `withOptions` is false, else close(CloseOptions) built from the
// options' timeout (null: not set) and operation name, or NULL for a null
// option (`operation` null), which the FFI refuses; through the `_cb` twins.
// A close of a consumer already closed goes to the core too.
// `done(error | null, closedNow)`.
napi_value js_consumer_close(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value argv[5];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool with_options = false;
    if (argc != 5 || napi_get_value_bool(env, argv[1], &with_options) != napi_ok || !is_function(env, argv[4])) {
        napi_throw_type_error(env, NULL, "close(handle, withOptions, timeoutMs, operation, done)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    if (c == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[3], &type), NULL);
    kafka_consumer_CloseOptions_t* options = NULL;
    if (with_options && type != napi_null && (options = close_options_build(env, argv[2], argv[3])) == NULL) return NULL;
    CloseCtx* ctx = (CloseCtx*)calloc(1, sizeof(CloseCtx));
    kafka_common_Error_t* ffi_err = NULL;
    if (ctx == NULL || napi_create_reference(env, argv[4], 1, &ctx->done) != napi_ok) {
        free(ctx);
        if (options != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_CloseOptions_destroy_err(options, &ffi_err)));
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    ctx->c = c;
    if (!with_options) {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_close_cb(c->view, close_on_complete, ctx);
    } else {
        cbq_call_begin(env, &c->queue);
        kafka_consumer_Consumer_close_with_options_cb(c->view, options, close_on_complete, ctx);
        // The call copied the options.
        if (options != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_CloseOptions_destroy_err(options, &ffi_err)));
    }
    return NULL;
}

// The queue's execute: the consumer's __execute_callbacks.
static int32_t consumer_execute(void* client, kafka_common_Error_t** out_err) {
    return kafka_consumer_Consumer__execute_callbacks_err(((Consumer*)client)->view, out_err);
}

static void noop_notify(void* opaque) {
    (void)opaque;
}

// Destroys the class handle of a closed consumer, on the JS thread, from the
// handle object's finalizer, outside any drain: the core runs the
// still-pending completions here, inline, once each, running no JS. Then
// releases the tsfn.
static void consumer_destroy(napi_env env, Consumer* c) {
    kafka_common_Error_t* ffi_err = NULL;
    c->queue.stopped = 1;
    // The destroy waits for the reports of the callbacks it runs: those still
    // waiting for a Promise are made now, as no JS will settle them.
    deferred_report_all(env, c);
    if (c->kc != NULL) error_warn(env, !c->queue.finalizing, FFI_ERR(kafka_consumer_KafkaConsumer_destroy_err(c->kc, &ffi_err)));
    else error_warn(env, !c->queue.finalizing, FFI_ERR(kafka_consumer_MockConsumer_destroy_err(c->mock, &ffi_err)));
    c->destroyed = 1;
    c->kc = NULL;
    c->mock = NULL;
    c->view = NULL;
    // The handle outlives the class handle, as its docs allow.
    if (c->handle != NULL) error_warn(env, !c->queue.finalizing, FFI_ERR(kafka_consumer_ConsumerHandle_destroy_err(c->handle, &ffi_err)));
    c->handle = NULL;
    // The core holds no record, error or listener of the consumer's any more.
    mock_records_free(env, &c->kept);
    injected_errors_release(env, &c->injected);
    listener_release(env, c->listener);
    c->listener = NULL;
    while (c->commits != NULL) commit_callback_free(env, c->commits);
    cbq_release(env, &c->queue);
}

// The queue's on_tsfn_finalize: the tsfn's ownership of the struct ends.
static void consumer_tsfn_finalized(napi_env env, void* client) {
    (void)env;
    Consumer* c = (Consumer*)client;
    if (consumer_alive(c)) {
        // Environment teardown finalized the tsfn before the handle object.
        kafka_common_Error_t* ffi_err = NULL;
        FFI_DISCARD(kafka_consumer_Consumer__set_callbacks_notify_err(c->view, noop_notify, NULL, &ffi_err));
    }
    consumer_release(c);
}

// The handle object's finalizer: a closed consumer is destroyed here, running
// no JS. An open one is never closed or destroyed by a finalizer
// (javascript/CLAUDE.md, rule 10): only an environment teardown finalizes
// one, and it is left to the core, with this struct.
static void consumer_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    Consumer* c = (Consumer*)data;
    c->queue.finalizing = 1;
    int open = consumer_alive(c) && !cbq_is_closed(&c->queue);
    if (consumer_alive(c) && cbq_is_closed(&c->queue)) consumer_destroy(env, c);
    cbq_detach(env, &c->queue);
    if (!open) consumer_release(c);
}

// ---- construction -----------------------------------------------------------

// Destroys class handle `kc` or `mock` (one is NULL), and then its
// reentrancy handle, on a construction's failure paths, before a handle
// object, and so a finalizer, exists.
static void class_destroy(napi_env env, kafka_consumer_KafkaConsumer_t* kc, kafka_consumer_MockConsumer_t* mock,
                          kafka_consumer_ConsumerHandle_t* handle) {
    kafka_common_Error_t* ffi_err = NULL;
    if (kc != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_KafkaConsumer_destroy_err(kc, &ffi_err)));
    if (mock != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_MockConsumer_destroy_err(mock, &ffi_err)));
    if (handle != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerHandle_destroy_err(handle, &ffi_err)));
}

// The native handle over class handle `kc` or `mock` (one is NULL), which it
// owns from now on: its view, callback queue and handle object, the same for
// a KafkaConsumer and a MockConsumer. Throws, and destroys the class handle,
// on failure.
static napi_value consumer_handle_new(napi_env env, kafka_consumer_KafkaConsumer_t* kc, kafka_consumer_MockConsumer_t* mock) {
    kafka_common_Error_t* ffi_err = NULL;
    Consumer* c = (Consumer*)calloc(1, sizeof(Consumer));
    if (c == NULL) {
        class_destroy(env, kc, mock, NULL);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    atomic_fetch_add(&stat_consumers_made, 1);
    c->env = env;
    c->kc = kc;
    c->mock = mock;
    kafka_common_Error_t* err = kc != NULL ? FFI_SET(c->view, kafka_consumer_KafkaConsumer__as_Consumer_err(kc, &ffi_err))
                                           : FFI_SET(c->view, kafka_consumer_MockConsumer__as_Consumer_err(mock, &ffi_err));
    // The reentrancy handle the window's calls go through, made with the consumer.
    if (err == NULL) err = FFI_SET(c->handle, kafka_consumer_Consumer_handle_err(c->view, &ffi_err));
    if (err != NULL || cbq_init(env, &c->queue, "confluent-kafka consumer callbacks", c, consumer_execute,
                                consumer_tsfn_finalized) != 0) {
        class_destroy(env, kc, mock, c->handle);
        free(c);
        atomic_fetch_add(&stat_consumers_freed, 1);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    c->owners = 1;  // the tsfn
    napi_value result = NULL;
    int thrown = 0;
    if (napi_create_object(env, &result) != napi_ok || napi_wrap(env, result, c, consumer_finalize, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "napi_wrap");
        thrown = 1;
    } else {
        c->owners = 2;  // and the handle object
        if (cbq_attach(env, &c->queue, result) != 0) thrown = 1;
    }
    // A test's faultNext('notify') fails the registration through the FFI's
    // own error path: a NULL view.
    if (!thrown)
        err = FFI_ERR(kafka_consumer_Consumer__set_callbacks_notify_err(fault_due(FAULT_NOTIFY) ? NULL : c->view, cbq_notify,
                                                                       &c->queue, &ffi_err));
    if (err != NULL || thrown) {
        class_destroy(env, kc, mock, c->handle);
        c->kc = NULL;
        c->mock = NULL;
        c->view = NULL;
        c->handle = NULL;
        // The handle object, never handed out, may be collected: its
        // finalizer releases its unit of c, a destroyed client's.
        cbq_detach(env, &c->queue);
        // Releases the tsfn, whose finalizer frees c when it is the only owner.
        cbq_release(env, &c->queue);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return result;
}

// consumerCreate(entries): a native consumer handle over
// KafkaConsumer(configs) with NULL deserializers, `entries` an array of
// [key, value] byte pairs. Throws the core's error when the configuration is
// rejected or the consumer cannot be built.
napi_value js_consumer_create(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool is_array = false;
    if (argc != 1 || napi_is_array(env, argv[0], &is_array) != napi_ok || !is_array) {
        napi_throw_type_error(env, NULL, "create(entries)");
        return NULL;
    }
    ConfigMap props;
    if (config_map_build(env, argv[0], &props) != 0) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_consumer_ConsumerConfig_t* config = NULL;
    kafka_consumer_KafkaConsumer_t* kc = NULL;
    // ConsumerConfig_new and KafkaConsumer_new take an out_err of their own.
    kafka_common_Error_t* err = FFI_SET(config, kafka_consumer_ConsumerConfig_new(props.map, &ffi_err));
    config_map_free(env, &props);
    if (err == NULL) err = FFI_SET(kc, kafka_consumer_KafkaConsumer_new(config, NULL, NULL, &ffi_err));
    if (config != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerConfig_destroy_err(config, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return consumer_handle_new(env, kc, NULL);
}

// ---- MockConsumer -------------------------------------------------------------

// mockConsumerCreate(offsetResetStrategy): a native consumer handle over the
// FFI's MockConsumer, Java's MockConsumer(String); a null strategy goes to
// the FFI as NULL, which refuses it with Java's message. Throws the core's
// error for a strategy it refuses.
napi_value js_mock_consumer_create(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "mockConsumerCreate(offsetResetStrategy)");
        return NULL;
    }
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[0], &type), NULL);
    char* strategy = NULL;
    if (type != napi_null && (strategy = js_string_dup(env, argv[0], "offsetResetStrategy must be a string")) == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_consumer_MockConsumer_t* mock = NULL;
    // MockConsumer_new takes an out_err of its own.
    kafka_common_Error_t* err = FFI_SET(mock, kafka_consumer_MockConsumer_new(strategy, &ffi_err));
    free(strategy);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return consumer_handle_new(env, NULL, mock);
}

// mockConsumerAddRecord(handle, record, topic, partition, offset,
// leaderEpoch): Java's addRecord(record) of the app's own ConsumerRecord
// `record`, or null, which goes to the FFI as NULL. The C record carries what
// the FFI's mock reads: the topic, partition, offset and leader epoch (-1 for
// none, the builder's setter not called); its key and value are a
// MockRecord's tags, and the MockRecord holds `record` until the destroy, as
// the core's copy of the C record shares them. A record the FFI refuses is
// released at once.
napi_value js_mock_consumer_add_record(napi_env env, napi_callback_info info) {
    size_t argc = 6;
    napi_value argv[6];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 6) {
        napi_throw_type_error(env, NULL, "mockConsumerAddRecord(handle, record, topic, partition, offset, leaderEpoch)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    kafka_consumer_MockConsumer_t* mock = mock_of(env, c);
    if (mock == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = NULL;
    if (type == napi_null) {
        // add_record takes an out_err of its own.
        err = FFI_ERR(kafka_consumer_MockConsumer_add_record(mock, NULL, &ffi_err));
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    int32_t partition = -1, leader_epoch = -1;
    int64_t offset = -1;
    if (napi_get_value_int32(env, argv[3], &partition) != napi_ok || napi_get_value_int32(env, argv[5], &leader_epoch) != napi_ok) {
        napi_throw_type_error(env, NULL, "a record's partition and leader epoch must be numbers");
        return NULL;
    }
    if (js_int64(env, argv[4], "a record's offset must be a bigint or a number", &offset) != 0) return NULL;
    char* topic = js_string_dup(env, argv[2], "a record's topic must be a string");
    if (topic == NULL) return NULL;
    MockRecord* kept = mock_record_new(env, argv[1]);
    if (kept == NULL) {
        free(topic);
        return NULL;
    }
    kafka_consumer_ConsumerRecordOptionsBuilder_t* builder = NULL;
    kafka_consumer_ConsumerRecordOptions_t* options = NULL;
    kafka_consumer_ConsumerRecord_t* record = NULL;
    err = FFI_SET(builder, kafka_consumer_ConsumerRecordOptionsBuilder_new_err(&ffi_err));
    if (err == NULL) err = FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_set_topic_err(builder, topic, &ffi_err));
    if (err == NULL) err = FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_set_partition_err(builder, partition, &ffi_err));
    if (err == NULL) err = FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_set_offset_err(builder, offset, &ffi_err));
    if (err == NULL) err = FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_set_key_err(builder, &kept->key_tag, &ffi_err));
    if (err == NULL) err = FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_set_value_err(builder, &kept->value_tag, &ffi_err));
    // A negative leader epoch is Java's Optional.empty(): the setter is not called.
    if (err == NULL && leader_epoch >= 0)
        err = FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_set_leader_epoch_err(builder, leader_epoch, &ffi_err));
    // build takes an out_err of its own.
    if (err == NULL) err = FFI_SET(options, kafka_consumer_ConsumerRecordOptionsBuilder_build(builder, &ffi_err));
    if (err == NULL) err = FFI_SET(record, kafka_consumer_ConsumerRecord_with_options_err(options, &ffi_err));
    free(topic);
    // add_record takes an out_err of its own; it copies the record, sharing its key and value.
    if (err == NULL) err = FFI_ERR(kafka_consumer_MockConsumer_add_record(mock, record, &ffi_err));
    if (record != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerRecord_destroy_err(record, &ffi_err)));
    if (options != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerRecordOptions_destroy_err(options, &ffi_err)));
    if (builder != NULL) error_warn(env, 1, FFI_ERR(kafka_consumer_ConsumerRecordOptionsBuilder_destroy_err(builder, &ffi_err)));
    if (err != NULL) {
        // Refused: the core keeps no copy of the record.
        mock_record_free(env, kept);
        error_throw_owned(env, err);
        return NULL;
    }
    kept->next = c->kept;
    c->kept = kept;
    return NULL;
}

// mockConsumerSetError(handle, which, error): Java's setPollException
// (`which` "poll") or setOffsetsException ("offsets") with `error`, or
// cleared by null; the mock throws the very same value, once.
napi_value js_mock_consumer_set_error(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    char* which = argc == 3 ? js_string_dup(env, argv[1], "which must be a string") : NULL;
    if (which == NULL) {
        if (argc != 3) napi_throw_type_error(env, NULL, "mockConsumerSetError(handle, which, error)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    kafka_consumer_MockConsumer_t* mock = mock_of(env, c);
    kafka_common_Error_t* error = NULL;
    if (mock == NULL || injected_error_new(env, &c->injected, argv[2], &error) != 0) {
        free(which);
        return NULL;
    }
    int poll = strcmp(which, "poll") == 0;
    free(which);
    kafka_common_Error_t* ffi_err = NULL;
    // The setter takes the error over.
    error_stat_given(error);
    kafka_common_Error_t* err = poll ? FFI_ERR(kafka_consumer_MockConsumer_set_poll_error_err(mock, error, &ffi_err))
                                     : FFI_ERR(kafka_consumer_MockConsumer_set_offsets_error_err(mock, error, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockConsumerUpdateOffsets(handle, which, entries): Java's
// updateBeginningOffsets (`which` "beginning"), updateEndOffsets ("end") or
// updateDurationOffsets ("duration"), `entries` [[topic, partition], offset]
// or null, a null map or key going to the FFI as NULL. The FFI copies the
// map, which is freed after the call.
napi_value js_mock_consumer_update_offsets(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    char* which = argc == 3 ? js_string_dup(env, argv[1], "which must be a string") : NULL;
    if (which == NULL) {
        if (argc != 3) napi_throw_type_error(env, NULL, "mockConsumerUpdateOffsets(handle, which, entries)");
        return NULL;
    }
    int end = strcmp(which, "end") == 0;
    int duration = strcmp(which, "duration") == 0;
    free(which);
    Consumer* c = unwrap_consumer(env, argv[0]);
    kafka_consumer_MockConsumer_t* mock = mock_of(env, c);
    if (mock == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    kafka_Map_t* map = NULL;
    if (type != napi_null && (map = tp_long_map_from_js(env, argv[2])) == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = NULL;
    if (end) err = FFI_ERR(kafka_consumer_MockConsumer_update_end_offsets_err(mock, map, &ffi_err));
    else if (duration) err = FFI_ERR(kafka_consumer_MockConsumer_update_duration_offsets_err(mock, map, &ffi_err));
    else err = FFI_ERR(kafka_consumer_MockConsumer_update_beginning_offsets_err(mock, map, &ffi_err));
    tp_long_map_free(env, map);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockConsumerUpdatePartitions(handle, topic, partitions): Java's
// updatePartitions(topic, partitions), each partition's fields built into a
// PartitionInfo (partition_info_list_from_js); a null topic, list, element or
// field goes to the FFI as NULL, which refuses it. The FFI copies the
// partitions, which are freed after the call.
napi_value js_mock_consumer_update_partitions(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3) {
        napi_throw_type_error(env, NULL, "mockConsumerUpdatePartitions(handle, topic, partitions)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    kafka_consumer_MockConsumer_t* mock = mock_of(env, c);
    if (mock == NULL) return NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    char* topic = NULL;
    if (type != napi_null && (topic = js_string_dup(env, argv[1], "topic must be a string")) == NULL) return NULL;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    kafka_List_t* partitions = NULL;
    if (type != napi_null && (partitions = partition_info_list_from_js(env, argv[2])) == NULL) {
        free(topic);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    // update_partitions takes an out_err of its own.
    kafka_common_Error_t* err = FFI_ERR(kafka_consumer_MockConsumer_update_partitions(mock, topic, partitions, &ffi_err));
    free(topic);
    list_free(env, partitions, partition_info_item_destroy);
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockConsumerSetMaxPollRecords(handle, maxPollRecords): Java's
// setMaxPollRecords(long); the FFI refuses a value below 1 with Java's
// message.
napi_value js_mock_consumer_set_max_poll_records(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2) {
        napi_throw_type_error(env, NULL, "mockConsumerSetMaxPollRecords(handle, maxPollRecords)");
        return NULL;
    }
    Consumer* c = unwrap_consumer(env, argv[0]);
    kafka_consumer_MockConsumer_t* mock = mock_of(env, c);
    if (mock == NULL) return NULL;
    int64_t max = 0;
    if (js_int64(env, argv[1], "maxPollRecords must be a bigint or a number", &max) != 0) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    // set_max_poll_records takes an out_err of its own.
    kafka_common_Error_t* err = FFI_ERR(kafka_consumer_MockConsumer_set_max_poll_records(mock, max, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// The mock of a call taking only the handle; NULL with a JS exception pending.
static kafka_consumer_MockConsumer_t* mock_arg(napi_env env, napi_callback_info info, const char* shape) {
    Consumer* c = handle_arg(env, info, shape);
    return c == NULL ? NULL : mock_of(env, c);
}

// mockConsumerScheduleNopPollTask(handle): Java's scheduleNopPollTask().
napi_value js_mock_consumer_schedule_nop_poll_task(napi_env env, napi_callback_info info) {
    kafka_consumer_MockConsumer_t* mock = mock_arg(env, info, "mockConsumerScheduleNopPollTask(handle)");
    if (mock == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_consumer_MockConsumer_schedule_nop_poll_task_err(mock, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockConsumerClosed(handle): Java's closed(), which the FFI reads through
// the handle, so it answers while another call is pending.
napi_value js_mock_consumer_closed(napi_env env, napi_callback_info info) {
    kafka_consumer_MockConsumer_t* mock = mock_arg(env, info, "mockConsumerClosed(handle)");
    if (mock == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    int8_t answer = 0;
    kafka_common_Error_t* err = FFI_SET(answer, kafka_consumer_MockConsumer_closed_err(mock, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value v;
    NAPI_CHECK(env, napi_get_boolean(env, answer == 1, &v), NULL);
    return v;
}

// mockConsumerLastPollTimeout(handle): Java's lastPollTimeout() as a bigint
// of milliseconds, -1 for Java's null (no poll yet); the FFI reads it through
// the handle, so it answers while another call is pending.
napi_value js_mock_consumer_last_poll_timeout(napi_env env, napi_callback_info info) {
    kafka_consumer_MockConsumer_t* mock = mock_arg(env, info, "mockConsumerLastPollTimeout(handle)");
    if (mock == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    int64_t timeout = -1;
    kafka_common_Error_t* err = FFI_SET(timeout, kafka_consumer_MockConsumer_last_poll_timeout_err(mock, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value v;
    NAPI_CHECK(env, napi_create_bigint_int64(env, timeout, &v), NULL);
    return v;
}

// ---- test-only --------------------------------------------------------------

static int stat_put(napi_env env, napi_value obj, const char* name, atomic_long* count) {
    napi_value v;
    if (napi_create_int64(env, (int64_t)atomic_load(count), &v) != napi_ok || napi_set_named_property(env, obj, name, v) != napi_ok) {
        js_throw_last_napi_error(env, name);
        return -1;
    }
    return 0;
}

// consumerStats(): the counts of the consumers, batches, external buffers,
// header sources and copies made and freed, the listeners' and commit
// callbacks' contexts and the deferred reports, and the mocks' records and
// the carried values. Test-only.
napi_value js_consumer_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (stat_put(env, obj, "consumersMade", &stat_consumers_made) || stat_put(env, obj, "consumersFreed", &stat_consumers_freed)
        || stat_put(env, obj, "batchesMade", &stat_batches_made) || stat_put(env, obj, "batchesDestroyed", &stat_batches_destroyed)
        || stat_put(env, obj, "buffersMade", &stat_buffers_made) || stat_put(env, obj, "buffersFreed", &stat_buffers_freed)
        || stat_put(env, obj, "sourcesMade", &stat_sources_made) || stat_put(env, obj, "sourcesFreed", &stat_sources_freed)
        || stat_put(env, obj, "copies", &stat_copies) || stat_put(env, obj, "listenersMade", &stat_listeners_made)
        || stat_put(env, obj, "listenersFreed", &stat_listeners_freed)
        || stat_put(env, obj, "commitCallbacksMade", &stat_commit_callbacks_made)
        || stat_put(env, obj, "commitCallbacksFreed", &stat_commit_callbacks_freed) || stat_put(env, obj, "deferralsMade", &stat_deferrals_made)
        || stat_put(env, obj, "deferralsFreed", &stat_deferrals_freed))
        return NULL;
    // The mocks' records and injected errors, counted for every mock.
    if (mock_values_stats_put(env, obj) != 0) {
        js_throw_last_napi_error(env, "consumerStats");
        return NULL;
    }
    return obj;
}

// consumerRefuseExternalBuffers(on): makes the addon act as if Node-API
// refused external buffers, as Electron does, for the batches built from now
// on. Test-only.
napi_value js_consumer_refuse_external_buffers(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool on = false;
    if (argc != 1 || napi_get_value_bool(env, argv[0], &on) != napi_ok) {
        napi_throw_type_error(env, NULL, "consumerRefuseExternalBuffers(on)");
        return NULL;
    }
    atomic_store(&refuse_external, on ? 1 : 0);
    return NULL;
}

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

// The admin client handle: a marshaling-only bridge to the Rust admin FFI
// (javascript/CLAUDE.md). It runs no thread of its own and holds no Kafka
// logic.
//
// One handle type serves KafkaAdminClient (an owned `kafka_admin_Admin_t`
// from AdminClient_create) and MockAdminClient (the FFI's mock, through its
// `__as_Admin` view), as the producer's serves KafkaProducer and
// MockProducer. Every RPC is a plain synchronous call that returns its Result
// at once, as Java's does: the native function builds the options from the
// setters the JS options object ran, calls `kafka_admin_Admin_<rpc>_with_options`
// and wraps the owned `<Rpc>Result_t` in a result box (a tagged object whose
// finalizer destroys it). The result's accessors hand out futures
// (admin_values.c's descriptor tables name them): each future lives in a
// context in the admin client's slot table, as the producer's record futures
// do, and JS holds only its slot number. Its untimed wait (`get_cb`) starts
// when JS makes the Result (adminFutureWait), so the outcome is in JS when the
// core completes the future, as Java's admin thread completes it; a timed
// `get` is `get_with_timeout_cb`. Both completions are queued on the admin
// client's callback vector, which its CallbackQueue drains on the JS thread.
//
// Every `_cb` call is counted on the queue (javascript/CLAUDE.md, "Threads and
// liveness"): `cbq_call_begin` right before the call and `cbq_call_end` as the
// last statement of its completion (close_on_complete, future_on_wait,
// future_on_timed_wait), so an open admin client keeps Node running and a
// call made on a closed one holds Node until its answer. There is no closed
// flag: an RPC after close() goes to the FFI, whose futures fail with the
// core's error. close() does not free the handle; its completion makes the
// queue's reference on the handle object weak (cbq_closed), and the handle
// object's finalizer then destroys the closed client, running no JS. No
// finalizer closes or destroys an open client.

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "admin.h"

static const napi_type_tag ADMIN_TAG = {0x6b6a7361646d696eULL, 0x5f68616e646c6531ULL};
static const napi_type_tag RESULT_TAG = {0x6b6a7361646d696eULL, 0x5f726573756c7431ULL};
static const napi_type_tag BUILDER_TAG = {0x6b6a7361646d696eULL, 0x5f6275696c646531ULL};

// The counts adminStats() reads, for the lifetime tests. Test-only.
static atomic_long stat_admins_made;
static atomic_long stat_admins_freed;
static atomic_long stat_results_made;
static atomic_long stat_results_freed;
static atomic_long stat_futures_made;
static atomic_long stat_futures_freed;
static atomic_long stat_waits_started;
static atomic_long stat_waits_completed;

static void admin_release(Admin* a) {
    if (--a->owners != 0) return;
    cbq_dispose(&a->queue);
    free(a->slots);
    free(a->free_next);
    free(a);
    atomic_fetch_add(&stat_admins_freed, 1);
}

// Whether the class handle is alive: not destroyed (only the handle object's
// finalizer destroys it).
static int admin_alive(const Admin* a) {
    return a->owned != NULL || a->mock != NULL;
}

// ---- the slot table of futures ----------------------------------------------

static int32_t slot_take(Admin* a, AdminFuture* f) {
    if (a->free_head < 0) {
        int32_t grown = a->slot_count == 0 ? 16 : a->slot_count * 2;
        if (grown <= a->slot_count) return -1;
        AdminFuture** slots = (AdminFuture**)realloc(a->slots, (size_t)grown * sizeof(AdminFuture*));
        if (slots == NULL) return -1;
        a->slots = slots;
        int32_t* next = (int32_t*)realloc(a->free_next, (size_t)grown * sizeof(int32_t));
        if (next == NULL) return -1;
        a->free_next = next;
        for (int32_t i = grown - 1; i >= a->slot_count; i--) {
            a->slots[i] = NULL;
            a->free_next[i] = a->free_head;
            a->free_head = i;
        }
        a->slot_count = grown;
    }
    int32_t i = a->free_head;
    a->free_head = a->free_next[i];
    a->slots[i] = f;
    return i;
}

static void slot_give_back(Admin* a, int32_t i) {
    a->slots[i] = NULL;
    a->free_next[i] = a->free_head;
    a->free_head = i;
}

// Drops one reference on a future's context: the last one gives its slot
// back, destroys the future it owns (or drops the reference on the box that
// owns it) and releases the context's unit of the admin struct. JS thread.
static void future_release(napi_env env, AdminFuture* f) {
    if (--f->refs != 0) return;
    Admin* a = f->a;
    kafka_common_Error_t* ffi_err = NULL;
    if (f->slot >= 0) slot_give_back(a, f->slot);
    if (f->owned && f->future != NULL)
        error_warn(env, !a->queue.finalizing, FFI_ERR(kafka_common_KafkaFuture_destroy_err(f->future, &ffi_err)));
    delete_ref(env, &f->keep);
    free(f);
    atomic_fetch_add(&stat_futures_freed, 1);
    admin_release(a);
}

int32_t admin_future_new(napi_env env, Admin* a, kafka_common_KafkaFuture_t* future, int owned, napi_value keep,
                         admin_conv_fn conv) {
    kafka_common_Error_t* ffi_err = NULL;
    AdminFuture* f = (AdminFuture*)calloc(1, sizeof(AdminFuture));
    if (f == NULL) {
        if (owned) error_warn(env, 1, FFI_ERR(kafka_common_KafkaFuture_destroy_err(future, &ffi_err)));
        napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    f->a = a;
    f->future = future;
    f->owned = owned;
    f->conv = conv;
    f->refs = 1;
    f->slot = -1;
    a->owners++;
    atomic_fetch_add(&stat_futures_made, 1);
    if (keep != NULL && napi_create_reference(env, keep, 1, &f->keep) != napi_ok) {
        future_release(env, f);
        js_throw_last_napi_error(env, "napi_create_reference");
        return -1;
    }
    f->slot = slot_take(a, f);
    if (f->slot < 0) {
        future_release(env, f);
        napi_throw_error(env, NULL, "out of memory");
        return -1;
    }
    return f->slot;
}

// ---- unwrapping -------------------------------------------------------------

static void* unwrap_tagged(napi_env env, napi_value value, const napi_type_tag* tag, const char* what) {
    bool tagged = false;
    void* data = NULL;
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_object
        || napi_check_object_type_tag(env, value, tag, &tagged) != napi_ok || !tagged
        || napi_unwrap(env, value, &data) != napi_ok || data == NULL) {
        napi_throw_type_error(env, NULL, what);
        return NULL;
    }
    return data;
}

Admin* admin_unwrap(napi_env env, napi_value handle) {
    Admin* a = (Admin*)unwrap_tagged(env, handle, &ADMIN_TAG, "not an admin client handle");
    if (a == NULL) return NULL;
    // Unreachable from JS: destroy runs only from the handle object's
    // finalizer, once nothing can call through it. A guard of memory, not of
    // the closed state.
    if (!admin_alive(a)) {
        napi_throw_error(env, NULL, "the admin client has been destroyed");
        return NULL;
    }
    return a;
}

AdminResultBox* admin_result_unwrap(napi_env env, napi_value box) {
    return (AdminResultBox*)unwrap_tagged(env, box, &RESULT_TAG, "not an admin result");
}

static void result_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    AdminResultBox* r = (AdminResultBox*)data;
    kafka_common_Error_t* ffi_err = NULL;
    if (r->result != NULL) error_warn(env, 0, FFI_ERR(r->desc->destroy(r->result, &ffi_err)));
    if (r->map != NULL) error_warn(env, 0, FFI_ERR(kafka_Map_destroy_err(r->map, &ffi_err)));
    free(r);
    atomic_fetch_add(&stat_results_freed, 1);
}

napi_value admin_box_new(napi_env env, void* result, kafka_Map_t* map, const AdminResultDesc* desc) {
    kafka_common_Error_t* ffi_err = NULL;
    AdminResultBox* r = (AdminResultBox*)calloc(1, sizeof(AdminResultBox));
    napi_value obj = NULL;
    if (r == NULL) {
        if (result != NULL) error_warn(env, 1, FFI_ERR(desc->destroy(result, &ffi_err)));
        if (map != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    r->result = result;
    r->map = map;
    r->desc = desc;
    atomic_fetch_add(&stat_results_made, 1);
    if (napi_create_object(env, &obj) != napi_ok || napi_type_tag_object(env, obj, &RESULT_TAG) != napi_ok
        || napi_wrap(env, obj, r, result_finalize, NULL, NULL) != napi_ok) {
        result_finalize(env, r, NULL);
        js_throw_last_napi_error(env, "napi_wrap");
        return NULL;
    }
    return obj;
}

// ---- the futures' waits -----------------------------------------------------

// A wait on a future: the untimed one (`get_cb`, its value converted) or a
// timed one (`get_with_timeout_cb`, its value left to the untimed wait). It
// holds one reference on the context, and is one of the client's counted
// calls.
typedef struct {
    AdminFuture* f;
    int timed;
    napi_ref done;
} FutureWait;

// Whether a completion may run JS: not while the handle object's finalizer
// destroys the client (inline, inside the destroy).
static int may_run_js(const Admin* a) {
    return !(a->queue.finalizing && admin_alive(a));
}

// The completion of the untimed wait (kafka_common_KafkaFuture_get_cb_t): run
// by the client's drain, or by its destroy; on the JS thread. `value` is
// borrowed from the future until this returns, so it is converted here.
// `done(value | null, error | null)`.
static void future_on_wait(void* value, kafka_common_Error_t* error, void* opaque) {
    error_stat_owned(error);
    FutureWait* w = (FutureWait*)opaque;
    AdminFuture* f = w->f;
    Admin* a = f->a;
    napi_env env = a->env;
    if (may_run_js(a)) {
        napi_value argv[2];
        if (error != NULL) {
            napi_get_null(env, &argv[0]);
            argv[1] = error_take_to_js(env, error);
            error = NULL;
        } else {
            argv[0] = f->conv != NULL ? f->conv(env, value) : NULL;
            if (argv[0] == NULL && f->conv == NULL) napi_get_null(env, &argv[0]);
            if (argv[0] == NULL) {
                // The conversion failed: the pending exception is the future's failure.
                napi_value exc = NULL;
                bool pending = false;
                if (napi_is_exception_pending(env, &pending) == napi_ok && pending) napi_get_and_clear_last_exception(env, &exc);
                napi_get_null(env, &argv[0]);
                argv[1] = exc;
            } else {
                napi_get_null(env, &argv[1]);
            }
        }
        if (argv[1] != NULL) call_ref(env, w->done, 2, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &w->done);
    free(w);
    atomic_fetch_add(&stat_waits_completed, 1);
    // The value is borrowed from the future until this returns: the wait's
    // reference, then the slot's (JS reads the future no more once it has its
    // outcome), are dropped only now.
    future_release(env, f);
    future_release(env, f);
    cbq_call_end(env, &a->queue);
    admin_release(a);
}

// The completion of a timed wait (kafka_common_KafkaFuture_get_with_timeout_cb_t).
// `done(error | null, timedOut)`: the outcome itself comes from the untimed
// wait.
static void future_on_timed_wait(void* value, kafka_common_Error_t* error, void* opaque) {
    (void)value;
    error_stat_owned(error);
    FutureWait* w = (FutureWait*)opaque;
    AdminFuture* f = w->f;
    Admin* a = f->a;
    napi_env env = a->env;
    kafka_common_Error_t* ffi_err = NULL;
    if (may_run_js(a)) {
        int timed_out = 0;
        if (error != NULL) {
            int code = 0;
            error_warn(env, 1, FFI_SET(code, (int)kafka_common_Error__enum_err(error, &ffi_err)));
            timed_out = code == kafka_common_Error_LOCAL_TIMEOUT;
        }
        napi_value argv[2];
        argv[0] = error_take_to_js(env, error);
        error = NULL;
        if (argv[0] != NULL && napi_get_boolean(env, timed_out != 0, &argv[1]) == napi_ok) call_ref(env, w->done, 2, argv, NULL);
        else js_warn_thrown(env);
    }
    error_discard(error);
    delete_ref(env, &w->done);
    free(w);
    atomic_fetch_add(&stat_waits_completed, 1);
    future_release(env, f);
    cbq_call_end(env, &a->queue);
    admin_release(a);
}

static int is_function(napi_env env, napi_value v) {
    napi_valuetype type;
    return napi_typeof(env, v, &type) == napi_ok && type == napi_function;
}

// The future in `slot`, or NULL with an Error pending: unreachable for a slot
// JS was given, which is given back only once JS reads it no more.
static AdminFuture* future_at(napi_env env, Admin* a, napi_value slot_value) {
    int32_t slot = -1;
    if (napi_get_value_int32(env, slot_value, &slot) != napi_ok || slot < 0 || slot >= a->slot_count
        || a->slots[slot] == NULL) {
        napi_throw_error(env, NULL, "no admin future in this slot");
        return NULL;
    }
    return a->slots[slot];
}

// adminFutureWait(handle, slot, done): the untimed wait on the future in
// `slot`, through `get_cb`, started once, when JS makes the future. `done(value
// | null, error | null)` runs once it completed, from the client's drain.
napi_value js_admin_future_wait(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "adminFutureWait(handle, slot, done)");
        return NULL;
    }
    Admin* a = admin_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
    AdminFuture* f = future_at(env, a, argv[1]);
    if (f == NULL) return NULL;
    if (f->waiting) {
        napi_throw_error(env, NULL, "the admin future's wait has started");
        return NULL;
    }
    FutureWait* w = (FutureWait*)calloc(1, sizeof(FutureWait));
    if (w == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    w->f = f;
    if (napi_create_reference(env, argv[2], 1, &w->done) != napi_ok) {
        free(w);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    f->waiting = 1;
    f->refs++;
    a->owners++;
    atomic_fetch_add(&stat_waits_started, 1);
    cbq_call_begin(env, &a->queue);
    kafka_common_KafkaFuture_get_cb(f->future, future_on_wait, w);
    return NULL;
}

// adminFutureGet(handle, slot, timeoutMs, done): a timed wait on the future
// in `slot` through `get_with_timeout_cb`, which keeps the time (a negative
// timeout waits no time). `done(error | null, timedOut)`.
napi_value js_admin_future_get(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    int64_t timeout = 0;
    if (argc != 4 || !is_function(env, argv[3]) || napi_get_value_int64(env, argv[2], &timeout) != napi_ok) {
        napi_throw_type_error(env, NULL, "adminFutureGet(handle, slot, timeoutMs, done)");
        return NULL;
    }
    Admin* a = admin_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
    AdminFuture* f = future_at(env, a, argv[1]);
    if (f == NULL) return NULL;
    FutureWait* w = (FutureWait*)calloc(1, sizeof(FutureWait));
    if (w == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    w->f = f;
    w->timed = 1;
    if (napi_create_reference(env, argv[3], 1, &w->done) != napi_ok) {
        free(w);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    f->refs++;
    a->owners++;
    atomic_fetch_add(&stat_waits_started, 1);
    cbq_call_begin(env, &a->queue);
    kafka_common_KafkaFuture_get_with_timeout_cb(f->future, timeout, future_on_timed_wait, w);
    return NULL;
}

// ---- the result's accessors -------------------------------------------------

static const AdminAccessor* accessor_named(napi_env env, const AdminResultDesc* desc, napi_value name_value) {
    char name[64];
    size_t len = 0;
    if (napi_get_value_string_utf8(env, name_value, name, sizeof(name), &len) != napi_ok) {
        napi_throw_type_error(env, NULL, "an accessor name");
        return NULL;
    }
    for (const AdminAccessor* acc = desc->accessors; acc->name != NULL; acc++)
        if (strcmp(acc->name, name) == 0) return acc;
    napi_throw_error(env, NULL, "no such accessor of the admin result");
    return NULL;
}

// adminResultFuture(handle, box, name, key): the future accessor `name` of the
// result hands out, its key `null` (an accessor of no key, `all()`) or a
// string (`config(topic)`), as a slot number. Throws the core's error when the
// accessor fails.
napi_value js_admin_result_future(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value argv[4];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 4) {
        napi_throw_type_error(env, NULL, "adminResultFuture(handle, box, name, key)");
        return NULL;
    }
    Admin* a = admin_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
    AdminResultBox* r = admin_result_unwrap(env, argv[1]);
    if (r == NULL) return NULL;
    const AdminAccessor* acc = accessor_named(env, r->desc, argv[2]);
    if (acc == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_KafkaFuture_t* future = NULL;
    kafka_common_Error_t* err = NULL;
    if (acc->one != NULL) {
        err = FFI_SET(future, acc->one(r->result, &ffi_err));
    } else if (acc->by_str != NULL) {
        char* key = js_string_dup(env, argv[3], "the accessor's key must be a string");
        if (key == NULL) return NULL;
        err = FFI_SET(future, acc->by_str(r->result, key, &ffi_err));
        free(key);
    } else if (acc->by_js != NULL) {
        err = FFI_SET(future, acc->by_js(env, r->result, argv[3], &ffi_err));
        bool pending = false;
        if (err == NULL && future == NULL && napi_is_exception_pending(env, &pending) == napi_ok && pending) return NULL;
    } else {
        napi_throw_error(env, NULL, "the accessor returns a map of futures");
        return NULL;
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (future == NULL) {
        napi_throw_error(env, NULL, "the accessor returned no future");
        return NULL;
    }
    int32_t slot = admin_future_new(env, a, future, !acc->borrowed, acc->borrowed ? argv[1] : NULL, acc->conv);
    if (slot < 0) return NULL;
    napi_value result;
    NAPI_CHECK(env, napi_create_int32(env, slot, &result), NULL);
    return result;
}

// adminResultFutures(handle, box, name): the futures of map accessor `name`
// (`values()`), as an array of [key, slot] in the map's order, each key as the
// accessor's key converter reads it, or null when the accessor gives no map. The map owns its futures: it is kept in
// a box of its own, which each future's context holds.
napi_value js_admin_result_futures(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3) {
        napi_throw_type_error(env, NULL, "adminResultFutures(handle, box, name)");
        return NULL;
    }
    Admin* a = admin_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
    AdminResultBox* r = admin_result_unwrap(env, argv[1]);
    if (r == NULL) return NULL;
    const AdminAccessor* acc = accessor_named(env, r->desc, argv[2]);
    if (acc == NULL) return NULL;
    if (acc->map == NULL) {
        napi_throw_error(env, NULL, "the accessor returns one future");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, acc->map(r->result, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value out;
    // No map: Java's null (topicIdValues() of a request by names).
    if (map == NULL) {
        NAPI_CHECK(env, napi_get_null(env, &out), NULL);
        return out;
    }
    NAPI_CHECK(env, napi_create_array(env, &out), NULL);
    // The map's own box: destroyed, with its futures, once no context holds it.
    napi_value holder = admin_box_new(env, NULL, map, r->desc);
    if (holder == NULL) return NULL;
    int32_t n = 0;
    err = FFI_SET(n, kafka_Map_size_err(map, &ffi_err));
    // The contexts made so far, released when a later entry fails, so no
    // slot stays taken by a future JS never sees.
    int32_t* made = err == NULL && n > 0 ? (int32_t*)calloc((size_t)n, sizeof(int32_t)) : NULL;
    if (err == NULL && n > 0 && made == NULL) {
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    int32_t count = 0;
    int ok = err == NULL;
    for (int32_t i = 0; ok && i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        err = FFI_SET(k, kafka_Map_key_err(map, i, &ffi_err));
        if (err == NULL) err = FFI_SET(v, kafka_Map_value_err(map, i, &ffi_err));
        if (err != NULL) {
            ok = 0;
            break;
        }
        napi_value key = acc->key(env, k);
        int32_t slot = key == NULL ? -1 : admin_future_new(env, a, (kafka_common_KafkaFuture_t*)v, 0, holder, acc->conv);
        if (slot < 0) {
            ok = 0;
            break;
        }
        made[count++] = slot;
        napi_value pair, slot_value;
        if (napi_create_array_with_length(env, 2, &pair) != napi_ok || napi_create_int32(env, slot, &slot_value) != napi_ok
            || napi_set_element(env, pair, 0, key) != napi_ok || napi_set_element(env, pair, 1, slot_value) != napi_ok
            || napi_set_element(env, out, (uint32_t)i, pair) != napi_ok) {
            js_throw_last_napi_error(env, "the futures' array");
            ok = 0;
        }
    }
    if (!ok) {
        for (int32_t j = 0; j < count; j++) future_release(env, a->slots[made[j]]);
    }
    free(made);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (!ok) return NULL;
    return out;
}

// ---- options ----------------------------------------------------------------

char admin_null_options_marker;

void* admin_options_build(napi_env env, const AdminOptionsDesc* desc, napi_value pairs) {
    kafka_common_Error_t* ffi_err = NULL;
    napi_valuetype type;
    if (napi_typeof(env, pairs, &type) != napi_ok) {
        js_throw_last_napi_error(env, "napi_typeof");
        return NULL;
    }
    // Java's null options: the entry point gets NULL, which the FFI reports
    // as its own error.
    if (type == napi_null) return ADMIN_NULL_OPTIONS;
    void* options = NULL;
    kafka_common_Error_t* err = FFI_SET(options, desc->create(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    uint32_t count = 0;
    if (napi_get_array_length(env, pairs, &count) != napi_ok) {
        admin_options_free(env, desc, options);
        napi_throw_type_error(env, NULL, "options must be an array of [setter, value]");
        return NULL;
    }
    for (uint32_t i = 0; i < count; i++) {
        napi_value pair, name_value, value;
        char name[64];
        size_t len = 0;
        if (napi_get_element(env, pairs, i, &pair) != napi_ok || napi_get_element(env, pair, 0, &name_value) != napi_ok
            || napi_get_element(env, pair, 1, &value) != napi_ok
            || napi_get_value_string_utf8(env, name_value, name, sizeof(name), &len) != napi_ok) {
            admin_options_free(env, desc, options);
            napi_throw_type_error(env, NULL, "options must be an array of [setter, value]");
            return NULL;
        }
        const AdminOptionSetter* s = desc->setters;
        while (s->name != NULL && strcmp(s->name, name) != 0) s++;
        if (s->name == NULL) {
            admin_options_free(env, desc, options);
            napi_throw_error(env, NULL, "no such setter of the admin options");
            return NULL;
        }
        int rc = 0;
        err = FFI_SET(rc, s->set(env, options, value, &ffi_err));
        if (rc < 0) {
            error_discard(err);
            admin_options_free(env, desc, options);
            return NULL;
        }
        if (err != NULL) {
            admin_options_free(env, desc, options);
            error_throw_owned(env, err);
            return NULL;
        }
    }
    return options;
}

void admin_options_free(napi_env env, const AdminOptionsDesc* desc, void* options) {
    if (options == NULL || options == ADMIN_NULL_OPTIONS) return;
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 1, FFI_ERR(desc->destroy(options, &ffi_err)));
}

napi_value admin_rpc_return(napi_env env, const AdminOptionsDesc* odesc, void* options, kafka_common_Error_t* err,
                            void* result, const AdminResultDesc* rdesc) {
    kafka_common_Error_t* ffi_err = NULL;
    admin_options_free(env, odesc, options);
    if (err != NULL) {
        if (result != NULL) error_warn(env, 1, FFI_ERR(rdesc->destroy(result, &ffi_err)));
        error_throw_owned(env, err);
        return NULL;
    }
    if (result == NULL) {
        napi_throw_error(env, NULL, "the admin RPC returned no result");
        return NULL;
    }
    return admin_box_new(env, result, NULL, rdesc);
}

int admin_option_timeout(napi_env env, napi_value value, int32_t* out) {
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, value, &type), -1);
    // Java's null: the client's default.api.timeout.ms (-1 in the C API).
    if (type == napi_null) {
        *out = -1;
        return 0;
    }
    int32_t t = 0;
    if (type != napi_number || napi_get_value_int32(env, value, &t) != napi_ok) {
        napi_throw_type_error(env, NULL, "timeoutMs must be a number or null");
        return -1;
    }
    // A negative timeout is sent as 0, which Java's calcDeadlineMs makes of
    // it (root CLAUDE.md, JavaScript Binding Conventions, Signatures, Q22).
    *out = t < 0 ? 0 : t;
    return 0;
}

int admin_option_bool(napi_env env, napi_value value, int8_t* out) {
    bool b = false;
    if (napi_get_value_bool(env, value, &b) != napi_ok) {
        napi_throw_type_error(env, NULL, "the option must be a boolean");
        return -1;
    }
    *out = b ? 1 : 0;
    return 0;
}

int admin_option_int32(napi_env env, napi_value value, int32_t* out) {
    if (napi_get_value_int32(env, value, out) != napi_ok) {
        napi_throw_type_error(env, NULL, "the option must be a number");
        return -1;
    }
    return 0;
}

// A Java `long` option: the bigint TypeScript recorded.
int admin_option_int64(napi_env env, napi_value value, int64_t* out) {
    bool lossless = false;
    if (napi_get_value_bigint_int64(env, value, out, &lossless) != napi_ok || !lossless) {
        napi_throw_type_error(env, NULL, "the option must be a bigint within Java's long");
        return -1;
    }
    return 0;
}

// ---- close and destroy --------------------------------------------------------

typedef struct {
    Admin* a;
    napi_ref done;
} CloseCtx;

static void noop_notify(void* opaque) {
    (void)opaque;
}

// Destroys the class handle of a closed client, on the JS thread, from the
// handle object's finalizer: the core fires the waits still pending here,
// inline, running no JS. Then releases the tsfn.
static void admin_destroy(napi_env env, Admin* a) {
    kafka_common_Error_t* ffi_err = NULL;
    a->queue.stopped = 1;
    a->destroying = 1;
    if (a->owned != NULL) error_warn(env, !a->queue.finalizing, FFI_ERR(kafka_admin_Admin_destroy_err(a->owned, &ffi_err)));
    else error_warn(env, !a->queue.finalizing, FFI_ERR(kafka_admin_MockAdminClient_destroy_err(a->mock, &ffi_err)));
    a->owned = NULL;
    a->mock = NULL;
    a->view = NULL;
    cbq_release(env, &a->queue);
}

// The close completion (kafka_admin_Admin_close_cb_t), on the JS thread: the
// admin client's close always closes it (Java's close throws nothing), so the
// handle object may be collected and Node may exit once no call made on the
// client waits for its answer, this close included until its cbq_call_end.
// `done(closedNow)`.
static void close_on_complete(void* opaque) {
    CloseCtx* ctx = (CloseCtx*)opaque;
    Admin* a = ctx->a;
    napi_env env = a->env;
    int closed_now = a->queue.finalizing ? 0 : cbq_closed(env, &a->queue);
    if (!a->queue.finalizing) {
        napi_value argv[1];
        if (napi_get_boolean(env, closed_now != 0, &argv[0]) == napi_ok) call_ref(env, ctx->done, 1, argv, NULL);
    }
    delete_ref(env, &ctx->done);
    free(ctx);
    cbq_call_end(env, &a->queue);
}

// adminClose(handle, timeoutMs, done): Admin.close() for a timeoutMs of null,
// else close(Duration.ofMillis(timeoutMs)), through the `_cb` twins: the form
// called picks the call. A negative timeoutMs goes to the core as it is, which
// reads it as zero. timeoutMs is a number, or a bigint (Long.MAX_VALUE, the
// no-limit timeout). `done(closedNow)`.
napi_value js_admin_close(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 3 || !is_function(env, argv[2])) {
        napi_throw_type_error(env, NULL, "adminClose(handle, timeoutMs, done)");
        return NULL;
    }
    Admin* a = admin_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
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
    ctx->a = a;
    if (napi_create_reference(env, argv[2], 1, &ctx->done) != napi_ok) {
        free(ctx);
        js_throw_last_napi_error(env, "napi_create_reference");
        return NULL;
    }
    if (!timed) {
        cbq_call_begin(env, &a->queue);
        kafka_admin_Admin_close_cb(a->view, close_on_complete, ctx);
    } else {
        cbq_call_begin(env, &a->queue);
        kafka_admin_Admin_close_with_timeout_cb(a->view, timeout, close_on_complete, ctx);
    }
    return NULL;
}

// ---- the drain and finalization ---------------------------------------------

// The queue's execute: the admin client's __execute_callbacks.
static int32_t admin_execute(void* client, kafka_common_Error_t** out_err) {
    return kafka_admin_Admin__execute_callbacks_err(((Admin*)client)->view, out_err);
}

// The queue's on_tsfn_finalize: the tsfn's ownership of the struct ends.
static void admin_tsfn_finalized(napi_env env, void* client) {
    (void)env;
    Admin* a = (Admin*)client;
    if (admin_alive(a) && !a->destroying) {
        // Environment teardown finalized the tsfn before the handle object.
        kafka_common_Error_t* ffi_err = NULL;
        FFI_DISCARD(kafka_admin_Admin__set_callbacks_notify_err(a->view, noop_notify, NULL, &ffi_err));
    }
    admin_release(a);
}

// The handle object's finalizer: a closed client is destroyed here, running no
// JS. An open one is never closed or destroyed by a finalizer: only an
// environment teardown finalizes one, and it is left to the core.
static void admin_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    Admin* a = (Admin*)data;
    a->queue.finalizing = 1;
    int open = admin_alive(a) && !cbq_is_closed(&a->queue);
    if (admin_alive(a) && cbq_is_closed(&a->queue)) admin_destroy(env, a);
    cbq_detach(env, &a->queue);
    if (!open) admin_release(a);
}

// ---- construction -------------------------------------------------------------

static void class_destroy(napi_env env, kafka_admin_Admin_t* owned, kafka_admin_MockAdminClient_t* mock) {
    kafka_common_Error_t* ffi_err = NULL;
    if (owned != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_Admin_destroy_err(owned, &ffi_err)));
    if (mock != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_MockAdminClient_destroy_err(mock, &ffi_err)));
}

// The native handle over class handle `owned` or `mock` (one is NULL), which
// it owns from now on. Throws, and destroys the class handle, on failure.
static napi_value admin_handle_new(napi_env env, kafka_admin_Admin_t* owned, kafka_admin_MockAdminClient_t* mock) {
    kafka_common_Error_t* ffi_err = NULL;
    Admin* a = (Admin*)calloc(1, sizeof(Admin));
    if (a == NULL) {
        class_destroy(env, owned, mock);
        napi_throw_error(env, NULL, "out of memory");
        return NULL;
    }
    atomic_fetch_add(&stat_admins_made, 1);
    a->env = env;
    a->owned = owned;
    a->mock = mock;
    a->free_head = -1;
    kafka_common_Error_t* err = NULL;
    if (owned != NULL) a->view = owned;
    else err = FFI_SET(a->view, kafka_admin_MockAdminClient__as_Admin_err(mock, &ffi_err));
    if (err == NULL
        && cbq_init(env, &a->queue, "confluent-kafka admin client callbacks", a, admin_execute, admin_tsfn_finalized)
               != 0) {
        class_destroy(env, owned, mock);
        free(a);
        atomic_fetch_add(&stat_admins_freed, 1);
        return NULL;
    }
    if (err != NULL) {
        class_destroy(env, owned, mock);
        free(a);
        atomic_fetch_add(&stat_admins_freed, 1);
        error_throw_owned(env, err);
        return NULL;
    }
    a->owners = 1;  // the tsfn
    napi_value result = NULL;
    int thrown = 0;
    if (napi_create_object(env, &result) != napi_ok || napi_type_tag_object(env, result, &ADMIN_TAG) != napi_ok
        || napi_wrap(env, result, a, admin_finalize, NULL, NULL) != napi_ok) {
        js_throw_last_napi_error(env, "napi_wrap");
        thrown = 1;
    } else {
        a->owners = 2;  // and the handle object
        if (cbq_attach(env, &a->queue, result) != 0) thrown = 1;
    }
    if (!thrown) err = FFI_ERR(kafka_admin_Admin__set_callbacks_notify_err(a->view, cbq_notify, &a->queue, &ffi_err));
    if (err != NULL || thrown) {
        class_destroy(env, owned, mock);
        a->owned = NULL;
        a->mock = NULL;
        a->destroying = 1;
        cbq_release(env, &a->queue);
        if (err != NULL) error_throw_owned(env, err);
        return NULL;
    }
    return result;
}

// adminCreate(entries): a native handle over Admin.create(configs), `entries`
// an array of [key, value] byte pairs (encodeLikeJava's). Throws a KafkaError
// when the configuration is rejected or the client cannot be built.
napi_value js_admin_create(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    bool is_array = false;
    if (argc != 1 || napi_is_array(env, argv[0], &is_array) != napi_ok || !is_array) {
        napi_throw_type_error(env, NULL, "adminCreate(entries)");
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
    kafka_admin_AdminClientConfig_t* config = NULL;
    kafka_admin_Admin_t* owned = NULL;
    int thrown = 0;
    // A C-built kafka_Map_t borrows its char * keys and values: freed after it.
    kafka_common_Error_t* err = FFI_SET(props, kafka_Map_new_err(&ffi_err));
    for (uint32_t i = 0; err == NULL && !thrown && i < count; i++) {
        napi_value entry, k, v;
        if (napi_get_element(env, argv[0], i, &entry) != napi_ok || napi_get_element(env, entry, 0, &k) != napi_ok
            || napi_get_element(env, entry, 1, &v) != napi_ok) {
            js_throw_last_napi_error(env, "reading a config entry");
            thrown = 1;
            break;
        }
        strings[2 * i] = js_bytes_cstring(env, k, "config keys must be strings");
        strings[2 * i + 1] = strings[2 * i] != NULL ? js_bytes_cstring(env, v, "config values must be strings") : NULL;
        if (strings[2 * i + 1] == NULL) {
            thrown = 1;
            break;
        }
        err = FFI_ERR(kafka_Map_put_err(props, strings[2 * i], strings[2 * i + 1], &ffi_err));
    }
    // AdminClientConfig_new and AdminClient_create take an out_err of their
    // own (no `_err` twin).
    if (err == NULL && !thrown) err = FFI_SET(config, kafka_admin_AdminClientConfig_new(props, &ffi_err));
    if (props != NULL) error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(props, &ffi_err)));
    for (uint32_t i = 0; i < count * 2; i++) free(strings[i]);
    free(strings);
    if (err == NULL && !thrown) err = FFI_SET(owned, kafka_admin_AdminClient_create(config, &ffi_err));
    if (config != NULL) error_warn(env, 1, FFI_ERR(kafka_admin_AdminClientConfig_destroy_err(config, &ffi_err)));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    if (thrown) return NULL;
    return admin_handle_new(env, owned, NULL);
}

// ---- MockAdminClient ----------------------------------------------------------

Admin* admin_mock_unwrap(napi_env env, napi_value handle) {
    Admin* a = admin_unwrap(env, handle);
    if (a == NULL) return NULL;
    if (a->mock == NULL) {
        napi_throw_type_error(env, NULL, "not a MockAdminClient handle");
        return NULL;
    }
    return a;
}

// A JS node's fields ({ id, host, port, rack, isFenced }) as an owned
// kafka_common_Node_t, or NULL with an exception pending.
kafka_common_Node_t* admin_node_from_js(napi_env env, napi_value node) {
    napi_value v;
    int32_t id = 0, port = 0;
    bool fenced = false;
    napi_valuetype type;
    char* host = NULL;
    char* rack = NULL;
    if (napi_get_named_property(env, node, "id", &v) != napi_ok || napi_get_value_int32(env, v, &id) != napi_ok
        || napi_get_named_property(env, node, "port", &v) != napi_ok || napi_get_value_int32(env, v, &port) != napi_ok
        || napi_get_named_property(env, node, "isFenced", &v) != napi_ok || napi_get_value_bool(env, v, &fenced) != napi_ok) {
        napi_throw_type_error(env, NULL, "a node's fields");
        return NULL;
    }
    if (napi_get_named_property(env, node, "host", &v) != napi_ok || napi_typeof(env, v, &type) != napi_ok) return NULL;
    if (type != napi_null && (host = js_string_dup(env, v, "a node's host must be a string")) == NULL) return NULL;
    if (napi_get_named_property(env, node, "rack", &v) != napi_ok || napi_typeof(env, v, &type) != napi_ok) {
        free(host);
        return NULL;
    }
    if (type != napi_null && (rack = js_string_dup(env, v, "a node's rack must be a string")) == NULL) {
        free(host);
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Node_t* n = NULL;
    kafka_common_Error_t* err =
        FFI_SET(n, kafka_common_Node_with_rack_is_fenced_err(id, host, port, rack, fenced ? 1 : 0, &ffi_err));
    free(host);
    free(rack);
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return n;
}

static void node_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_common_Node_destroy_err((kafka_common_Node_t*)item, out_err);
}

void admin_list_free(napi_env env, kafka_List_t* list, void (*destroy)(void*, kafka_common_Error_t**)) {
    if (list == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    for (int32_t i = 0; err == NULL && destroy != NULL && i < n; i++) {
        void* item = NULL;
        err = FFI_SET(item, kafka_List_get_err(list, i, &ffi_err));
        if (err == NULL && item != NULL) err = FFI_ERR(destroy(item, &ffi_err));
    }
    error_warn(env, 1, err);
    error_warn(env, 1, FFI_ERR(kafka_List_destroy_err(list, &ffi_err)));
}

static void string_list_destroy(void* item, kafka_common_Error_t** out_err) {
    kafka_List_t* list = (kafka_List_t*)item;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_List_size_err(list, &ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* s = NULL;
        err = FFI_SET(s, kafka_List_get_err(list, i, &ffi_err));
        if (err == NULL) free(s);
    }
    error_discard(err);
    kafka_List_destroy_err(list, out_err);
}

// An array of JS node fields as an owned kafka_List_t of owned nodes, or NULL
// with an exception pending; `null` is NULL with none (*is_null set).
kafka_List_t* admin_nodes_from_js(napi_env env, napi_value array, int* is_null) {
    napi_valuetype type;
    *is_null = 0;
    if (napi_typeof(env, array, &type) != napi_ok) return NULL;
    if (type == napi_null) {
        *is_null = 1;
        return NULL;
    }
    uint32_t count = 0;
    if (napi_get_array_length(env, array, &count) != napi_ok) {
        napi_throw_type_error(env, NULL, "the nodes must be an array");
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* list = NULL;
    kafka_common_Error_t* err = FFI_SET(list, kafka_List_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    for (uint32_t i = 0; i < count; i++) {
        napi_value item;
        kafka_common_Node_t* n = NULL;
        if (napi_get_element(env, array, i, &item) != napi_ok || (n = admin_node_from_js(env, item)) == NULL) {
            admin_list_free(env, list, node_destroy);
            return NULL;
        }
        err = FFI_ERR(kafka_List_add_err(list, n, &ffi_err));
        if (err != NULL) {
            FFI_DISCARD(kafka_common_Node_destroy_err(n, &ffi_err));
            admin_list_free(env, list, node_destroy);
            error_throw_owned(env, err);
            return NULL;
        }
    }
    return list;
}

// mockAdminCreate(brokers, controller): Java's MockAdminClient() when both are
// undefined, else MockAdminClient(brokers, controller), each node as its
// fields; a null one goes to the FFI as NULL, which reports it.
napi_value js_mock_admin_create(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_MockAdminClient_t* mock = NULL;
    kafka_common_Error_t* err = NULL;
    if (argc == 0) {
        err = FFI_SET(mock, kafka_admin_MockAdminClient_new_err(&ffi_err));
    } else if (argc == 2) {
        int brokers_null = 0;
        kafka_List_t* brokers = admin_nodes_from_js(env, argv[0], &brokers_null);
        if (brokers == NULL && !brokers_null) return NULL;
        napi_valuetype type;
        kafka_common_Node_t* controller = NULL;
        if (napi_typeof(env, argv[1], &type) != napi_ok
            || (type != napi_null && (controller = admin_node_from_js(env, argv[1])) == NULL)) {
            admin_list_free(env, brokers, node_destroy);
            return NULL;
        }
        err = FFI_SET(mock, kafka_admin_MockAdminClient_with_brokers_controller(brokers, controller, &ffi_err));
        admin_list_free(env, brokers, node_destroy);
        if (controller != NULL) error_warn(env, 1, FFI_ERR(kafka_common_Node_destroy_err(controller, &ffi_err)));
    } else {
        napi_throw_type_error(env, NULL, "mockAdminCreate() or mockAdminCreate(brokers, controller)");
        return NULL;
    }
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return admin_handle_new(env, NULL, mock);
}

// mockAdminController(handle, node): Java's MockAdminClient.controller(Node).
napi_value js_mock_admin_controller(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2) {
        napi_throw_type_error(env, NULL, "mockAdminController(handle, node)");
        return NULL;
    }
    Admin* a = admin_mock_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
    napi_valuetype type;
    kafka_common_Node_t* node = NULL;
    NAPI_CHECK(env, napi_typeof(env, argv[1], &type), NULL);
    if (type != napi_null && (node = admin_node_from_js(env, argv[1])) == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_admin_MockAdminClient_set_controller(a->mock, node, &ffi_err));
    if (node != NULL) error_warn(env, 1, FFI_ERR(kafka_common_Node_destroy_err(node, &ffi_err)));
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockAdminTimeoutNextRequest(handle, numberOfRequests).
napi_value js_mock_admin_timeout_next_request(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    int32_t n = 0;
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 2 || napi_get_value_int32(env, argv[1], &n) != napi_ok) {
        napi_throw_type_error(env, NULL, "mockAdminTimeoutNextRequest(handle, numberOfRequests)");
        return NULL;
    }
    Admin* a = admin_mock_unwrap(env, argv[0]);
    if (a == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = FFI_ERR(kafka_admin_MockAdminClient_timeout_next_request_err(a->mock, n, &ffi_err));
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// ---- MockAdminClient.Builder --------------------------------------------------

static void builder_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    kafka_common_Error_t* ffi_err = NULL;
    error_warn(env, 0, FFI_ERR(kafka_admin_MockAdminClient_Builder_destroy_err((kafka_admin_MockAdminClient_Builder_t*)data, &ffi_err)));
}

// mockAdminBuilderNew(): Java's new MockAdminClient.Builder(), a handle whose
// finalizer destroys it.
napi_value js_mock_admin_builder_new(napi_env env, napi_callback_info info) {
    (void)info;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_MockAdminClient_Builder_t* b = NULL;
    kafka_common_Error_t* err = FFI_SET(b, kafka_admin_MockAdminClient_Builder_new_err(&ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    napi_value obj;
    if (napi_create_object(env, &obj) != napi_ok || napi_type_tag_object(env, obj, &BUILDER_TAG) != napi_ok
        || napi_wrap(env, obj, b, builder_finalize, NULL, NULL) != napi_ok) {
        FFI_DISCARD(kafka_admin_MockAdminClient_Builder_destroy_err(b, &ffi_err));
        js_throw_last_napi_error(env, "napi_wrap");
        return NULL;
    }
    return obj;
}

// An array of [string, number] pairs as an owned kafka_Map_t of malloc'ed
// keys to boxed int16 values (`shorts`) or of malloc'ed strings, or NULL with
// an exception pending; `null` is NULL with none (*is_null set).
static kafka_Map_t* pairs_map(napi_env env, napi_value pairs, int shorts, int* is_null) {
    napi_valuetype type;
    *is_null = 0;
    if (napi_typeof(env, pairs, &type) != napi_ok) return NULL;
    if (type == napi_null) {
        *is_null = 1;
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_Map_t* map = NULL;
    kafka_common_Error_t* err = FFI_SET(map, kafka_Map_new_err(&ffi_err));
    uint32_t count = 0;
    if (err == NULL && napi_get_array_length(env, pairs, &count) != napi_ok) {
        admin_map_free(env, map);
        napi_throw_type_error(env, NULL, "a map's entries");
        return NULL;
    }
    for (uint32_t i = 0; err == NULL && i < count; i++) {
        napi_value pair, k, v;
        char* key = NULL;
        void* value = NULL;
        if (napi_get_element(env, pairs, i, &pair) != napi_ok || napi_get_element(env, pair, 0, &k) != napi_ok
            || napi_get_element(env, pair, 1, &v) != napi_ok || (key = js_string_dup(env, k, "a map key must be a string")) == NULL) {
            admin_map_free(env, map);
            return NULL;
        }
        if (shorts) {
            int32_t n = 0;
            int16_t* box = (int16_t*)malloc(sizeof(int16_t));
            if (box == NULL || napi_get_value_int32(env, v, &n) != napi_ok) {
                free(box);
                free(key);
                admin_map_free(env, map);
                napi_throw_type_error(env, NULL, "a map value must be a number");
                return NULL;
            }
            *box = (int16_t)n;
            value = box;
        } else if ((value = js_string_dup(env, v, "a map value must be a string")) == NULL) {
            free(key);
            admin_map_free(env, map);
            return NULL;
        }
        err = FFI_ERR(kafka_Map_put_err(map, key, value, &ffi_err));
        if (err != NULL) {
            free(key);
            free(value);
        }
    }
    if (err != NULL) {
        admin_map_free(env, map);
        error_throw_owned(env, err);
        return NULL;
    }
    return map;
}

void admin_map_free(napi_env env, kafka_Map_t* map) {
    if (map == NULL) return;
    kafka_common_Error_t* ffi_err = NULL;
    int32_t n = 0;
    kafka_common_Error_t* err = FFI_SET(n, kafka_Map_size_err(map, &ffi_err));
    for (int32_t i = 0; err == NULL && i < n; i++) {
        void* k = NULL;
        void* v = NULL;
        err = FFI_SET(k, kafka_Map_key_err(map, i, &ffi_err));
        if (err == NULL) err = FFI_SET(v, kafka_Map_value_err(map, i, &ffi_err));
        if (err == NULL) {
            free(k);
            free(v);
        }
    }
    error_warn(env, 1, err);
    error_warn(env, 1, FFI_ERR(kafka_Map_destroy_err(map, &ffi_err)));
}

// An array of arrays of strings as an owned kafka_List_t of owned lists of
// malloc'ed strings, or NULL with an exception pending; `null` is NULL with
// none (*is_null set).
static kafka_List_t* string_lists(napi_env env, napi_value arrays, int* is_null) {
    napi_valuetype type;
    *is_null = 0;
    if (napi_typeof(env, arrays, &type) != napi_ok) return NULL;
    if (type == napi_null) {
        *is_null = 1;
        return NULL;
    }
    kafka_common_Error_t* ffi_err = NULL;
    kafka_List_t* outer = NULL;
    kafka_common_Error_t* err = FFI_SET(outer, kafka_List_new_err(&ffi_err));
    uint32_t count = 0;
    if (err == NULL && napi_get_array_length(env, arrays, &count) != napi_ok) {
        admin_list_free(env, outer, string_list_destroy);
        napi_throw_type_error(env, NULL, "brokerLogDirs must be an array of arrays of strings");
        return NULL;
    }
    for (uint32_t i = 0; err == NULL && i < count; i++) {
        napi_value inner_value;
        uint32_t n = 0;
        kafka_List_t* inner = NULL;
        if (napi_get_element(env, arrays, i, &inner_value) != napi_ok || napi_get_array_length(env, inner_value, &n) != napi_ok) {
            admin_list_free(env, outer, string_list_destroy);
            napi_throw_type_error(env, NULL, "brokerLogDirs must be an array of arrays of strings");
            return NULL;
        }
        err = FFI_SET(inner, kafka_List_new_err(&ffi_err));
        if (err == NULL) {
            err = FFI_ERR(kafka_List_add_err(outer, inner, &ffi_err));
            // Not added: freed here, the outer list does not own it.
            if (err != NULL) FFI_DISCARD(string_list_destroy(inner, &ffi_err));
        }
        for (uint32_t j = 0; err == NULL && j < n; j++) {
            napi_value s_value;
            char* s = NULL;
            if (napi_get_element(env, inner_value, j, &s_value) != napi_ok
                || (s = js_string_dup(env, s_value, "a log dir must be a string")) == NULL) {
                admin_list_free(env, outer, string_list_destroy);
                return NULL;
            }
            err = FFI_ERR(kafka_List_add_err(inner, s, &ffi_err));
            if (err != NULL) free(s);
        }
    }
    if (err != NULL) {
        admin_list_free(env, outer, string_list_destroy);
        error_throw_owned(env, err);
        return NULL;
    }
    return outer;
}

// mockAdminBuilderSet(builder, setter, value): one of Java's Builder setters,
// run at once, as Java runs them: clusterId, brokers (node fields), numBrokers,
// controller (an index), brokerLogDirs, defaultReplicationFactor,
// usingRaftController, defaultPartitions, featureLevels,
// minSupportedFeatureLevels, maxSupportedFeatureLevels (arrays of [name,
// level]) and defaultGroupConfigs (an array of [name, value]). A null goes to
// the FFI as NULL, which reports it.
napi_value js_mock_admin_builder_set(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value argv[3];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    char name[48];
    size_t len = 0;
    if (argc != 3 || napi_get_value_string_utf8(env, argv[1], name, sizeof(name), &len) != napi_ok) {
        napi_throw_type_error(env, NULL, "mockAdminBuilderSet(builder, setter, value)");
        return NULL;
    }
    kafka_admin_MockAdminClient_Builder_t* b =
        (kafka_admin_MockAdminClient_Builder_t*)unwrap_tagged(env, argv[0], &BUILDER_TAG, "not a MockAdminClient.Builder handle");
    if (b == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_t* err = NULL;
    napi_valuetype type;
    NAPI_CHECK(env, napi_typeof(env, argv[2], &type), NULL);
    int is_null = 0;
    if (strcmp(name, "clusterId") == 0) {
        char* s = NULL;
        if (type != napi_null && (s = js_string_dup(env, argv[2], "clusterId must be a string")) == NULL) return NULL;
        err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_cluster_id_err(b, s, &ffi_err));
        free(s);
    } else if (strcmp(name, "brokers") == 0) {
        kafka_List_t* nodes = admin_nodes_from_js(env, argv[2], &is_null);
        if (nodes == NULL && !is_null) return NULL;
        err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_brokers(b, nodes, &ffi_err));
        admin_list_free(env, nodes, node_destroy);
    } else if (strcmp(name, "numBrokers") == 0 || strcmp(name, "controller") == 0
               || strcmp(name, "defaultReplicationFactor") == 0 || strcmp(name, "defaultPartitions") == 0) {
        int32_t n = 0;
        if (napi_get_value_int32(env, argv[2], &n) != napi_ok) {
            napi_throw_type_error(env, NULL, "the setter takes a number");
            return NULL;
        }
        if (name[0] == 'n') err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_num_brokers(b, n, &ffi_err));
        else if (name[0] == 'c') err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_controller(b, n, &ffi_err));
        else if (strcmp(name, "defaultPartitions") == 0)
            err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_default_partitions_err(b, (int16_t)n, &ffi_err));
        else err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_default_replication_factor_err(b, n, &ffi_err));
    } else if (strcmp(name, "usingRaftController") == 0) {
        bool v = false;
        if (napi_get_value_bool(env, argv[2], &v) != napi_ok) {
            napi_throw_type_error(env, NULL, "usingRaftController takes a boolean");
            return NULL;
        }
        err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_using_raft_controller_err(b, v ? 1 : 0, &ffi_err));
    } else if (strcmp(name, "brokerLogDirs") == 0) {
        kafka_List_t* dirs = string_lists(env, argv[2], &is_null);
        if (dirs == NULL && !is_null) return NULL;
        err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_broker_log_dirs_err(b, dirs, &ffi_err));
        admin_list_free(env, dirs, string_list_destroy);
    } else if (strcmp(name, "defaultGroupConfigs") == 0) {
        kafka_Map_t* map = pairs_map(env, argv[2], 0, &is_null);
        if (map == NULL && !is_null) return NULL;
        err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_default_group_configs_err(b, map, &ffi_err));
        admin_map_free(env, map);
    } else if (strcmp(name, "featureLevels") == 0 || strcmp(name, "minSupportedFeatureLevels") == 0
               || strcmp(name, "maxSupportedFeatureLevels") == 0) {
        kafka_Map_t* map = pairs_map(env, argv[2], 1, &is_null);
        if (map == NULL && !is_null) return NULL;
        if (name[0] == 'f') err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_feature_levels_err(b, map, &ffi_err));
        else if (name[1] == 'i')
            err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_min_supported_feature_levels_err(b, map, &ffi_err));
        else err = FFI_ERR(kafka_admin_MockAdminClient_Builder_set_max_supported_feature_levels_err(b, map, &ffi_err));
        admin_map_free(env, map);
    } else {
        napi_throw_error(env, NULL, "no such MockAdminClient.Builder setter");
        return NULL;
    }
    if (err != NULL) error_throw_owned(env, err);
    return NULL;
}

// mockAdminBuilderBuild(builder): Java's Builder.build(), a native handle over
// the mock; the builder stays usable.
napi_value js_mock_admin_builder_build(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CHECK(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL), NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "mockAdminBuilderBuild(builder)");
        return NULL;
    }
    kafka_admin_MockAdminClient_Builder_t* b =
        (kafka_admin_MockAdminClient_Builder_t*)unwrap_tagged(env, argv[0], &BUILDER_TAG, "not a MockAdminClient.Builder handle");
    if (b == NULL) return NULL;
    kafka_common_Error_t* ffi_err = NULL;
    kafka_admin_MockAdminClient_t* mock = NULL;
    kafka_common_Error_t* err = FFI_SET(mock, kafka_admin_MockAdminClient_Builder_build(b, &ffi_err));
    if (err != NULL) {
        error_throw_owned(env, err);
        return NULL;
    }
    return admin_handle_new(env, NULL, mock);
}

// ---- stats ----------------------------------------------------------------------

static int stat_put(napi_env env, napi_value obj, const char* name, atomic_long* count) {
    napi_value v;
    if (napi_create_int64(env, (int64_t)atomic_load(count), &v) != napi_ok || napi_set_named_property(env, obj, name, v) != napi_ok)
        return -1;
    return 0;
}

// adminStats(): the counts of the admin client's native objects, for the
// lifetime tests. Test-only.
napi_value js_admin_stats(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value obj;
    NAPI_CHECK(env, napi_create_object(env, &obj), NULL);
    if (stat_put(env, obj, "adminsMade", &stat_admins_made) != 0 || stat_put(env, obj, "adminsFreed", &stat_admins_freed) != 0
        || stat_put(env, obj, "resultsMade", &stat_results_made) != 0 || stat_put(env, obj, "resultsFreed", &stat_results_freed) != 0
        || stat_put(env, obj, "futuresMade", &stat_futures_made) != 0 || stat_put(env, obj, "futuresFreed", &stat_futures_freed) != 0
        || stat_put(env, obj, "waitsStarted", &stat_waits_started) != 0
        || stat_put(env, obj, "waitsCompleted", &stat_waits_completed) != 0) {
        js_throw_last_napi_error(env, "adminStats");
        return NULL;
    }
    return obj;
}

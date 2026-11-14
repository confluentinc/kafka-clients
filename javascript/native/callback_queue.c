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

// The callback queue of a client (util.h): notify hook -> threadsafe function
// -> drain of the client's __execute_callbacks on the JS thread, keep-alive
// while operations are pending, and the threadsafe function's teardown. It
// holds no Kafka logic and runs no thread of its own.
//
// Every callback the core queues for a client runs inside the drain, on the JS
// main thread, so it builds its JS values there directly. The drain runs from
// the threadsafe function's call_js (what the notify hook asked for) or by
// hand from a client call (the producer's `pump`, run by send()); drains never
// nest: a callback that calls back into the client (a send from a delivery
// callback) finds `draining` set, and so does a drain once the client's
// destroy began (`stopped`) or while the handle is finalized.
//
// Liveness: the threadsafe function is ref'd while operations are pending and
// unref'd when the count is back to zero, so an idle client does not keep the
// process alive. The handle object is held by a strong reference from its
// creation until the client is destroyed: garbage collection never finalizes
// a client that was not closed (no finalizer closes or destroys one); only
// the environment's teardown does, and a client released after its destroy
// is collected as any object.
//
// The notify hook and the threadsafe function's end: replacing the hook with
// __set_callbacks_notify does not wait for a call of the old one in flight on
// another thread, so swapping in a no-op before the tsfn goes away would not
// keep such a call off a deleted tsfn (environment teardown deletes it while
// the client still runs). The hook therefore calls the tsfn only under
// `notify_mtx` and only while `notify_open`, which the JS thread clears under
// the same lock before the tsfn is released or once it is finalized: a hook
// call in flight then either finished its tsfn call or sees it closed.

#include "util.h"

// Stops the notify hook from reaching the tsfn; once this returns, no hook
// call is inside napi_call_threadsafe_function. JS thread.
static void notify_close(CallbackQueue* q) {
    pthread_mutex_lock(&q->notify_mtx);
    q->notify_open = 0;
    pthread_mutex_unlock(&q->notify_mtx);
}

// Refs the tsfn while there is pending work, unrefs it when idle or torn
// down. JS thread only.
static void update_liveness(napi_env env, CallbackQueue* q) {
    int want = q->pending > 0 && q->tsfn_alive && !q->finalizing;
    if (want == q->busy) return;
    if (want) napi_ref_threadsafe_function(env, q->tsfn);
    else if (q->tsfn_alive) napi_unref_threadsafe_function(env, q->tsfn);
    q->busy = want;
}

// The tsfn's call_js, on the JS thread: the drain the notify hook asked for.
static void call_js(napi_env env, napi_value js_cb, void* context, void* data) {
    (void)js_cb;
    (void)data;
    if (env == NULL) return;
    cbq_drain(env, (CallbackQueue*)context);
}

// The tsfn's finalizer: environment teardown, or the release in cbq_release.
// A hook call in flight finishes its tsfn call before Node deletes the tsfn.
static void tsfn_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    CallbackQueue* q = (CallbackQueue*)data;
    notify_close(q);
    q->tsfn_alive = 0;
    q->on_tsfn_finalize(env, q->client);
}

int cbq_init(napi_env env, CallbackQueue* q, const char* name, void* client, cbq_execute_fn execute,
             cbq_client_fn after_drain, cbq_client_fn on_tsfn_finalize) {
    q->env = env;
    q->client = client;
    q->execute = execute;
    q->after_drain = after_drain;
    q->on_tsfn_finalize = on_tsfn_finalize;
    if (pthread_mutex_init(&q->notify_mtx, NULL) != 0) {
        napi_throw_error(env, NULL, "pthread_mutex_init failed");
        return -1;
    }
    napi_value resource_name;
    if (napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &resource_name) != napi_ok
        || napi_create_threadsafe_function(env, NULL, NULL, resource_name, 0, 1, q, tsfn_finalize, q, call_js,
                                           &q->tsfn) != napi_ok) {
        pthread_mutex_destroy(&q->notify_mtx);
        js_throw_last_napi_error(env, "napi_create_threadsafe_function");
        return -1;
    }
    q->tsfn_alive = 1;
    q->notify_open = 1;
    // Idle until something is pending.
    napi_unref_threadsafe_function(env, q->tsfn);
    return 0;
}

int cbq_attach(napi_env env, CallbackQueue* q, napi_value handle) {
    // Strong until cbq_release: the handle is never collected unclosed.
    NAPI_CHECK(env, napi_create_reference(env, handle, 1, &q->self_ref), -1);
    q->self_strong = 1;
    return 0;
}

void cbq_notify(void* opaque) {
    CallbackQueue* q = (CallbackQueue*)opaque;
    pthread_mutex_lock(&q->notify_mtx);
    if (q->notify_open) napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);
    pthread_mutex_unlock(&q->notify_mtx);
}

void cbq_schedule(CallbackQueue* q) {
    if (q->tsfn_alive) napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);
}

void cbq_pending_add(napi_env env, CallbackQueue* q, int64_t delta) {
    q->pending += delta;
    update_liveness(env, q);
}

void cbq_drain(napi_env env, CallbackQueue* q) {
    if (q->stopped || q->finalizing || q->draining) return;
    kafka_common_Error_t* ffi_err = NULL;
    q->draining = 1;
    while (!q->stopped) {
        int32_t ran = q->execute(q->client, &ffi_err);
        error_warn(env, !q->finalizing, ffi_take(&ffi_err));
        if (ran <= 0) break;
    }
    q->draining = 0;
    if (q->after_drain != NULL) q->after_drain(env, q->client);
}

void cbq_release(napi_env env, CallbackQueue* q) {
    q->stopped = 1;
    notify_close(q);
    if (q->tsfn_alive) {
        if (q->busy) napi_unref_threadsafe_function(env, q->tsfn);
        q->tsfn_alive = 0;
        // Its finalizer runs later, from the event loop.
        napi_release_threadsafe_function(q->tsfn, napi_tsfn_release);
    }
    update_liveness(env, q);
    // The client is destroyed: its handle object may now be collected.
    if (q->self_strong && !q->finalizing && q->self_ref != NULL) napi_reference_unref(env, q->self_ref, NULL);
    q->self_strong = 0;
}

void cbq_detach(napi_env env, CallbackQueue* q) {
    q->finalizing = 1;
    delete_ref(env, &q->self_ref);
}

void cbq_dispose(CallbackQueue* q) {
    pthread_mutex_destroy(&q->notify_mtx);
}

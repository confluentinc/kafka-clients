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
// -> drain of the client's __execute_callbacks on the JS thread, the client's
// hold on Node and on its handle object, and the threadsafe function's
// teardown. It holds no Kafka logic and runs no thread of its own.
//
// Every callback the core queues for a client runs inside the drain, on the JS
// main thread, so it builds its JS values there directly. The drain runs from
// the threadsafe function's call_js (what the notify hook asked for) or by
// hand from a client call (the producer's `pump`, run by send()); drains never
// nest: a callback that calls back into the client (a send from a delivery
// callback) finds `draining` set, and so does a drain once the client's
// destroy began (`stopped`) or while the handle is finalized.
//
// Liveness (javascript/CLAUDE.md, "Threads and liveness"): the threadsafe
// function stays referenced, as Node-API creates it, from the client's
// creation until the core reports the client closed (cbq_closed), so an open
// client keeps Node running and nothing is counted; it is unreferenced then,
// and stays alive for the completions of calls made after the close. The
// handle object is held by a strong reference until then too, so garbage
// collection never finalizes an open client; once closed it is weak, and the
// handle object's finalizer destroys the client.
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
             cbq_client_fn on_tsfn_finalize) {
    q->env = env;
    q->client = client;
    q->execute = execute;
    q->on_tsfn_finalize = on_tsfn_finalize;
    if (pthread_mutex_init(&q->notify_mtx, NULL) != 0) {
        napi_throw_error(env, NULL, "pthread_mutex_init failed");
        return -1;
    }
    napi_value resource_name;
    // Referenced, as Node-API creates it: the client keeps Node running until
    // it is closed (cbq_closed).
    if (napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &resource_name) != napi_ok
        || napi_create_threadsafe_function(env, NULL, NULL, resource_name, 0, 1, q, tsfn_finalize, q, call_js,
                                           &q->tsfn) != napi_ok) {
        pthread_mutex_destroy(&q->notify_mtx);
        js_throw_last_napi_error(env, "napi_create_threadsafe_function");
        return -1;
    }
    q->tsfn_alive = 1;
    q->notify_open = 1;
    return 0;
}

int cbq_attach(napi_env env, CallbackQueue* q, napi_value handle) {
    // Strong until cbq_closed: an open client's handle is never collected.
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

int cbq_closed(napi_env env, CallbackQueue* q) {
    if (!q->self_strong) return 0;
    // Node may exit from now on; the tsfn itself stays, for late completions.
    if (q->tsfn_alive) napi_unref_threadsafe_function(env, q->tsfn);
    // The handle object may now be collected: its finalizer destroys the client.
    if (q->self_ref != NULL) napi_reference_unref(env, q->self_ref, NULL);
    q->self_strong = 0;
    return 1;
}

int cbq_is_closed(const CallbackQueue* q) {
    return !q->self_strong;
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
}

void cbq_release(napi_env env, CallbackQueue* q) {
    (void)env;
    q->stopped = 1;
    notify_close(q);
    if (q->tsfn_alive) {
        q->tsfn_alive = 0;
        // Its finalizer runs later, from the event loop.
        napi_release_threadsafe_function(q->tsfn, napi_tsfn_release);
    }
}

void cbq_detach(napi_env env, CallbackQueue* q) {
    q->finalizing = 1;
    delete_ref(env, &q->self_ref);
}

void cbq_dispose(CallbackQueue* q) {
    pthread_mutex_destroy(&q->notify_mtx);
}

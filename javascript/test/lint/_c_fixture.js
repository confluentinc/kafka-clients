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

'use strict';

// A small addon for the N rules' fixture projects: a header, the out_err
// macros, a callback queue, a producer and a record reader that pass every N
// rule under the test allowlists below, and a lib/ whose delivery completion
// runs the user callback before it completes the future.

const HEADER = `void kafka_x_P_flush(const kafka_x_P_t *self_, kafka_common_Error_t **out_err);

void kafka_x_P_flush_cb(const kafka_x_P_t *self_, kafka_x_P_flush_cb_t cb, void *opaque);

int32_t kafka_x_P__execute_callbacks_err(const kafka_x_P_t *self_, kafka_common_Error_t **out_err);

void kafka_x_P__set_callbacks_notify_err(const kafka_x_P_t *self_, kafka_x_notify_fn_t notify, void *opaque, kafka_common_Error_t **out_err);

void kafka_x_P__set_callback_result_err(const kafka_x_P_t *self_, int64_t id, void *result, kafka_common_Error_t *error, kafka_common_Error_t **out_err);

kafka_x_Thing_t *kafka_x_Thing_new_err(kafka_common_Error_t **out_err);

void kafka_x_Thing_destroy_err(kafka_x_Thing_t *self_, kafka_common_Error_t **out_err);

kafka_x_Callback_t *kafka_x_Callback_new_err(void *self_, kafka_x_Callback_on_completion_fn_t on_completion, kafka_common_Error_t **out_err);

/**
 * Sends.
 *
 * # Safety
 *
 * \`callback\` a live handle (consumed).
 */
void kafka_x_P_send_cb(const kafka_x_P_t *self_, kafka_x_Callback_t *callback, kafka_x_P_send_cb_t cb, void *opaque);

void kafka_common_Error_destroy_err(kafka_common_Error_t *self_, kafka_common_Error_t **out_err);
`;

const UTIL_H = `#include <pthread.h>
#define FFI_ERR(call) ((void)(call), ffi_take(&ffi_err))
#define FFI_SET(var, call) ((var) = (call), ffi_take(&ffi_err))
#define FFI_DISCARD(call) error_discard(FFI_ERR(call))
typedef struct {
    pthread_mutex_t notify_mtx;
    int notify_open;
    void* tsfn;
    void* client;
} CallbackQueue;
typedef int32_t (*cbq_execute_fn)(void* client, kafka_common_Error_t** out_err);
typedef void (*cbq_client_fn)(void* client);
`;

const QUEUE_C = `#include "util.h"

void error_discard(kafka_common_Error_t* e) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_common_Error_destroy_err(e, &ffi_err);
    kafka_common_Error_t* panic = ffi_take(&ffi_err);
    (void)panic;
}

void cbq_notify(void* opaque) {
    CallbackQueue* q = (CallbackQueue*)opaque;
    pthread_mutex_lock(&q->notify_mtx);
    if (q->notify_open) napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);
    pthread_mutex_unlock(&q->notify_mtx);
}

void cbq_drain(CallbackQueue* q) {
    kafka_common_Error_t* ffi_err = NULL;
    while (1) {
        int32_t ran = q->execute(q->client, &ffi_err);
        error_discard(ffi_take(&ffi_err));
        if (ran <= 0) break;
    }
    if (q->after_drain != NULL) q->after_drain(q->client);
}

static void call_js(void* env, void* context) {
    cbq_drain((CallbackQueue*)context);
}

int cbq_init(CallbackQueue* q, void* client, cbq_execute_fn execute, cbq_client_fn after_drain) {
    q->client = client;
    q->execute = execute;
    q->after_drain = after_drain;
    return 0;
}
`;

const PRODUCER_C = `#include "util.h"

static int32_t producer_execute(void* client, kafka_common_Error_t** out_err) {
    return kafka_x_P__execute_callbacks_err(client, out_err);
}

static void send_on_completion(void* self_, const kafka_x_M_t* metadata, const kafka_common_Error_t* error, int64_t id) {
    kafka_common_Error_t* ffi_err = NULL;
    call_ref(self_, metadata, error);
    if (!p->destroying) {
        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));
    }
}

static void on_handover(kafka_x_Thing_t* value, kafka_common_Error_t* error, void* opaque) {
    kafka_common_Error_t* ffi_err = NULL;
    FFI_DISCARD(kafka_x_Thing_destroy_err(value, &ffi_err));
    error_discard(error);
}

void js_send(void* p) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_x_Callback_t* cb = NULL;
    void* ctx = calloc(1, 8);
    error_discard(FFI_SET(cb, kafka_x_Callback_new_err(ctx, send_on_completion, &ffi_err)));
    kafka_x_P_send_cb(p, cb, on_handover, ctx);
    kafka_x_P_flush_cb(p, on_handover, ctx);
}

void js_pump(CallbackQueue* q) {
    cbq_drain(q);
}

void js_setup(void* p) {
    kafka_common_Error_t* ffi_err = NULL;
    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, cbq_notify, p, &ffi_err));
}

static void producer_after_drain(void* client) {
    (void)client;
}

void js_create(CallbackQueue* q, void* p) {
    cbq_init(q, p, producer_execute, producer_after_drain);
}
`;

const RECORD_C = `#include "util.h"

int js_bytes(void* env, void* value, JsBytes* out) {
    void* data = NULL;
    size_t length = 0;
    napi_get_typedarray_info(env, value, NULL, &length, &data, NULL, NULL);
    out->bytes.data = data;
    out->bytes.len = (int32_t)length;
    return 0;
}
`;

const NATIVE_TS = `interface Native {
  send(onComplete: (m: unknown, e: unknown) => void): void;
}
declare const loaded: Native;
const native: Native = loaded;
export = native;
`;

const PRODUCER_TS = `import native = require('../_native');
import type { Callback } from './callback';
class Future {
  private _complete(v: unknown): void {
    void v;
  }
}
export class Producer {
  send(callback: Callback): void {
    const future = new Future();
    const onComplete = (m: unknown, e: unknown): void => {
      callback(m, e);
      future['_complete'](m);
    };
    native.send(onComplete);
  }
}
`;

const FILES = {
  'confluent_kafka_bindings.h': HEADER,
  'src/util.h': UTIL_H,
  'src/callback_queue.c': QUEUE_C,
  'src/producer.c': PRODUCER_C,
  'src/record.c': RECORD_C,
  'lib/_native.ts': NATIVE_TS,
  'lib/producer/callback.ts': 'export type Callback = (m: unknown, e: unknown) => void;\n',
  'lib/producer/producer.ts': PRODUCER_TS,
};

/** The rule with the fixture's allowlists in place of the real tree's. */
function withLists(rule, lists) {
  return { ...rule, ...lists };
}

/** The allowlists that fit the fixture. */
const LISTS = {
  'check-out-err': { ALLOWED: [] },
  'check-cb-twin-only': { ALLOWED: [] },
  'check-single-drainer': {
    EXECUTORS: [{ key: 'producer_execute', reason: 'the fixture executor' }],
    DRAIN_CALLERS: [
      { key: 'call_js', reason: 'the tsfn call_js' },
      { key: 'js_pump', reason: 'the pump' },
    ],
  },
  'check-completion-order': { COMPLETIONS: [{ key: 'send_on_completion', reason: 'the record callback' }] },
  'check-zero-copy': { ALLOWED: [{ key: 'src/producer.c:js_send:calloc#1', reason: "the send's context" }] },
  'check-no-binding-queue': { ALLOWED: [{ key: 'src/util.h:CallbackQueue.notify_mtx', reason: "the queue's lock" }] },
};

module.exports = { FILES, HEADER, UTIL_H, QUEUE_C, PRODUCER_C, RECORD_C, PRODUCER_TS, LISTS, withLists };

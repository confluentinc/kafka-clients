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

// A small client for the native rules' fixture projects: a header with a
// waiting call and its `_cb` twin, an addon registering its functions, and
// a src/ calling them through the handle src/internal/native.ts exports.

const HEADER = `void kafka_x_Producer_flush(const kafka_x_Producer_t *self_, kafka_common_Error_t **out_err);

void kafka_x_Producer_flush_cb(const kafka_x_Producer_t *self_, kafka_x_Producer_flush_cb_t cb, void *opaque);

int32_t kafka_x_Producer__execute_callbacks_err(const kafka_x_Producer_t *self_, kafka_common_Error_t **out_err);
`;

const ADDON = `#include "util.h"

static napi_value js_probe(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_header_stats(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_producer_stats(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_error_stats(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_record_spy(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_metrics_stats(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_fault_next(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_consumer_stats(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_consumer_refuse_external_buffers(napi_env env, napi_callback_info info) {
    return NULL;
}

static napi_value js_config_stats(napi_env env, napi_callback_info info) {
    return NULL;
}

napi_value addon_init(napi_env env, napi_value exports) {
    napi_property_descriptor props[] = {
        {"probe", NULL, js_probe, NULL, NULL, NULL, napi_default, NULL},
        {"headerStats", NULL, js_header_stats, NULL, NULL, NULL, napi_default, NULL},
        {"producerStats", NULL, js_producer_stats, NULL, NULL, NULL, napi_default, NULL},
        {"errorStats", NULL, js_error_stats, NULL, NULL, NULL, napi_default, NULL},
        {"recordSpy", NULL, js_record_spy, NULL, NULL, NULL, napi_default, NULL},
        {"metricsStats", NULL, js_metrics_stats, NULL, NULL, NULL, napi_default, NULL},
        {"faultNext", NULL, js_fault_next, NULL, NULL, NULL, napi_default, NULL},
        {"consumerStats", NULL, js_consumer_stats, NULL, NULL, NULL, napi_default, NULL},
        {"consumerRefuseExternalBuffers", NULL, js_consumer_refuse_external_buffers, NULL, NULL, NULL, napi_default, NULL},
        {"configStats", NULL, js_config_stats, NULL, NULL, NULL, napi_default, NULL},
        {"flush", NULL, js_flush, NULL, NULL, NULL, napi_default, NULL},
        {"pump", NULL, js_pump, NULL, NULL, NULL, napi_default, NULL},
        {"setFactories", NULL, js_setup, NULL, NULL, NULL, napi_default, NULL},
    };
    return exports;
}
`;

const PRODUCER_C = `#include "util.h"

static void start_flush(Producer* p) {
    kafka_x_Producer_flush_cb(p->view, on_flush, p);
}

napi_value js_flush(napi_env env, napi_callback_info info) {
    start_flush(NULL);
    return NULL;
}

napi_value js_pump(napi_env env, napi_callback_info info) {
    FFI_ERR(kafka_x_Producer__execute_callbacks_err(NULL, &ffi_err));
    return NULL;
}

napi_value js_setup(napi_env env, napi_callback_info info) {
    return NULL;
}
`;

const NATIVE_TS = `interface Native {
  probe(): boolean;
  headerStats(): object;
  producerStats(): object;
  errorStats(): object;
  recordSpy(arm: boolean): object[];
  metricsStats(): object;
  faultNext(step: string | null): string | null;
  consumerStats(): object;
  consumerRefuseExternalBuffers(on: boolean): void;
  configStats(): object;
  flush(done: () => void): void;
  pump(): void;
  setFactories(): void;
}
declare const loaded: Native;
const native: Native = loaded;
native.setFactories();
export = native;
`;

const PRODUCER_TS = `import native = require('../internal/native');
export class Producer {
  flush(): Promise<void> {
    return new Promise((resolve) => this.#start(resolve));
  }
  pump(): void {
    native.pump();
  }
  #start(done: () => void): void {
    native.flush(done);
  }
}
`;

/** The fixture's files: header, addon, src/, all passing the native rules. */
const FILES = {
  'confluent_kafka_bindings.h': HEADER,
  'native/addon.c': ADDON,
  'native/producer.c': PRODUCER_C,
  'src/internal/native.ts': NATIVE_TS,
  'src/producer/producer.ts': PRODUCER_TS,
  'test/unit/internal/native.test.js': "const native = require('../../../dist/internal/native');\nnative.probe();\nnative.configStats();\n",
  'test/memory/native_counts.test.js':
    "const native = require('../../dist/internal/native');\nnative.producerStats();\nnative.errorStats();\nnative.headerStats();\n",
  'test/memory/record_read_back.test.js': "const native = require('../../dist/internal/native');\nnative.recordSpy(false);\n",
  'test/memory/metrics.test.js': "const native = require('../../dist/internal/native');\nnative.metricsStats();\n",
  'test/unit/producer/kafka_producer.test.js': "const native = require('../../../dist/internal/native');\nnative.faultNext(null);\n",
  'test/memory/consumer_batch.test.js':
    "const native = require('../../dist/internal/native');\nnative.consumerStats();\nnative.consumerRefuseExternalBuffers(false);\n",
};

module.exports = { FILES, ADDON, PRODUCER_C, NATIVE_TS, PRODUCER_TS };

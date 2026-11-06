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

// N4 check-single-drainer (scripts/lint/rules/single_drainer.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { QUEUE_C, PRODUCER_C, LISTS } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-single-drainer';

test('one drainer: the executor, cbq_drain from call_js and the pump, no destroy in the loop', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  // The executor's call, the two drain calls, the 3 calls in the loop, the 2 completions, the executor
  // cbq_init is given, and error_discard, which the loop calls.
  assert.ok(r.out.includes('✅ check-single-drainer: 10 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = runN(RULE, {
    'src/producer.c': PRODUCER_C.replace(
      'void js_pump(CallbackQueue* q) {\n    cbq_drain(q);\n}',
      'void js_pump(CallbackQueue* q) {\n    cbq_drain(q);\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_P__execute_callbacks_err(q, &ffi_err));\n    producer_execute(q, &ffi_err);\n}\n\nvoid js_other(CallbackQueue* q) {\n    cbq_drain(q);\n}',
    ),
    'src/callback_queue.c': QUEUE_C.replace(
      '        if (ran <= 0) break;',
      '        if (ran <= 0) break;\n        FFI_DISCARD(kafka_x_Thing_destroy_err(q->client, &ffi_err));',
    ),
  });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/producer.c:34: js_pump calls the executor producer_execute directly',
    "src/callback_queue.c:23: cbq_drain's loop calls kafka_x_Thing_destroy_err: destroy after the loop",
    "src/producer.c:33: js_pump calls kafka_x_P__execute_callbacks_err, which only the queue's executor may",
    'src/producer.c:38: js_other calls cbq_drain, which only call_js and the pump may',
  ]);
});

test('a client destroy reached from a completion, and a drain whose loop is a for, are seen', () => {
  const { HEADER } = require('./_c_fixture');
  const header = `${HEADER}\nvoid kafka_x_K_destroy_err(kafka_x_K_t *self_, kafka_common_Error_t **out_err);\n\nconst kafka_x_P_t *kafka_x_K__as_P_err(kafka_x_K_t *self_, kafka_common_Error_t **out_err);\n`;
  let r = runN(RULE, {
    'confluent_kafka_bindings.h': header,
    'src/producer.c': PRODUCER_C.replace(
      'static void on_handover(',
      'static void destroy_client(void* k) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_K_destroy_err(k, &ffi_err));\n}\n\nstatic void on_handover(',
    ).replace('    error_discard(error);\n}', '    error_discard(error);\n    destroy_client(opaque);\n}'),
  });
  assert.deepEqual(findings(r), [
    'on_handover: runs inside the drain (a completion given to kafka_x_P_flush_cb) and reaches kafka_x_K_destroy_err through on_handover -> destroy_client -> kafka_x_K_destroy_err (src/producer.c:17): mark the client and destroy after the drain',
  ]);
  r = runN(RULE, {
    'src/callback_queue.c': QUEUE_C.replace('    while (1) {', '    for (;;) {').replace(
      '        if (ran <= 0) break;',
      '        if (ran <= 0) break;\n        FFI_DISCARD(kafka_x_Thing_destroy_err(q->client, &ffi_err));',
    ),
  });
  assert.deepEqual(findings(r), ["src/callback_queue.c:23: cbq_drain's loop calls kafka_x_Thing_destroy_err: destroy after the loop"]);
  r = runN(RULE, {
    'src/callback_queue.c': QUEUE_C.replace(/    while \(1\) \{[\s\S]*?\n    \}\n/, '    int32_t ran = q->execute(q->client, &ffi_err);\n    error_discard(ffi_take(&ffi_err));\n    (void)ran;\n'),
  });
  assert.deepEqual(findings(r), ['cbq_drain: has no loop (while, for, do) the rule can read']);
});

test('the executor cbq_init is given, and what the loop calls, run inside the drain', () => {
  const { HEADER } = require('./_c_fixture');
  const header = `${HEADER}\nvoid kafka_x_K_destroy_err(kafka_x_K_t *self_, kafka_common_Error_t **out_err);\n\nconst kafka_x_P_t *kafka_x_K__as_P_err(kafka_x_K_t *self_, kafka_common_Error_t **out_err);\n`;
  const destroy = 'static void producer_destroy(void* k) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_K_destroy_err(k, &ffi_err));\n}\n\n';
  // The executor destroys the client: the loop's next q->execute uses a freed view.
  let r = runN(RULE, {
    'confluent_kafka_bindings.h': header,
    'src/producer.c': PRODUCER_C.replace('static int32_t producer_execute(', `${destroy}static int32_t producer_execute(`).replace(
      '    return kafka_x_P__execute_callbacks_err(client, out_err);',
      '    int32_t ran = kafka_x_P__execute_callbacks_err(client, out_err);\n    producer_destroy(client);\n    return ran;',
    ),
  });
  assert.deepEqual(findings(r), [
    'producer_execute: runs inside the drain (given to cbq_init as the executor) and reaches kafka_x_K_destroy_err through producer_execute -> producer_destroy -> kafka_x_K_destroy_err (src/producer.c:5): mark the client and destroy after the drain',
  ]);
  // The after-drain hook called inside the loop, and a src destroy the loop calls by name.
  r = runN(RULE, {
    'confluent_kafka_bindings.h': header,
    'src/callback_queue.c': QUEUE_C.replace('        if (ran <= 0) break;\n', '        if (ran <= 0) break;\n        if (q->after_drain != NULL) q->after_drain(q->client);\n        client_destroy(q->client);\n').replace(
      'void cbq_drain(',
      `${destroy.replace('static void producer_destroy', 'static void client_destroy')}void cbq_drain(`,
    ),
  });
  assert.deepEqual(findings(r), [
    "src/callback_queue.c:28: cbq_drain calls the queue's after_drain inside its loop: call it after the loop",
    "client_destroy: runs inside the drain (called by cbq_drain's loop) and reaches kafka_x_K_destroy_err through client_destroy -> kafka_x_K_destroy_err (src/callback_queue.c:19): mark the client and destroy after the drain",
  ]);
  // No executor the rule can follow.
  r = runN(RULE, { 'src/producer.c': PRODUCER_C.replace('    cbq_init(q, p, producer_execute, producer_after_drain);\n', '') });
  assert.deepEqual(findings(r), [
    "cbq_init: is passed no src function as its cbq_execute_fn: the rule cannot follow the executor cbq_drain's loop calls",
  ]);
});

test('an allowlist entry that matches nothing is a finding', () => {
  const lists = { ...LISTS[RULE], DRAIN_CALLERS: [...LISTS[RULE].DRAIN_CALLERS, { key: 'js_gone', reason: 'the test' }] };
  const r = runN(RULE, {}, lists);
  assert.deepEqual(findings(r), ['js_gone: is allowed as the test, but nothing matches it any more: delete the entry']);
});

test('without cbq_drain it is a finding', () => {
  const r = runN(RULE, { 'src/callback_queue.c': QUEUE_C.replace('void cbq_drain(CallbackQueue* q) {', 'void cbq_flush(CallbackQueue* q) {') });
  assert.deepEqual(findings(r), ['cbq_drain: is defined in no src/*.c']);
});

test('cannot run without the header', () => {
  const r = runN(RULE, { 'confluent_kafka_bindings.h': undefined });
  assert.match(r.err[1], /^❌ check-single-drainer: cannot run, no bindings header at /);
});

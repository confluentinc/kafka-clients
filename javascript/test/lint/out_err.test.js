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

// N1 check-out-err (scripts/lint/rules/out_err.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { PRODUCER_C, QUEUE_C } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-out-err';

test('slots read by FFI_ERR / FFI_SET / FFI_DISCARD, by ffi_take next, or forwarded pass', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  // Five header calls, the executor's forwarded slot, and the drain's call through q->execute.
  assert.ok(r.out.includes('✅ check-out-err: 7 item(s) checked'), r.text);
});

test('a NULL slot and an unread one are findings, a cast inside the macro is not', () => {
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, cbq_notify, p, &ffi_err));',
      '    kafka_x_P__set_callbacks_notify_err(p, cbq_notify, p, NULL);\n    kafka_x_P__set_callbacks_notify_err(p, cbq_notify, p, &ffi_err);\n' +
        '    int n = 0;\n    error_discard(FFI_SET(n, (int)kafka_x_P__execute_callbacks_err(p, &ffi_err)));',
    ),
    'native/callback_queue.c': QUEUE_C.replace('    kafka_common_Error_t* panic = ffi_take(&ffi_err);\n    (void)panic;\n', ''),
  });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'native/callback_queue.c:error_discard:kafka_common_Error_destroy_err#1: its out_err &ffi_err (line 5) is read by no FFI_ERR / FFI_SET / FFI_DISCARD, nor ffi_take in the next statement',
    'native/producer.c:js_setup:kafka_x_P__set_callbacks_notify_err#1: gets NULL as its out_err (line 36)',
    'native/producer.c:js_setup:kafka_x_P__set_callbacks_notify_err#2: its out_err &ffi_err (line 37) is read by no FFI_ERR / FFI_SET / FFI_DISCARD, nor ffi_take in the next statement',
  ]);
});

test("a forwarded slot's callers are checked, through a function pointer too", () => {
  // The drain no longer reads what the executor stored, then passes NULL.
  let r = runN(RULE, { 'native/callback_queue.c': QUEUE_C.replace('        error_discard(ffi_take(&ffi_err));\n', '') });
  assert.deepEqual(findings(r), [
    'native/callback_queue.c:cbq_drain:execute#1: its out_err &ffi_err (line 20) is read by no FFI_ERR / FFI_SET / FFI_DISCARD, nor ffi_take in the next statement',
  ]);
  r = runN(RULE, { 'native/callback_queue.c': QUEUE_C.replace('q->execute(q->client, &ffi_err)', 'q->execute(q->client, NULL)') });
  assert.deepEqual(findings(r), ['native/callback_queue.c:cbq_drain:execute#1: gets NULL as its out_err (line 20)']);
  // A direct call of the forwarder.
  r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('void js_pump(CallbackQueue* q) {\n', 'void js_pump(CallbackQueue* q) {\n    kafka_common_Error_t* e = NULL;\n    producer_execute(q, &e);\n') });
  assert.deepEqual(findings(r), [
    'native/producer.c:js_pump:producer_execute#1: its out_err &e (line 32) is read by no FFI_ERR / FFI_SET / FFI_DISCARD, nor ffi_take in the next statement',
  ]);
});

test('inside FFI_ERR / FFI_SET / FFI_DISCARD the slot is &ffi_err, the one they read', () => {
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, cbq_notify, p, &ffi_err));',
      '    kafka_common_Error_t* close_err = NULL;\n    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, cbq_notify, p, &close_err));',
    ),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:js_setup:kafka_x_P__set_callbacks_notify_err#1: its out_err &close_err (line 37) is wrapped in FFI_DISCARD, which reads &ffi_err alone',
  ]);
});

test('an allowlist entry waives exactly its call site; one that waives nothing is a finding', () => {
  const unread = { 'native/callback_queue.c': QUEUE_C.replace('    kafka_common_Error_t* panic = ffi_take(&ffi_err);\n    (void)panic;\n', '') };
  let r = runN(RULE, unread, { ALLOWED: [{ key: 'native/callback_queue.c:error_discard:kafka_common_Error_destroy_err#1', reason: 'the test' }] });
  assert.equal(r.status, 0, r.text);
  r = runN(RULE, {}, { ALLOWED: [{ key: 'native/callback_queue.c:error_discard:kafka_common_Error_destroy_err#2', reason: 'the test' }] });
  assert.deepEqual(findings(r), ['native/callback_queue.c:error_discard:kafka_common_Error_destroy_err#2: is allowed as the test, but nothing matches it any more: delete the entry']);
  assert.deepEqual(require('../../scripts/lint/rules/out_err').ALLOWED.map((e) => e.key), ['native/error.c:error_discard:kafka_common_Error_destroy_err#2']);
});

test('cannot run without the header or the C sources', () => {
  let r = runN(RULE, { 'confluent_kafka_bindings.h': undefined });
  assert.match(r.err[1], /^❌ check-out-err: cannot run, no bindings header at /);
  r = runN(RULE, { 'native/producer.c': undefined, 'native/callback_queue.c': undefined, 'native/record.c': undefined });
  assert.equal(r.err[1], '❌ check-out-err: cannot run, no C sources under native/');
});

test('no call with an out_err fails: 0 items checked', () => {
  const r = runN(RULE, { 'native/producer.c': 'void f(void) {}\n', 'native/callback_queue.c': 'void g(void) {}\n', 'native/record.c': 'void h(void) {}\n' });
  assert.ok(r.err.includes('❌ check-out-err: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

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

// N6 check-ownership (scripts/lint/rules/ownership.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { PRODUCER_C, HEADER } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-ownership';

test('owned handles destroyed or consumed, borrowed ones left alone', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  // Owned types: Callback_t, Error_t, Thing_t; borrowed parameters of send_on_completion: metadata, error.
  assert.ok(r.out.includes('✅ check-ownership: 5 item(s) checked'), r.text);
});

test('an owned type never destroyed nor consumed is a finding', () => {
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace('    FFI_DISCARD(kafka_x_Thing_destroy_err(value, &ffi_err));\n', ''),
    // Without its "(consumed)", the send no longer takes the callback over.
    'confluent_kafka_bindings.h': HEADER.replace(' (consumed)', ''),
  });
  assert.deepEqual(findings(r), [
    'kafka_x_Callback_t: native/ receives it owned (from kafka_x_Callback_new_err, native/producer.c:24) but never destroys it (kafka_x_Callback_destroy / kafka_x_Callback_destroy_err) nor hands it to a call that consumes it',
    "kafka_x_Thing_t: native/ receives it owned (as on_handover's value, a completion of kafka_x_P_send_cb) but never destroys it (kafka_x_Thing_destroy / kafka_x_Thing_destroy_err) nor hands it to a call that consumes it",
  ]);
});

test('a borrowed parameter passed to a destroy, directly or through a native helper, is a finding', () => {
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    call_ref(self_, metadata, error);',
      '    call_ref(self_, metadata, error);\n    error_discard(error);\n    FFI_DISCARD(kafka_x_Thing_destroy_err(metadata, &ffi_err));',
    ),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:11: send_on_completion passes its borrowed metadata (const) to kafka_x_Thing_destroy_err, which destroys it',
    'native/producer.c:10: send_on_completion passes its borrowed error (const) to error_discard, which destroys it',
  ]);
});

test('a cast does not hide a borrowed parameter passed to a destroy', () => {
  // Under -Werror a const handle reaches a destroy only through a cast, in the
  // destroyer's own body too (a helper that destroys its parameter through one).
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    call_ref(self_, metadata, error);',
      '    call_ref(self_, metadata, error);\n    error_drop(error);\n    FFI_DISCARD(kafka_x_Thing_destroy_err((kafka_x_Thing_t*)(metadata), &ffi_err));',
    ).replace(
      'static void send_on_completion(',
      'static void error_drop(const kafka_common_Error_t* e) {\n    error_discard((kafka_common_Error_t *) e);\n}\n\nstatic void send_on_completion(',
    ),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:15: send_on_completion passes its borrowed metadata (const) to kafka_x_Thing_destroy_err, which destroys it',
    'native/producer.c:14: send_on_completion passes its borrowed error (const) to error_drop, which destroys it',
  ]);
});

test('cannot run without the header', () => {
  const r = runN(RULE, { 'confluent_kafka_bindings.h': undefined });
  assert.match(r.err[1], /^❌ check-ownership: cannot run, no bindings header at /);
});

test('nothing owned and no completion fails: 0 items checked', () => {
  const r = runN(RULE, { 'native/producer.c': 'void f(void) {}\n', 'native/callback_queue.c': 'void g(void) {}\n', 'native/record.c': 'void h(void) {}\n' });
  assert.ok(r.err.includes('❌ check-ownership: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

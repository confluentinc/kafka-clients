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

// N2 check-cb-twin-only (scripts/lint/rules/cb_twin_only.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { PRODUCER_C } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-cb-twin-only';
const BLOCKING = PRODUCER_C.replace(
  '    kafka_x_P_flush_cb(p, on_handover, ctx);',
  '    kafka_x_P_flush_cb(p, on_handover, ctx);\n    FFI_DISCARD(kafka_x_P_flush(p, &ffi_err));',
);

test('only _cb twins are called', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  // Every header call of native/.
  assert.ok(r.out.includes('✅ check-cb-twin-only: 8 item(s) checked'), r.text);
});

test('the blocking form of a _cb twin is a finding', () => {
  const r = runN(RULE, { 'native/producer.c': BLOCKING });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), ['native/producer.c:36: js_send calls kafka_x_P_flush, the blocking form of kafka_x_P_flush_cb: call the _cb twin']);
});

test('reading the future is a finding in every form, its _cb twins included', () => {
  const { HEADER } = require('./_c_fixture');
  const header = `${HEADER}\nvoid *kafka_common_KafkaFuture_get(const kafka_common_KafkaFuture_t *self_, kafka_common_Error_t **out_err);\n\nvoid kafka_common_KafkaFuture_get_cb(const kafka_common_KafkaFuture_t *self_, kafka_common_KafkaFuture_get_cb_t cb, void *opaque);\n\nvoid kafka_common_KafkaFuture_get_with_timeout_cb(const kafka_common_KafkaFuture_t *self_, int64_t timeout_ms, kafka_common_KafkaFuture_get_with_timeout_cb_t cb, void *opaque);\n`;
  const r = runN(RULE, {
    'confluent_kafka_bindings.h': header,
    'native/producer.c': PRODUCER_C.replace(
      '    FFI_DISCARD(kafka_x_Thing_destroy_err(value, &ffi_err));',
      '    kafka_common_KafkaFuture_get_cb(value, on_handover, opaque);\n    kafka_common_KafkaFuture_get_with_timeout_cb(value, 0, on_handover, opaque);\n    FFI_DISCARD(kafka_x_Thing_destroy_err(value, &ffi_err));',
    ),
  });
  const reason = "never read the future (javascript/CLAUDE.md rule 5): a send's future is destroyed unread, the delivery result comes from the record's Callback_t";
  assert.deepEqual(findings(r), [
    `native/producer.c:22: on_handover calls kafka_common_KafkaFuture_get_cb: ${reason}`,
    `native/producer.c:23: on_handover calls kafka_common_KafkaFuture_get_with_timeout_cb: ${reason}`,
  ]);
});

test("a macro's body is checked at its definition", () => {
  const { UTIL_H } = require('./_c_fixture');
  const r = runN(RULE, {
    'native/util.h': `${UTIL_H}#define FLUSH_VIEW(view) \\\n    FFI_DISCARD(kafka_x_P_flush((view), &ffi_err))\n`,
    'native/producer.c': PRODUCER_C.replace('    kafka_x_P_flush_cb(p, on_handover, ctx);', '    FLUSH_VIEW(p);'),
  });
  assert.deepEqual(findings(r), ['native/util.h:14: the macro FLUSH_VIEW calls kafka_x_P_flush, the blocking form of kafka_x_P_flush_cb: call the _cb twin']);
});

test('an allowlist entry allows exactly its call; one that allows nothing is a finding', () => {
  let r = runN(RULE, { 'native/producer.c': BLOCKING }, { ALLOWED: [{ key: 'native/producer.c:js_send:kafka_x_P_flush(p, &ffi_err)', reason: 'the test' }] });
  assert.equal(r.status, 0, r.text);
  r = runN(RULE, {}, { ALLOWED: [{ key: 'native/producer.c:js_send:kafka_x_P_flush(p, 0, &ffi_err)', reason: 'the test' }] });
  assert.deepEqual(findings(r), ['native/producer.c:js_send:kafka_x_P_flush(p, 0, &ffi_err): is allowed as the test, but nothing matches it any more: delete the entry']);
});

test("the real tree's entry is the finalizer's force-close", () => {
  const { ALLOWED } = require('../../scripts/lint/rules/cb_twin_only');
  assert.deepEqual(ALLOWED.map((e) => e.key), ['native/producer.c:producer_force_close:kafka_producer_Producer_close_with_timeout(p->view, 0, &ffi_err)']);
});

test('cannot run without the header', () => {
  const r = runN(RULE, { 'confluent_kafka_bindings.h': undefined });
  assert.match(r.err[1], /^❌ check-cb-twin-only: cannot run, no bindings header at /);
});

test('no header call fails: 0 items checked', () => {
  const r = runN(RULE, { 'native/producer.c': 'void f(void) {}\n', 'native/callback_queue.c': 'void g(void) {}\n', 'native/record.c': 'void h(void) {}\n' });
  assert.ok(r.err.includes('❌ check-cb-twin-only: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

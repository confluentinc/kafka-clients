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

// N3 check-notify-hook (scripts/lint/rules/notify_hook.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { QUEUE_C, PRODUCER_C } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-notify-hook';
const HOOK = '    if (q->notify_open) napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);';

test('a hook that only signals, and one that does nothing, pass', () => {
  const r = runN(RULE, {
    'src/producer.c': PRODUCER_C.replace(
      'void js_setup(void* p) {',
      'static void noop_notify(void* opaque) {\n    (void)opaque;\n}\n\nvoid js_teardown(void* p) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, noop_notify, NULL, &ffi_err));\n}\n\nvoid js_setup(void* p) {',
    ),
  });
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-notify-hook: 2 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = runN(RULE, {
    'src/callback_queue.c': QUEUE_C.replace(HOOK, '    cbq_drain(q);\n    napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_blocking);').replace(
      '    pthread_mutex_lock(&q->notify_mtx);\n',
      '',
    ),
  });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/callback_queue.c:12: cbq_notify calls cbq_drain: the hook only signals',
    'src/callback_queue.c:13: cbq_notify calls the threadsafe function napi_tsfn_blocking, not napi_tsfn_nonblocking',
    'cbq_notify: calls the threadsafe function other than once, under if (…notify_open)',
    'cbq_notify: calls the threadsafe function outside a notify_mtx lock / unlock',
  ]);
});

test('a negated guard is unguarded', () => {
  const r = runN(RULE, { 'src/callback_queue.c': QUEUE_C.replace('if (q->notify_open)', 'if (!q->notify_open)') });
  assert.deepEqual(findings(r), ['cbq_notify: calls the threadsafe function other than once, under if (…notify_open)']);
});

test('a hook src/ does not define is a finding', () => {
  const r = runN(RULE, { 'src/callback_queue.c': QUEUE_C.replace('void cbq_notify(void* opaque) {', 'void cbq_notified(void* opaque) {') });
  assert.deepEqual(findings(r), ['cbq_notify: is installed as a notify hook but defined in no src/*.c']);
});

test('no hook installed fails: 0 items checked', () => {
  const r = runN(RULE, { 'src/producer.c': 'void f(void) {}\n' });
  assert.ok(r.err.includes('❌ check-notify-hook: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'src/producer.c': undefined, 'src/callback_queue.c': undefined, 'src/record.c': undefined });
  assert.equal(r.err[1], '❌ check-notify-hook: cannot run, no C sources under src/');
});

test("two files' static hooks of one name are each checked, as their own file's", () => {
  const noop = 'static void noop_notify(void* opaque) {\n    (void)opaque;\n}\n\n';
  const install = (name) =>
    `void ${name}(void* p) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, noop_notify, NULL, &ffi_err));\n}\n`;
  const r = runN(RULE, {
    'src/producer.c': PRODUCER_C.replace('void js_setup(void* p) {', `${noop}${install('js_teardown')}\nvoid js_setup(void* p) {`),
    // A second client file with its own static noop_notify, which blocks; read before producer.c's.
    'src/consumer.c': `#include "util.h"\n\nstatic void noop_notify(void* opaque) {\n    CallbackQueue* q = (CallbackQueue*)opaque;\n    napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_blocking);\n}\n\n${install('js_consumer_teardown')}`,
  });
  assert.deepEqual(findings(r), [
    'src/consumer.c:5: noop_notify calls the threadsafe function napi_tsfn_blocking, not napi_tsfn_nonblocking',
    'noop_notify: calls the threadsafe function other than once, under if (…notify_open)',
    'noop_notify: calls the threadsafe function outside a notify_mtx lock / unlock',
  ]);
});

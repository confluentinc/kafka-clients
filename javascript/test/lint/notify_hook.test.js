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
    'native/producer.c': PRODUCER_C.replace(
      'void js_setup(void* p) {',
      'static void noop_notify(void* opaque) {\n    (void)opaque;\n}\n\nvoid js_teardown(void* p) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, noop_notify, NULL, &ffi_err));\n}\n\nvoid js_setup(void* p) {',
    ),
  });
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-notify-hook: 2 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = runN(RULE, {
    'native/callback_queue.c': QUEUE_C.replace(HOOK, '    cbq_drain(q);\n    napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_blocking);').replace(
      '    pthread_mutex_lock(&q->notify_mtx);\n',
      '',
    ),
  });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'native/callback_queue.c:12: cbq_notify calls cbq_drain: the hook only signals',
    'native/callback_queue.c:13: cbq_notify calls the threadsafe function napi_tsfn_blocking, not napi_tsfn_nonblocking',
    'cbq_notify: calls the threadsafe function other than once, under if (…notify_open)',
    'cbq_notify: calls the threadsafe function outside a notify_mtx lock / unlock',
  ]);
});

test('a negated guard is unguarded', () => {
  const r = runN(RULE, { 'native/callback_queue.c': QUEUE_C.replace('if (q->notify_open)', 'if (!q->notify_open)') });
  assert.deepEqual(findings(r), ['cbq_notify: calls the threadsafe function other than once, under if (…notify_open)']);
});

test('a hook native/ does not define is a finding', () => {
  const r = runN(RULE, { 'native/callback_queue.c': QUEUE_C.replace('void cbq_notify(void* opaque) {', 'void cbq_notified(void* opaque) {') });
  assert.deepEqual(findings(r), ['cbq_notify: is installed as a notify hook but defined in no native/*.c']);
});

test('no hook installed fails: 0 items checked', () => {
  const r = runN(RULE, { 'native/producer.c': 'void f(void) {}\n' });
  assert.ok(r.err.includes('❌ check-notify-hook: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'native/producer.c': undefined, 'native/callback_queue.c': undefined, 'native/record.c': undefined });
  assert.equal(r.err[1], '❌ check-notify-hook: cannot run, no C sources under native/');
});

test("two files' static hooks of one name are each checked, as their own file's", () => {
  const noop = 'static void noop_notify(void* opaque) {\n    (void)opaque;\n}\n\n';
  const install = (name) =>
    `void ${name}(void* p) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, noop_notify, NULL, &ffi_err));\n}\n`;
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace('void js_setup(void* p) {', `${noop}${install('js_teardown')}\nvoid js_setup(void* p) {`),
    // A second client file with its own static noop_notify, which blocks; read before producer.c's.
    'native/consumer.c': `#include "util.h"\n\nstatic void noop_notify(void* opaque) {\n    CallbackQueue* q = (CallbackQueue*)opaque;\n    napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_blocking);\n}\n\n${install('js_consumer_teardown')}`,
  });
  assert.deepEqual(findings(r), [
    'native/consumer.c:5: noop_notify calls the threadsafe function napi_tsfn_blocking, not napi_tsfn_nonblocking',
    'noop_notify: calls the threadsafe function other than once, under if (…notify_open)',
    'noop_notify: calls the threadsafe function outside a notify_mtx lock / unlock',
  ]);
});

test('any statement besides the lock, the guarded call and the unlock is a finding', () => {
  const lock = '    pthread_mutex_lock(&q->notify_mtx);\n';
  const unlock = '    pthread_mutex_unlock(&q->notify_mtx);\n';
  const cases = [
    // A write to the queue before the lock, and one after the call: races with the JS thread.
    [QUEUE_C.replace(lock, `    q->pending += 1;\n${lock}`), 'native/callback_queue.c:12: cbq_notify runs `q->pending += 1;`'],
    [QUEUE_C.replace(`${HOOK}\n`, `${HOOK}\n    q->notify_open = 0;\n`), 'native/callback_queue.c:14: cbq_notify runs `q->notify_open = 0;`'],
    // A return between the lock and the unlock leaves notify_mtx held.
    [QUEUE_C.replace(lock, `${lock}    if (q->stopped) return;\n`), 'native/callback_queue.c:13: cbq_notify runs `if (q->stopped) return;`'],
    // A statement inside the guard's block.
    [
      QUEUE_C.replace(HOOK, '    if (q->notify_open) {\n        q->pending = 0;\n        napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);\n    }'),
      'native/callback_queue.c:14: cbq_notify runs `q->pending = 0;`',
    ],
  ];
  for (const [queue, finding] of cases) {
    const r = runN(RULE, { 'native/callback_queue.c': queue });
    assert.deepEqual(findings(r), [
      `${finding}: the hook is the notify_mtx lock, if (q->notify_open) and its one nonblocking call, and the unlock, nothing else`,
    ]);
  }
});

test('the guarded call may be braced', () => {
  const r = runN(RULE, {
    'native/callback_queue.c': QUEUE_C.replace(HOOK, '    if (q->notify_open) {\n        napi_call_threadsafe_function(q->tsfn, NULL, napi_tsfn_nonblocking);\n    }'),
  });
  assert.equal(r.status, 0, r.text);
});

test('a hook that does not signal does nothing but (void) its opaque', () => {
  const r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      'void js_setup(void* p) {',
      'static int fired;\n\nstatic void noop_notify(void* opaque) {\n    (void)opaque;\n    fired = 1;\n}\n\nvoid js_teardown(void* p) {\n    kafka_common_Error_t* ffi_err = NULL;\n    FFI_DISCARD(kafka_x_P__set_callbacks_notify_err(p, noop_notify, NULL, &ffi_err));\n}\n\nvoid js_setup(void* p) {',
    ),
  });
  assert.deepEqual(findings(r), ['native/producer.c:38: noop_notify runs `fired = 1;`: a hook that does not signal does nothing']);
});

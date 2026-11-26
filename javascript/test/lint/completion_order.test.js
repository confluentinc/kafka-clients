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

// check-completion-order (scripts/lint/rules/completion_order.js).

const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { HEADER, PRODUCER_C, PRODUCER_TS, LISTS } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-completion-order';

// A listener of two methods sharing one run, the first required one given as
// `cond ? fn : NULL`, reporting unless its consumer's destroy has returned.
const LISTENER_H = `${HEADER}
kafka_x_L_t *kafka_x_L_new_err(void *self_, kafka_x_L_on_a_fn_t on_a, kafka_x_L_on_b_fn_t on_b, kafka_common_Error_t **out_err);
`;
const LISTENER_C = `#include "util.h"

static void listener_run(void* self_, int which, int64_t id) {
    kafka_common_Error_t* ffi_err = NULL;
    call_ref(self_, which);
    if (!c->destroyed) {
        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));
    }
}

static void listener_on_a(void* self_, int64_t id) {
    listener_run(self_, 0, id);
}

static void listener_on_b(void* self_, int64_t id) {
    listener_run(self_, 1, id);
}

void js_listen(void* self_, int has_a) {
    kafka_common_Error_t* ffi_err = NULL;
    kafka_x_L_t* l = NULL;
    error_discard(FFI_SET(l, kafka_x_L_new_err(self_, has_a ? listener_on_a : NULL, listener_on_b, &ffi_err)));
}
`;
const LISTENER_FILES = { 'confluent_kafka_bindings.h': LISTENER_H, 'native/consumer.c': LISTENER_C };
/** @param {boolean} via - whether the listener's entries name its run */
const listed = (via) => ({
  COMPLETIONS: [
    ...LISTS[RULE].COMPLETIONS,
    { key: 'listener_on_a', ...(via ? { via: 'listener_run' } : {}), reason: 'the test' },
    { key: 'listener_on_b', ...(via ? { via: 'listener_run' } : {}), reason: 'the test' },
  ],
});

test('the callback, then the future, then one report: in C and in TypeScript', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-completion-order: 2 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  let r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    call_ref(self_, metadata, error);\n    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }',
      '    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }\n    call_ref(self_, metadata, error);',
    ),
    'src/producer/producer.ts': PRODUCER_TS.replace(
      '      callback(m, e);\n      future.internalSetDelivery(m);',
      '      future.internalSetDelivery(m);\n      callback(m, e);',
    ),
  });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'send_on_completion: reports __set_callback_result_err (line 10) before running the callback (line 12)',
    'src/producer/producer.ts:11: records the delivery before running the user callback',
  ]);
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    call_ref(self_, metadata, error);',
      '    call_ref(self_, metadata, error);\n    call_ref(self_, metadata, error);',
    ),
    'src/producer/producer.ts': PRODUCER_TS.replace(
      '      future.internalSetDelivery(m);',
      '      future.internalSetDelivery(m);\n      future.internalSetDelivery(m);',
    ),
  });
  assert.deepEqual(findings(r), [
    'send_on_completion: calls call_ref 2 time(s) and __set_callback_result_err 1 time(s), not once each',
    'src/producer/producer.ts:11: runs the user callback 1 time(s) and records the delivery 2 time(s), not once each',
  ]);
});

test('a report outside the destroying guard is a finding (rule 6)', () => {
  const r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    if (!p->destroying) {\n', '    {\n') });
  assert.deepEqual(findings(r), [
    'send_on_completion: reports __set_callback_result_err (line 11) outside an if (!…destroying) or if (!…destroyed) guard',
  ]);
});

test('the C report must be what the guard guards; a nested block inside the guard is fine', () => {
  let r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }',
      '    if (!p->destroying) delivered = 1;\n    FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));',
    ),
  });
  assert.deepEqual(findings(r), [
    'send_on_completion: reports __set_callback_result_err (line 11) outside an if (!…destroying) or if (!…destroyed) guard',
  ]);
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    if (!p->destroying) {\n',
      '    if (!p->destroying) {\n        if (id < 0) {\n            id = 0;\n        }\n',
    ),
  });
  assert.equal(r.status, 0, r.text);
  // Unbraced, the report is the guarded statement.
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }',
      '    if (!p->destroying) FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));',
    ),
  });
  assert.equal(r.status, 0, r.text);
});

test('a method given as cond ? fn : NULL is a completion, and a via completion is checked in the function it hands its run to', () => {
  // Both listener methods are found (the first one only through the
  // conditional, or its entry would be unused) and checked in listener_run,
  // whose report its destroyed guard guards.
  let r = runN(RULE, LISTENER_FILES, listed(true));
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-completion-order: 4 item(s) checked'), r.text);
  // Without `via`, each method's own body is checked, and it reports nothing.
  r = runN(RULE, LISTENER_FILES, listed(false));
  assert.deepEqual(findings(r), [
    'listener_on_a: calls call_ref 0 time(s) and __set_callback_result_err 0 time(s), not once each',
    'listener_on_b: calls call_ref 0 time(s) and __set_callback_result_err 0 time(s), not once each',
  ]);
  // The run checked is listener_run's: a report before the callback is a finding for each method.
  r = runN(
    RULE,
    {
      ...LISTENER_FILES,
      'native/consumer.c': LISTENER_C.replace('    call_ref(self_, which);\n', '').replace(
        '    }\n}\n\nstatic void listener_on_a',
        '    }\n    call_ref(self_, which);\n}\n\nstatic void listener_on_a',
      ),
    },
    listed(true),
  );
  assert.deepEqual(findings(r), [
    'listener_on_a: reports __set_callback_result_err (line 6) before running the callback (line 8)',
    'listener_on_b: reports __set_callback_result_err (line 6) before running the callback (line 8)',
  ]);
  // A method that calls its run twice, or not at all.
  r = runN(
    RULE,
    {
      ...LISTENER_FILES,
      'native/consumer.c': LISTENER_C.replace(
        '    listener_run(self_, 0, id);\n',
        '    listener_run(self_, 0, id);\n    listener_run(self_, 0, id);\n',
      ).replace('    listener_run(self_, 1, id);\n', ''),
    },
    listed(true),
  );
  assert.deepEqual(findings(r), [
    'listener_on_a: calls listener_run 2 time(s), not once',
    'listener_on_b: calls listener_run 0 time(s), not once',
  ]);
  // A report outside the destroyed guard.
  r = runN(RULE, { ...LISTENER_FILES, 'native/consumer.c': LISTENER_C.replace('    if (!c->destroyed) {\n', '    {\n') }, listed(true));
  assert.deepEqual(findings(r), [
    'listener_on_a: reports __set_callback_result_err (line 7) outside an if (!…destroying) or if (!…destroyed) guard',
    'listener_on_b: reports __set_callback_result_err (line 7) outside an if (!…destroying) or if (!…destroyed) guard',
  ]);
});

test('a delivery callback is followed into the functions it calls, and one the rule cannot follow is a finding', () => {
  // The body moved into a module function that completes the future first.
  let r = runN(RULE, {
    'src/producer/producer.ts':
      PRODUCER_TS.replace(
        '    const onComplete = (m: unknown, e: unknown): void => {\n      callback(m, e);\n      future.internalSetDelivery(m);\n    };',
        '    const onComplete = (m: unknown, e: unknown): void => deliver(future, callback, m, e);',
      ) +
      '\nfunction deliver(future: Future, callback: Callback, m: unknown, e: unknown): void {\n  future.internalSetDelivery(m);\n  callback(m, e);\n}\n',
  });
  assert.deepEqual(findings(r), ['src/producer/producer.ts:11: records the delivery before running the user callback']);
  // A delivery callback that never completes the future.
  r = runN(RULE, {
    'src/producer/producer.ts': PRODUCER_TS.replace('      future.internalSetDelivery(m);\n', '      void future;\n'),
  });
  assert.deepEqual(findings(r), [
    'src/producer/producer.ts:11: runs the user callback 1 time(s) and records the delivery 0 time(s), not once each',
  ]);
  // One built by a call: nothing to read.
  r = runN(RULE, {
    'src/producer/producer.ts': PRODUCER_TS.replace('    native.send(onComplete);', '    native.send(((f) => f)(onComplete));'),
  });
  assert.deepEqual(findings(r), [
    'src/producer/producer.ts:15: hands native.send a delivery callback the rule cannot follow: a function literal, or a const, readonly or module function of src/',
  ]);
});

test('one completion site reached from two paths counts once: a Promise the callback returned completes it later', () => {
  let r = runN(RULE, {
    'src/producer/producer.ts':
      PRODUCER_TS.replace(
        '    const onComplete = (m: unknown, e: unknown): void => {\n      callback(m, e);\n      future.internalSetDelivery(m);\n    };',
        '    const onComplete = (m: unknown, e: unknown): unknown => {\n      const r: unknown = callback(m, e);\n' +
          '      if (r instanceof Promise) return r.then(() => complete(future, m));\n      return complete(future, m);\n    };',
      ) + '\nfunction complete(future: Future, m: unknown): void {\n  future.internalSetDelivery(m);\n}\n',
  });
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-completion-order: 2 item(s) checked'), r.text);
  // Two sites are still two.
  r = runN(RULE, {
    'src/producer/producer.ts': PRODUCER_TS.replace(
      '    const onComplete = (m: unknown, e: unknown): void => {\n      callback(m, e);\n      future.internalSetDelivery(m);\n    };',
      '    const onComplete = (m: unknown, e: unknown): unknown => {\n      const r: unknown = callback(m, e);\n' +
        '      if (r instanceof Promise) return r.then(() => future.internalSetDelivery(m));\n      return future.internalSetDelivery(m);\n    };',
    ),
  });
  assert.deepEqual(findings(r), [
    'src/producer/producer.ts:11: runs the user callback 1 time(s) and records the delivery 2 time(s), not once each',
  ]);
});

test('an unlisted completion and an unused entry are findings', () => {
  const r = runN(RULE, {}, { COMPLETIONS: [{ key: 'send_gone', reason: 'the test' }] });
  assert.deepEqual(findings(r), [
    'send_on_completion: is a delivery completion the rule does not list',
    'send_gone: is allowed as the test, but nothing matches it any more: delete the entry',
  ]);
  assert.deepEqual(
    require('../../scripts/lint/rules/completion_order').COMPLETIONS.map((e) => e.key),
    ['send_on_completion'],
  );
  void LISTS;
});

test('cannot run without the TypeScript program', () => {
  const r = runN(RULE, { 'tsconfig.json': undefined });
  assert.equal(r.err[1], '❌ check-completion-order: cannot run, no tsconfig.json');
});

test('no completion fails: 0 items checked', () => {
  const r = runN(RULE, { 'native/producer.c': 'void f(void) {}\n', 'src/producer/producer.ts': 'export {};\n' }, { COMPLETIONS: [] });
  assert.ok(r.err.includes('❌ check-completion-order: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

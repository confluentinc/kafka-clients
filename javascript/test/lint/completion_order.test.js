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

// N5 check-completion-order (scripts/lint/rules/completion_order.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { PRODUCER_C, PRODUCER_TS, LISTS } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-completion-order';

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
    'native/producer.c': PRODUCER_C.replace('    call_ref(self_, metadata, error);', '    call_ref(self_, metadata, error);\n    call_ref(self_, metadata, error);'),
    'src/producer/producer.ts': PRODUCER_TS.replace('      future.internalSetDelivery(m);', '      future.internalSetDelivery(m);\n      future.internalSetDelivery(m);'),
  });
  assert.deepEqual(findings(r), [
    'send_on_completion: calls call_ref 2 time(s) and __set_callback_result_err 1 time(s), not once each',
    'src/producer/producer.ts:11: runs the user callback 1 time(s) and records the delivery 2 time(s), not once each',
  ]);
});

test('a report outside the destroying guard is a finding (rule 6)', () => {
  const r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    if (!p->destroying) {\n', '    {\n') });
  assert.deepEqual(findings(r), ['send_on_completion: reports __set_callback_result_err (line 11) outside an if (!…destroying) guard']);
});

test('the C report must be what the guard guards; a nested block inside the guard is fine', () => {
  let r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }',
      '    if (!p->destroying) delivered = 1;\n    FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));',
    ),
  });
  assert.deepEqual(findings(r), ['send_on_completion: reports __set_callback_result_err (line 11) outside an if (!…destroying) guard']);
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

test('a delivery callback is followed into the functions it calls, and one the rule cannot follow is a finding', () => {
  // The body moved into a module function that completes the future first.
  let r = runN(RULE, {
    'src/producer/producer.ts': PRODUCER_TS.replace(
      "    const onComplete = (m: unknown, e: unknown): void => {\n      callback(m, e);\n      future.internalSetDelivery(m);\n    };",
      '    const onComplete = (m: unknown, e: unknown): void => deliver(future, callback, m, e);',
    ) + "\nfunction deliver(future: Future, callback: Callback, m: unknown, e: unknown): void {\n  future.internalSetDelivery(m);\n  callback(m, e);\n}\n",
  });
  assert.deepEqual(findings(r), ['src/producer/producer.ts:11: records the delivery before running the user callback']);
  // A delivery callback that never completes the future.
  r = runN(RULE, {
    'src/producer/producer.ts': PRODUCER_TS.replace("      future.internalSetDelivery(m);\n", '      void future;\n'),
  });
  assert.deepEqual(findings(r), ['src/producer/producer.ts:11: runs the user callback 1 time(s) and records the delivery 0 time(s), not once each']);
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
    'src/producer/producer.ts': PRODUCER_TS.replace(
      "    const onComplete = (m: unknown, e: unknown): void => {\n      callback(m, e);\n      future.internalSetDelivery(m);\n    };",
      '    const onComplete = (m: unknown, e: unknown): unknown => {\n      const r: unknown = callback(m, e);\n' +
        '      if (r instanceof Promise) return r.then(() => complete(future, m));\n      return complete(future, m);\n    };',
    ) + '\nfunction complete(future: Future, m: unknown): void {\n  future.internalSetDelivery(m);\n}\n',
  });
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-completion-order: 2 item(s) checked'), r.text);
  // Two sites are still two.
  r = runN(RULE, {
    'src/producer/producer.ts': PRODUCER_TS.replace(
      "    const onComplete = (m: unknown, e: unknown): void => {\n      callback(m, e);\n      future.internalSetDelivery(m);\n    };",
      '    const onComplete = (m: unknown, e: unknown): unknown => {\n      const r: unknown = callback(m, e);\n' +
        '      if (r instanceof Promise) return r.then(() => future.internalSetDelivery(m));\n      return future.internalSetDelivery(m);\n    };',
    ),
  });
  assert.deepEqual(findings(r), ['src/producer/producer.ts:11: runs the user callback 1 time(s) and records the delivery 2 time(s), not once each']);
});

test('an unlisted completion and an unused entry are findings', () => {
  const r = runN(RULE, {}, { COMPLETIONS: [{ key: 'send_gone', reason: 'the test' }] });
  assert.deepEqual(findings(r), [
    'send_on_completion: is a delivery completion the rule does not list',
    'send_gone: is allowed as the test, but nothing matches it any more: delete the entry',
  ]);
  assert.deepEqual(require('../../scripts/lint/rules/completion_order').COMPLETIONS.map((e) => e.key), ['send_on_completion']);
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

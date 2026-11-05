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
    'src/producer.c': PRODUCER_C.replace(
      '    call_ref(self_, metadata, error);\n    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }',
      '    if (!p->destroying) {\n        FFI_DISCARD(kafka_x_P__set_callback_result_err(self_, id, NULL, NULL, &ffi_err));\n    }\n    call_ref(self_, metadata, error);',
    ),
    'lib/producer/producer.ts': PRODUCER_TS.replace(
      '      callback(m, e);\n      future[\'_complete\'](m);',
      '      future[\'_complete\'](m);\n      callback(m, e);',
    ),
  });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'send_on_completion: reports __set_callback_result_err (line 10) before running the callback (line 12)',
    'lib/producer/producer.ts:11: completes the future before running the user callback',
  ]);
  r = runN(RULE, {
    'src/producer.c': PRODUCER_C.replace('    call_ref(self_, metadata, error);', '    call_ref(self_, metadata, error);\n    call_ref(self_, metadata, error);'),
    'lib/producer/producer.ts': PRODUCER_TS.replace('      future[\'_complete\'](m);', '      future[\'_complete\'](m);\n      future[\'_complete\'](m);'),
  });
  assert.deepEqual(findings(r), [
    'send_on_completion: calls call_ref 2 time(s) and __set_callback_result_err 1 time(s), not once each',
    'lib/producer/producer.ts:11: runs the user callback 1 time(s) and completes the future 2 time(s), not once each',
  ]);
});

test('a report outside the destroying guard is a finding (rule 6)', () => {
  const r = runN(RULE, { 'src/producer.c': PRODUCER_C.replace('    if (!p->destroying) {\n', '    {\n') });
  assert.deepEqual(findings(r), ['send_on_completion: reports __set_callback_result_err (line 11) outside an if (!…destroying) guard']);
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
  const r = runN(RULE, { 'src/producer.c': 'void f(void) {}\n', 'lib/producer/producer.ts': 'export {};\n' }, { COMPLETIONS: [] });
  assert.ok(r.err.includes('❌ check-completion-order: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

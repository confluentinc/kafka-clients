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

// A1 check-waiting-call-promise (scripts/lint/rules/waiting_call_promise.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint: lintAll, findings } = require('./_project');
const { withLists } = require('./_c_fixture');
const WAITING = require('../../scripts/lint/rules/waiting_call_promise');
const { FILES, PRODUCER_TS } = require('./_native_fixture');

const RULE = 'check-waiting-call-promise';

// The fixture has no KafkaFuture: the rule runs without the real tree's
// DEPENDANTS, which would match nothing there.
const lint = (root, names) => lintAll(root, names, { rules: [withLists(WAITING, { DEPENDANTS: [] })] });

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, ...FILES, ...files });

test('a method reaching a _cb twin, through a private method and a C helper, returns a Promise; one reaching none does not', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-waiting-call-promise: 2 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/producer.ts': PRODUCER_TS.replace('flush(): Promise<void> {\n    return new Promise((resolve) => this.#start(resolve));', 'flush(): void {\n    this.#start(() => {});')
        .replace('pump(): void {\n    native.pump();', 'pump(): Promise<void> {\n    native.pump();\n    return Promise.resolve();'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'Producer.flush: reaches kafka_x_Producer_flush_cb but returns void',
    'Producer.pump: returns Promise<void> but its native path (native.pump) reaches no _cb twin',
  ]);
});

test('the path is followed through a module helper, another class and an alias of the handle', () => {
  const helper = `import native = require('../internal/native');
function startFlush(done: () => void): void {
  reallyStart(done);
}
function reallyStart(done: () => void): void {
  native.flush(done);
}
class Inner {
  run(): void {
    const n = native;
    n.flush(() => {});
  }
}
export class Producer {
  flush(): void {
    startFlush(() => {});
  }
  close(): void {
    new Inner().run();
  }
  pump(): void {
    native.pump();
  }
}
`;
  const r = lint(fixture({ 'src/producer/producer.ts': helper }), [RULE]);
  assert.deepEqual(findings(r), [
    'Producer.flush: reaches kafka_x_Producer_flush_cb but returns void',
    'Producer.close: reaches kafka_x_Producer_flush_cb but returns void',
  ]);
});

test('a native function with no registration, or no C definition, is unresolved', () => {
  const r = lint(
    fixture({
      'src/internal/native.ts': "interface Native {\n  flush(done: () => void): void;\n  pump(): void;\n  gone(): void;\n  setFactories(): void;\n}\ndeclare const loaded: Native;\nconst native: Native = loaded;\nexport = native;\n",
      'src/producer/producer.ts': PRODUCER_TS.replace('native.pump();', 'native.gone();'),
      'native/producer.c': FILES['native/producer.c'].replace('napi_value js_flush(', 'napi_value js_flush_renamed('),
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    "Producer.flush: native.flush's implementation js_flush is defined in no native/*.c",
    'Producer.pump: native.gone is registered by no napi_property_descriptor of native/addon.c',
  ]);
});

test('cannot run without the header, or the C sources', () => {
  let r = lint(project({ 'tsconfig.json': TSCONFIG, ...FILES, 'confluent_kafka_bindings.h': undefined }), [RULE]);
  assert.match(r.err[1], /^❌ check-waiting-call-promise: cannot run, no bindings header at /);
  const files = { ...FILES };
  delete files['native/addon.c'];
  delete files['native/producer.c'];
  r = lint(project({ 'tsconfig.json': TSCONFIG, ...files }), [RULE]);
  assert.equal(r.err[1], '❌ check-waiting-call-promise: cannot run, no C sources under native/');
});

test('no method with a native call fails: 0 items checked', () => {
  const r = lint(fixture({ 'src/producer/producer.ts': 'export class Producer {\n  ready(): boolean { return true; }\n}\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-waiting-call-promise: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('a DEPENDANTS entry that matches nothing is a finding; the real tree lists the three dependants', () => {
  const r = lintAll(fixture(), [RULE], { rules: [withLists(WAITING, { DEPENDANTS: [{ key: 'Producer.nothing', reason: 'the test' }] })] });
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), ['Producer.nothing: is allowed as the test, but nothing matches it any more: delete the entry']);
  assert.deepEqual(
    WAITING.DEPENDANTS.map((d) => d.key),
    ['KafkaFuture.allOf', 'KafkaFuture.thenApply', 'KafkaFuture.whenComplete'],
  );
});

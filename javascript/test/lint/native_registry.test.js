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

// A2 check-native-registry (scripts/lint/rules/native_registry.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { FILES, ADDON, PRODUCER_TS } = require('./_native_fixture');

const RULE = 'check-native-registry';

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, ...FILES, ...files });

test('registrations and uses agree, and only the loader and src/internal/native.ts touch the addon', () => {
  const r = lint(fixture({ 'src/internal/native_loader.ts': "export const ADDON_FILE = 'x.node';\n", 'src/producer/index.ts': "export { Producer } from './producer';\n" }), [RULE]);
  assert.equal(r.status, 0, r.text);
  // 4 registrations (probe, a test-only one), 3 uses, 4 src/ files.
  assert.ok(r.out.includes('✅ check-native-registry: 11 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'native/addon.c': ADDON.replace('        {"probe", NULL, js_probe, NULL, NULL, NULL, napi_default, NULL},\n', '').replace('{"setFactories"', '{"extra", NULL, js_extra, NULL, NULL, NULL, napi_default, NULL},\n        {"setFactories"'),
      'src/producer/producer.ts': PRODUCER_TS.replace('native.pump();', "native.pump();\n    native.setFactories();\n    void require('../internal/native_loader');\n    void 'addon.node';\n    void `${String(native)}/other.node`;"),
      'src/producer/index.ts': "import '../internal/native';\nexport { Producer } from './producer';\n",
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'probe: is allowed unused as a test-only registration: the load check test/unit/internal/native.test.js calls, but native/addon.c does not register it: delete the entry',
    'native/addon.c:extra: registers extra (js_extra), which src/ never uses',
    'src/producer/producer.ts:8: native.setFactories is the one-time setup, which only src/internal/native.ts does',
    "src/producer/index.ts:1: imports '../internal/native': no index.ts loads the addon",
    "src/producer/producer.ts:10: names the .node file 'addon.node': only src/internal/native_loader.ts finds the addon",
    "src/producer/producer.ts:11: names the .node file '${String(native)}/other.node': only src/internal/native_loader.ts finds the addon",
    "src/producer/producer.ts:9: imports the loader '../internal/native_loader': only src/internal/native.ts loads the addon",
  ]);
});

test('the test-only entry also needs its test to call the registration', () => {
  const r = lint(fixture({ 'test/unit/internal/native.test.js': "require('../../../dist/internal/native');\n" }), [RULE]);
  assert.deepEqual(findings(r), [
    'probe: is allowed unused as a test-only registration: the load check test/unit/internal/native.test.js calls, but test/unit/internal/native.test.js does not call it: delete the entry',
  ]);
});

test('a use through an alias of the handle counts', () => {
  const r = lint(fixture({ 'src/producer/producer.ts': PRODUCER_TS.replace('    native.pump();', '    const n = native;\n    n.pump();') }), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('cannot run without the C sources', () => {
  const files = { ...FILES };
  delete files['native/addon.c'];
  delete files['native/producer.c'];
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...files }), [RULE]);
  assert.equal(r.err[1], '❌ check-native-registry: cannot run, no C sources under native/');
});

test('without src/internal/native.ts or native/addon.c it cannot run either', () => {
  let r = lint(fixture({ 'src/internal/native.ts': undefined }), [RULE]);
  assert.equal(r.err[1], '❌ check-native-registry: cannot run, src/internal/native.ts exports no handle (`export = <handle>`)');
  r = lint(fixture({ 'native/addon.c': undefined }), [RULE]);
  assert.equal(r.err[1], '❌ check-native-registry: cannot run, no native/addon.c');
});

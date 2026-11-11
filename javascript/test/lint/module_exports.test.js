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

// check-module-exports (scripts/lint/rules/module_exports.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-module-exports';

const REGISTRY = `export const ERRORS = [
  ['../illegal_state_error', 'IllegalStateError', 'java.lang.IllegalStateException'],
  ['../common/kafka_error', 'KafkaError', 'org.apache.kafka.common.KafkaException'],
];
`;

/** A src/ whose indexes follow the rule, plus `files`. */
function fixture(files = {}) {
  return project({
    'tsconfig.json': TSCONFIG,
    'src/internal/error_registry.ts': REGISTRY,
    'src/internal/index.ts': "export * as anything from '../producer';\n",
    'src/duration.ts': 'export type Duration = number;\n',
    'src/illegal_state_error.ts': 'export class IllegalStateError extends Error {}\n',
    'src/index.ts':
      "export type { Duration } from './duration';\nexport { IllegalStateError } from './illegal_state_error';\nexport declare const __esModule: true;\n",
    'src/common/kafka_error.ts': 'export class KafkaError extends Error {}\n',
    'src/common/index.ts': "export { KafkaError } from './kafka_error';\nexport declare const __esModule: true;\n",
    'src/producer/rec.ts': 'export class Rec {}\n',
    'src/producer/index.ts': "export { Rec } from './rec';\n",
    ...files,
  });
}

test('indexes re-exporting their own modules by name, and a root of built-in errors and Duration, pass', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // The statements of src/index.ts (3), src/common/index.ts (2) and src/producer/index.ts (1); src/internal/ is not read.
  assert.ok(r.out.includes('✅ check-module-exports: 6 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/index.ts':
        "export type { Duration } from './duration';\nexport { IllegalStateError } from './illegal_state_error';\n" +
        "export * as common from './common';\nexport { KafkaError } from './common/kafka_error';\nexport * from './illegal_state_error';\n" +
        "export { Wrong } from './wrong';\nexport declare const __esModule: true;\n",
      'src/wrong.ts': 'export class Wrong {}\n',
      'src/common/index.ts': "export { KafkaError } from './kafka_error';\nexport const VERSION = 1;\nexport { Rec } from '../producer/rec';\n",
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/common/index.ts:2: declares something of its own: an index only re-exports the modules of its folder',
    'src/common/index.ts:../producer/rec: re-exports ../producer/rec, which is not a module of src/common/: a package has one import path',
    'src/index.ts:common: re-exports ./common as the namespace common: each package has its own import path, and an index re-exports its classes by name',
    'src/index.ts:./common/kafka_error: re-exports ./common/kafka_error, which is not a module of src/: a package has one import path',
    "src/index.ts:*: exports everything of ./illegal_state_error: the root exports only Java's built-in errors and Duration",
    "src/index.ts:Wrong: exports Wrong: the root exports only Java's built-in errors and Duration",
  ]);
  assert.ok(r.err.includes('❌ check-module-exports: 6 finding(s).'), r.text);
});

test('a src/ with no index fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'src/a.ts': 'export const a = 1;\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-module-exports: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

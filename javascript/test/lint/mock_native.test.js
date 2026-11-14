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

// check-mock-native (scripts/lint/rules/mock_native.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-mock-native';

const NATIVE_TS = `interface Native {
  mockProducerCreate(): number;
}
declare const loaded: Native;
const native: Native = loaded;
export = native;
`;

const fixture = (files) =>
  project({ 'tsconfig.json': TSCONFIG, 'src/internal/native.ts': NATIVE_TS, 'src/producer/index.ts': "export { MockProducer } from './mock_producer';\n", ...files });

test('a mock whose constructor, directly or through a helper, or whose field reaches the addon passes', () => {
  for (const body of [
    'readonly #handle: number;\n  constructor() { this.#handle = native.mockProducerCreate(); }',
    'readonly #handle: number;\n  constructor() { this.#handle = create(); }',
    'readonly #handle = native.mockProducerCreate();',
  ]) {
    const r = lint(
      fixture({
        'src/producer/mock_producer.ts':
          "import native = require('../internal/native');\nfunction create(): number { return native.mockProducerCreate(); }\nvoid create;\n" +
          `export class MockProducer {\n  ${body}\n}\n`,
      }),
      [RULE],
    );
    assert.equal(r.status, 0, `${body}\n${r.text}`);
  }
});

test('a mock translated in TypeScript, holding no native handle, is a finding', () => {
  const r = lint(fixture({ 'src/producer/mock_producer.ts': 'export class MockProducer {\n  readonly #history: unknown[] = [];\n}\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "src/producer/mock_producer.ts:MockProducer: holds no native handle: a mock wraps the FFI's mock, its constructor or a field reaching native.<fn>",
  ]);
});

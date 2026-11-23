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

// check-native-wrappers (scripts/lint/rules/native_wrappers.js).

const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-native-wrappers';

const NATIVE_TS = `interface Native {
  mockProducerCreate(): number;
  headerNew(): number;
  recordFutureGet(): void;
}
declare const loaded: Native;
const native: Native = loaded;
export = native;
`;

const IMPORT = "import native = require('../internal/native');\n";

/** The wrappers of the fixture: the mock, through a helper, and Header, through a field. */
const WRAPPERS = {
  'src/producer/mock_producer.ts':
    `${IMPORT}function create(): number { return native.mockProducerCreate(); }\n` +
    'export class MockProducer {\n  readonly #handle: number;\n  constructor() { this.#handle = create(); }\n}\n',
  'src/common/header/header.ts':
    "import native = require('../../internal/native');\nexport class Header {\n  readonly #handle = native.headerNew();\n}\n",
};

const fixture = (/** @type {Record<string, string | undefined>} */ files) =>
  project({
    'tsconfig.json': TSCONFIG,
    'src/internal/native.ts': NATIVE_TS,
    'src/common/topic_partition.ts': 'export class TopicPartition {\n  topic(): string { return ""; }\n}\n',
    ...WRAPPERS,
    ...files,
  });

/** A lint-mapping.json listing `entries` as `nativeCallers`. */
const listing = (/** @type {Record<string, string>} */ entries) => `${JSON.stringify({ nativeCallers: entries }, null, 2)}\n`;

test('the wrappers the JS conventions name hold a native handle, directly, through a helper or through a field', () => {
  for (const body of [
    'readonly #handle: number;\n  constructor() { this.#handle = native.mockProducerCreate(); }',
    'readonly #handle: number;\n  constructor() { this.#handle = create(); }',
    'readonly #handle = native.mockProducerCreate();',
  ]) {
    const r = lint(
      fixture({
        'src/producer/mock_producer.ts':
          `${IMPORT}function create(): number { return native.mockProducerCreate(); }\nvoid create;\n` +
          `export class MockProducer {\n  ${body}\n}\n`,
      }),
      [RULE],
    );
    assert.equal(r.status, 0, `${body}\n${r.text}`);
  }
  // The two wrappers, and TopicPartition.
  assert.ok(lint(fixture({}), [RULE]).out.includes('✅ check-native-wrappers: 3 item(s) checked'));
});

test('a wrapper holding no native handle is a finding, Header and Headers as the mocks', () => {
  const r = lint(
    fixture({
      'src/producer/mock_producer.ts': 'export class MockProducer {\n  readonly #history: unknown[] = [];\n}\n',
      'src/common/header/header.ts': 'export class Header {\n  readonly #key = "";\n}\n',
      'src/common/header/headers.ts': 'export class Headers {}\n',
      'src/producer/kafka_producer.ts': 'export class KafkaProducer {}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "src/common/header/header.ts:Header: holds no native handle: it wraps the C RecordHeader (the JS conventions' RecordHeader), its constructor or a field reaching native.<fn>",
    'src/common/header/headers.ts:Headers: holds no native handle: it wraps the C RecordHeaders, its constructor or a field reaching native.<fn>',
    "src/producer/kafka_producer.ts:KafkaProducer: holds no native handle: it wraps the FFI's producer, its constructor or a field reaching native.<fn>",
    "src/producer/mock_producer.ts:MockProducer: holds no native handle: it wraps the FFI's mock, its constructor or a field reaching native.<fn>",
  ]);
});

test('any other class, exported or not, and any exported function reaching the addon is a finding unless listed', () => {
  const callers = {
    // Directly, in a method.
    'src/common/topic_partition.ts': `${IMPORT.replace('../', '../')}export class TopicPartition {\n  topic(): string { native.recordFutureGet(); return ""; }\n}\n`,
    // Through a src/internal/ helper, from a class no module exports.
    'src/internal/probe.ts': "import native = require('./native');\nexport function probe(): void { native.recordFutureGet(); }\n",
    'src/common/kafka_future.ts':
      "import { probe } from '../internal/probe';\nclass FutureImpl {\n  get(): void { probe(); }\n}\n" +
      'export class KafkaFuture {\n  static of(): unknown { return new FutureImpl(); }\n}\n',
    // An exported function, through its module's own helper.
    'src/common/serialization/string_serializer.ts':
      "import native = require('../../internal/native');\nfunction get(): void { native.recordFutureGet(); }\n" +
      'export function stringSerializer(): void { get(); }\n',
  };
  const r = lint(fixture(callers), [RULE]);
  assert.equal(r.status, 1);
  const tail =
    "which only the classes the JS conventions name call: list it in lint-mapping.json's nativeCallers with the reason the rules give, or move the call";
  assert.deepEqual(findings(r), [
    `src/common/kafka_future.ts:FutureImpl: reaches the addon (native.recordFutureGet, src/internal/probe.ts:2), ${tail}`,
    `src/common/topic_partition.ts:TopicPartition: reaches the addon (native.recordFutureGet, src/common/topic_partition.ts:3), ${tail}`,
    `src/common/serialization/string_serializer.ts:stringSerializer: reaches the addon (native.recordFutureGet, src/common/serialization/string_serializer.ts:2), ${tail}`,
  ]);
  // KafkaFuture calls into FutureImpl, a class of its own: the call is FutureImpl's.
  const listed = lint(
    fixture({
      ...callers,
      'lint-mapping.json': listing({
        'src/common/kafka_future.ts:FutureImpl': 'a future over a native handle',
        'src/common/topic_partition.ts:TopicPartition': 'a reason',
        'src/common/serialization/string_serializer.ts:stringSerializer': 'a reason',
      }),
    }),
    [RULE],
  );
  assert.equal(listed.status, 0, listed.text);
});

test("a call into a wrapper is the wrapper's, not the caller's", () => {
  const r = lint(
    fixture({
      'src/common/topic_partition.ts':
        "import { Header } from './header/header';\nexport class TopicPartition {\n  header(): Header { return new Header(); }\n}\n",
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
});

test('an entry naming no caller, a wrapper, or no reason is a finding', () => {
  const r = lint(
    fixture({
      'lint-mapping.json': listing({
        'src/common/topic_partition.ts:TopicPartition': 'it calls nothing',
        'src/producer/mock_producer.ts:MockProducer': 'a wrapper',
        'src/common/missing.ts:Missing': '',
      }),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  const unused =
    'nativeCallers lists it, but no class or exported function so keyed reaches the addon (a wrapper the JS conventions name needs no entry): drop the entry';
  assert.deepEqual(findings(r), [
    `src/common/topic_partition.ts:TopicPartition: ${unused}`,
    `src/producer/mock_producer.ts:MockProducer: ${unused}`,
    'src/common/missing.ts:Missing: nativeCallers lists it with no reason',
    `src/common/missing.ts:Missing: ${unused}`,
  ]);
});

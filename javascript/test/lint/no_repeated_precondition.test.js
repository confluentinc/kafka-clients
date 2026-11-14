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

// check-no-repeated-precondition (scripts/lint/rules/no_repeated_precondition.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-no-repeated-precondition';

test("the binding's own checks, with their own messages, pass; a precondition named in a comment is no literal", () => {
  const r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'src/a.ts':
        "// The FFI answers 'The timeout cannot be negative.' itself.\n" +
        "export const NULL_TIMEOUT = 'timeout must not be null';\n" +
        'export const FORM = `close() takes one of (), (timeout); got a string`;\n',
      'native/a.c': '/* Invalid negative timeout */\nconst char* m = "must be a number";\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
});

test('each precondition message the FFI or the core checks is a finding, in TypeScript and in C', () => {
  const r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'src/a.ts':
        "export const A = 'The timeout cannot be negative.';\n" +
        'export const B = (ms: number): string => `Invalid negative timeout ${ms}`;\n',
      'native/a.c': 'const char* m = "Timeout must not be negative";\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "src/a.ts:1: writes \"The timeout cannot be negative.\", the message of a precondition the binding does not repeat (Java's KafkaProducer.close and AsyncKafkaConsumer.close; the producer FFI's close checks it)",
    "src/a.ts:2: writes \"Invalid negative timeout\", the message of a precondition the binding does not repeat (Java's Timer; the consumer FFI checks a negative timeout)",
    "native/a.c:1: writes \"Timeout must not be negative\", the message of a precondition the binding does not repeat (the core's _with_timeout check, which the consumer FFI reports)",
  ]);
});

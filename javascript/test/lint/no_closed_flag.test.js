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

// check-no-closed-flag (scripts/lint/rules/no_closed_flag.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-no-closed-flag';

const CLEAN = {
  'tsconfig.json': TSCONFIG,
  'src/client.ts':
    'export class Client {\n  #handle = 1;\n  #readOnly = false;\n  close(): void { void this.#handle; void this.#readOnly; }\n}\n' +
    "export const READ_ONLY = 'RecordHeaders has been closed.';\n",
  'native/queue.c':
    '// p->closed in a comment is no code.\ntypedef struct { int self_strong; int closed_now; } Q;\n' +
    'int cbq_closed(Q* q) { q->self_strong = 0; return 1; }\nint cbq_is_closed(const Q* q) { return !q->self_strong; }\n',
};

test("a client with no closed state and no closed message passes: the queue's record of the core's answer is none", () => {
  const r = lint(project(CLEAN), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('each closed field, in a class or in C, and each closed message is a finding', () => {
  const r = lint(
    project({
      ...CLEAN,
      'src/client.ts':
        'export class Client {\n  #closed = false;\n  private _closing = false;\n  isClosed = false;\n}\n' +
        "export class Mock {\n  closed = false;\n  fail(): never { throw new Error(`MockProducer is already closed.`); }\n}\n" +
        "export const M = 'Cannot perform operation after producer has been closed';\n",
      'native/queue.c': 'typedef struct { int closed; } Q;\nvoid f(Q* q) {\n  if (q->closed) return;\n  throw_error("This consumer has already been closed.");\n}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/client.ts:2: Client has the field #closed: the binding keeps no closed flag',
    'src/client.ts:3: Client has the field _closing: the binding keeps no closed flag',
    'src/client.ts:4: Client has the field isClosed: the binding keeps no closed flag',
    'src/client.ts:7: Mock has the field closed: the binding keeps no closed flag',
    'native/queue.c:1: names closed: the binding keeps no closed flag',
    'native/queue.c:3: names closed: the binding keeps no closed flag',
    'src/client.ts:8: writes Java\'s closed message "MockProducer is already closed.": only the core reports a closed client',
    'src/client.ts:10: writes Java\'s closed message "Cannot perform operation after producer has been closed": only the core reports a closed client',
    'native/queue.c:4: writes Java\'s closed message "This consumer has already been closed.": only the core reports a closed client',
  ]);
});

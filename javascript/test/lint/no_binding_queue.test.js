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

// N8 check-no-binding-queue (scripts/lint/rules/no_binding_queue.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { PRODUCER_C, PRODUCER_TS } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-no-binding-queue';

test('no lock outside the queue, no store of records', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
});

test('each finding kind, with its exact text', () => {
  const r = runN(RULE, {
    'native/producer.c': `static pthread_mutex_t producer_lock;\n${PRODUCER_C}`,
    'src/producer/producer.ts': PRODUCER_TS.replace('export class Producer {', 'const MAX_RECORDS = 10;\nvoid MAX_RECORDS;\nexport class Producer {\n  #pending: unknown[] = [];\n  #byTopic = new Map<string, number>();'),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:1: declares the pthread_mutex_t producer_lock: no lock outside native/callback_queue.c',
    'src/producer/producer.ts:MAX_RECORDS: names a cap: the binding caps no records',
    'Producer.#pending: holds a unknown[]: the binding keeps no records',
    'Producer.#byTopic: holds a Map<string, number>: the binding keeps no records',
  ]);
});

test('the lock entry allows its one declaration; a module store and a parameter property are stores too', () => {
  const { UTIL_H } = require('./_c_fixture');
  const r = runN(RULE, {
    'native/util.h': UTIL_H.replace('    pthread_mutex_t notify_mtx;', '    pthread_mutex_t notify_mtx;\n    pthread_mutex_t record_mtx;'),
    'src/producer/producer.ts': PRODUCER_TS.replace(
      'export class Producer {',
      'const pendingRecords: string[] = [];\nvoid pendingRecords;\nexport class Producer {\n  constructor(private readonly records: string[] = []) {}',
    ),
  });
  assert.deepEqual(findings(r), [
    'native/util.h:7: declares the pthread_mutex_t CallbackQueue.record_mtx: no lock outside native/callback_queue.c',
    'src/producer/producer.ts:pendingRecords: holds a string[] at module level: the binding keeps no records',
    'Producer.records: holds a string[]: the binding keeps no records',
  ]);
});

test('a private helper of src/ is read too; a generated module is not', () => {
  const queue = 'export class SendQueue {\n  private readonly records: unknown[] = [];\n  push(r: unknown): void {\n    if (this.records.length < SEND_QUEUE_LIMIT) this.records.push(r);\n  }\n}\n';
  const r = runN(RULE, {
    'src/internal/send_queue.ts': `const SEND_QUEUE_LIMIT = 10000;\n${queue}`,
    'src/common/errors/_table.ts': '// GENERATED, DO NOT EDIT: by the test.\nexport const TABLE: string[] = [];\n',
  });
  assert.deepEqual(findings(r), [
    'src/internal/send_queue.ts:SEND_QUEUE_LIMIT: names a cap: the binding caps no records',
    'SendQueue.records: holds a unknown[]: the binding keeps no records',
  ]);
});

test('an allowlist entry that matches nothing is a finding', () => {
  const r = runN(RULE, {}, { ALLOWED: [{ key: 'native/util.h:CallbackQueue.notify_mtx', reason: "the queue's lock" }, { key: 'Producer.#gone', reason: 'the test' }] });
  assert.deepEqual(findings(r), ['Producer.#gone: is allowed as the test, but nothing matches it any more: delete the entry']);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'native/producer.c': undefined, 'native/callback_queue.c': undefined, 'native/record.c': undefined });
  assert.equal(r.err[1], '❌ check-no-binding-queue: cannot run, no C sources under native/');
});

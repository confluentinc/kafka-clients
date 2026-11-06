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

// N7 check-zero-copy (scripts/lint/rules/zero_copy.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { RECORD_C, PRODUCER_C } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-zero-copy';

test('bytes read over their own memory, the listed allocations only', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-zero-copy: 2 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  let r = runN(RULE, {
    'src/record.c': RECORD_C.replace('    out->bytes.len = (int32_t)length;', '    out->bytes.len = (int32_t)length;\n    memcpy(out->copy, data, length);'),
    'src/producer.c': PRODUCER_C.replace('    void* ctx = calloc(1, 8);', '    void* ctx = calloc(1, 8);\n    void* buf = malloc(8);'),
  });
  assert.deepEqual(findings(r), [
    'src/producer.c:25: js_send calls malloc on the send path, which no entry lists',
    'src/record.c:9: js_bytes calls memcpy on the send path: no copy of a key or value',
  ]);
  r = runN(RULE, { 'src/record.c': RECORD_C.replace('    out->bytes.data = data;\n', '') });
  assert.deepEqual(findings(r), [
    'src/record.c, src/producer.c: no function reads a key or value with napi_get_typedarray_info / napi_get_buffer_info into a kafka_Bytes_t',
  ]);
});

test('an entry allows its one call site: a second calloc in the function is a finding', () => {
  const r = runN(RULE, { 'src/producer.c': PRODUCER_C.replace('    void* ctx = calloc(1, 8);', '    void* ctx = calloc(1, 8);\n    void* more = calloc(1, 8);') });
  assert.deepEqual(findings(r), ['src/producer.c:25: js_send calls calloc on the send path, which no entry lists']);
});

test('an allowlist entry that matches nothing is a finding', () => {
  const r = runN(RULE, {}, { ALLOWED: [{ key: 'src/producer.c:js_send:calloc#1', reason: "the send's context" }, { key: 'src/record.c:gone:malloc#1', reason: 'the test' }] });
  assert.deepEqual(findings(r), ['src/record.c:gone:malloc#1: is allowed as the test, but nothing matches it any more: delete the entry']);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'src/producer.c': undefined, 'src/callback_queue.c': undefined, 'src/record.c': undefined });
  assert.equal(r.err[1], '❌ check-zero-copy: cannot run, no C sources under src/');
});

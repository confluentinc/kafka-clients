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

// N7 check-one-copy (scripts/lint/rules/one_copy.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { RECORD_C, PRODUCER_C } = require('./_c_fixture');
const { runN } = require('./_n_harness');

const RULE = 'check-one-copy';

test('bytes read into a kafka_Bytes_t, the one listed copy and the listed allocations only', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  // The C borrow, the context's allocation, the copy's allocation and its two
  // memcpy, and the 3 calls of the fixture's src/.
  assert.ok(r.out.includes('✅ check-one-copy: 8 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  let r = runN(RULE, {
    'native/record.c': RECORD_C.replace('    out->bytes.len = (int32_t)length;', '    out->bytes.len = (int32_t)length;\n    memcpy(out->copy, data, length);'),
    'native/producer.c': PRODUCER_C.replace('    void* ctx = calloc(1, 8);', '    void* ctx = calloc(1, 8);\n    void* buf = malloc(8);'),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:25: js_send calls malloc on the send path, which no entry lists',
    'native/record.c:9: js_bytes calls memcpy on the send path: no copy of a key or value but the listed one',
  ]);
  r = runN(RULE, { 'native/record.c': RECORD_C.replace('    out->bytes.data = data;\n', '') });
  assert.deepEqual(findings(r), [
    'native/record.c, native/producer.c: no function reads a key or value with napi_get_typedarray_info / napi_get_buffer_info into a kafka_Bytes_t',
  ]);
});

test("the copy's entries allow its memcpy calls only; another copy is never allowed", () => {
  // A third memcpy in the copy function is no listed one.
  let r = runN(RULE, {
    'native/record.c': RECORD_C.replace('    *copy = bytes;', '    memcpy(bytes, value->bytes.data, 1);\n    *copy = bytes;'),
  });
  assert.deepEqual(findings(r), ['native/record.c:16: js_bytes_copy calls memcpy on the send path: no copy of a key or value but the listed one']);
  // A strdup cannot be listed: its entry is unused, and the call a finding.
  r = runN(
    RULE,
    { 'native/record.c': RECORD_C.replace('    *copy = bytes;', '    char* s = strdup("k");\n    *copy = bytes;') },
    {
      ALLOWED: [
        { key: 'native/producer.c:js_send:calloc#1', reason: "the send's context" },
        { key: 'native/record.c:js_bytes_copy:malloc#1', reason: 'the copy' },
        { key: 'native/record.c:js_bytes_copy:memcpy#1', reason: 'the key' },
        { key: 'native/record.c:js_bytes_copy:memcpy#2', reason: 'the value' },
        { key: 'native/record.c:js_bytes_copy:strdup#1', reason: 'a duplicate' },
      ],
    },
  );
  assert.deepEqual(findings(r), [
    'native/record.c:16: js_bytes_copy calls strdup on the send path: no copy of a key or value but the listed one',
    'native/record.c:js_bytes_copy:strdup#1: is allowed as a duplicate, but nothing matches it any more: delete the entry',
  ]);
});

test('an entry allows its one call site: a second calloc in the function is a finding', () => {
  const r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    void* ctx = calloc(1, 8);', '    void* ctx = calloc(1, 8);\n    void* more = calloc(1, 8);') });
  assert.deepEqual(findings(r), ['native/producer.c:25: js_send calls calloc on the send path, which no entry lists']);
});

test('js_bytes_copy is called only from its listed call sites: a second call is a finding', () => {
  const LISTS = {
    ALLOWED: [
      { key: 'native/producer.c:js_send:calloc#1', reason: "the send's context" },
      { key: 'native/record.c:js_bytes_copy:malloc#1', reason: 'the copy' },
      { key: 'native/record.c:js_bytes_copy:memcpy#1', reason: 'the key' },
      { key: 'native/record.c:js_bytes_copy:memcpy#2', reason: 'the value' },
      { key: 'native/producer.c:js_send:js_bytes_copy#1', reason: "send's one copy" },
    ],
  };
  const once = PRODUCER_C.replace('    void* ctx = calloc(1, 8);', '    void* ctx = calloc(1, 8);\n    js_bytes_copy(0, 0, 0);');
  assert.equal(runN(RULE, { 'native/producer.c': once }, LISTS).status, 0);
  const twice = once.replace('    js_bytes_copy(0, 0, 0);', '    js_bytes_copy(0, 0, 0);\n    js_bytes_copy(0, 0, 0);');
  assert.deepEqual(findings(runN(RULE, { 'native/producer.c': twice }, LISTS)), [
    'native/producer.c:26: js_send calls js_bytes_copy: the key and value are copied once, at the listed call site only',
  ]);
});

test('an allowlist entry that matches nothing is a finding', () => {
  const r = runN(RULE, {}, {
    ALLOWED: [
      { key: 'native/producer.c:js_send:calloc#1', reason: "the send's context" },
      { key: 'native/record.c:js_bytes_copy:malloc#1', reason: 'the copy' },
      { key: 'native/record.c:js_bytes_copy:memcpy#1', reason: 'the key' },
      { key: 'native/record.c:js_bytes_copy:memcpy#2', reason: 'the value' },
      { key: 'native/record.c:gone:malloc#1', reason: 'the test' },
    ],
  });
  assert.deepEqual(findings(r), ['native/record.c:gone:malloc#1: is allowed as the test, but nothing matches it any more: delete the entry']);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'native/producer.c': undefined, 'native/callback_queue.c': undefined, 'native/record.c': undefined });
  assert.equal(r.err[1], '❌ check-one-copy: cannot run, no C sources under native/');
});

test("src/ copies none of the caller's bytes; encoding a string is no copy", () => {
  const RECORD_TS = [
    '// Node\'s Buffer, as @types/node declares the two calls used here.',
    'declare class Buffer extends Uint8Array {',
    '  static from(data: string | Uint8Array, encoding?: string): Buffer;',
    '}',
    'export type Data = Buffer | Uint8Array | string;',
    'export function toBytes(value: Data | null): Uint8Array | null {',
    '  if (value === null) return null;',
    "  return typeof value === 'string' ? Buffer.from(value, 'utf8') : Buffer.from(value);",
    '}',
    'export function copies(key: Uint8Array, value: Data): Uint8Array[] {',
    '  return [new Uint8Array(key), Uint8Array.from(key), key.slice(), new Uint8Array(key.buffer, 0, 1), key.subarray(0)];',
    '}',
    'void copies;',
    '',
  ].join('\n');
  const r = runN(RULE, { 'src/producer/record.ts': RECORD_TS });
  const why = "copies bytes the caller handed over: pass the Buffer or Uint8Array itself (the addon copies it once; a string is encoded once)";
  assert.deepEqual(findings(r), [
    `src/producer/record.ts:8: Buffer.from(value) ${why}`,
    `src/producer/record.ts:11: new Uint8Array(key) ${why}`,
    `src/producer/record.ts:11: Uint8Array.from(key) ${why}`,
    `src/producer/record.ts:11: key.slice() ${why}`,
  ]);
});

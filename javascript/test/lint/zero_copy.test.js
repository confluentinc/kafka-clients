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
const CTX = '    SendCtx* ctx = calloc(1, 8);';

test('bytes read into a kafka_Bytes_t, borrowed until the hand-over, and the listed allocations only', () => {
  const r = runN(RULE);
  assert.equal(r.status, 0, r.text);
  // The C borrow, the context's allocation, the one send, its delivery
  // callback, and the 3 calls of the fixture's src/.
  assert.ok(r.out.includes('✅ check-zero-copy: 7 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  let r = runN(RULE, {
    'native/record.c': RECORD_C.replace('    out->bytes.len = (int32_t)length;', '    out->bytes.len = (int32_t)length;\n    memcpy(out->copy, data, length);'),
    'native/producer.c': PRODUCER_C.replace(CTX, `${CTX}\n    void* buf = malloc(8);`),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:31: js_send calls malloc on the send path, which no entry lists',
    'native/record.c:9: js_bytes calls memcpy on the send path: no key or value is copied',
  ]);
  r = runN(RULE, { 'native/record.c': RECORD_C.replace('    out->bytes.data = data;\n', '') });
  assert.deepEqual(findings(r), [
    'native/record.c, native/producer.c: no function reads a key or value with napi_get_typedarray_info / napi_get_buffer_info into a kafka_Bytes_t',
  ]);
});

test('the send holds a reference on the key and the value', () => {
  const r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    napi_create_reference(env, value, 1, &ctx->value_ref);\n', '') });
  assert.deepEqual(findings(r), [
    'native/producer.c:33: js_send hands the record over without a napi_create_reference into value_ref: the borrowed Buffer may be collected while the core reads it',
  ]);
});

test('the hand-over completion drops both references, itself or through a function it calls', () => {
  let r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    delete_ref(env, &ctx->key_ref);\n', '') });
  assert.deepEqual(findings(r), [
    'native/producer.c:33: on_handover does not always drop key_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
  ]);
  // A release two calls deep (through the context's final free) is not at
  // the hand-over.
  const deeper = PRODUCER_C.replace('    release_buffers(NULL, opaque);\n', '    free_ctx(opaque);\n').replace(
    'static void on_handover(',
    'static void free_ctx(void* ctx) {\n    release_buffers(NULL, ctx);\n}\n\nstatic void on_handover(',
  );
  r = runN(RULE, { 'native/producer.c': deeper });
  assert.deepEqual(findings(r), [
    'native/producer.c:38: on_handover does not always drop key_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
    'native/producer.c:38: on_handover does not always drop value_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
  ]);
});

test('the hand-over completion drops the references unconditionally, and the delivery callback never does', () => {
  // Only on a failed hand-over: an accepted record would keep its Buffers.
  let r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    release_buffers(NULL, opaque);\n', '    if (error != NULL) release_buffers(NULL, opaque);\n') });
  assert.deepEqual(findings(r), [
    'native/producer.c:34: on_handover does not always drop key_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
    'native/producer.c:34: on_handover does not always drop value_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
  ]);
  // In a block, or behind &&: conditional too.
  r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    release_buffers(NULL, opaque);\n', '    error != NULL && release_buffers(NULL, opaque);\n') });
  assert.equal(findings(r).length, 2, r.text);
  r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace('    delete_ref(env, &ctx->key_ref);\n', '    if (ctx) {\n        delete_ref(env, &ctx->key_ref);\n    }\n') });
  assert.deepEqual(findings(r), [
    'native/producer.c:36: on_handover does not always drop key_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
  ]);
  // The delivery callback releasing them holds an accepted record's Buffers until delivery.
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace('    call_ref(self_, metadata, error);\n', '    call_ref(self_, metadata, error);\n    release_buffers(NULL, self_);\n').replace(
      'static void send_on_completion(',
      'static void release_buffers(void* env, SendCtx* ctx);\n\nstatic void send_on_completion(',
    ),
  });
  assert.deepEqual(findings(r), [
    "native/producer.c:36: send_on_completion, the delivery callback, drops key_ref: an accepted record's Buffers are released at the hand-over, not held until delivery",
    "native/producer.c:36: send_on_completion, the delivery callback, drops value_ref: an accepted record's Buffers are released at the hand-over, not held until delivery",
  ]);
});

test('a memcpy may be listed; another copy never', () => {
  // A strdup cannot be listed: its entry is unused, and the call a finding.
  const r = runN(
    RULE,
    { 'native/record.c': RECORD_C.replace('    return 0;\n}', '    char* s = strdup("k");\n    return 0;\n}') },
    {
      ALLOWED: [
        { key: 'native/producer.c:js_send:calloc#1', reason: "the send's context" },
        { key: 'native/record.c:js_bytes:strdup#1', reason: 'a duplicate' },
      ],
    },
  );
  assert.deepEqual(findings(r), [
    'native/record.c:9: js_bytes calls strdup on the send path: no key or value is copied',
    'native/record.c:js_bytes:strdup#1: is allowed as a duplicate, but nothing matches it any more: delete the entry',
  ]);
});

test('an entry allows its one call site: a second calloc in the function is a finding', () => {
  const r = runN(RULE, { 'native/producer.c': PRODUCER_C.replace(CTX, `${CTX}\n    void* more = calloc(1, 8);`) });
  assert.deepEqual(findings(r), ['native/producer.c:31: js_send calls calloc on the send path, which no entry lists']);
});

test('an allowlist entry that matches nothing is a finding', () => {
  const r = runN(RULE, {}, {
    ALLOWED: [
      { key: 'native/producer.c:js_send:calloc#1', reason: "the send's context" },
      { key: 'native/record.c:gone:malloc#1', reason: 'the test' },
    ],
  });
  assert.deepEqual(findings(r), ['native/record.c:gone:malloc#1: is allowed as the test, but nothing matches it any more: delete the entry']);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'native/producer.c': undefined, 'native/callback_queue.c': undefined, 'native/record.c': undefined });
  assert.equal(r.err[1], '❌ check-zero-copy: cannot run, no C sources under native/');
});

test("src/ copies none of the caller's bytes; encoding a string is no copy", () => {
  const RECORD_TS = [
    '// Node\'s Buffer, as @types/node declares the two calls used here.',
    'declare class Buffer extends Uint8Array {',
    '  static from(data: string | Uint8Array, encoding?: string): Buffer;',
    '  static alloc(size: number): Buffer;',
    '  static concat(list: readonly Uint8Array[]): Buffer;',
    '  copy(target: Uint8Array): number;',
    '}',
    'export type Data = Buffer | Uint8Array | string;',
    'export function toBytes(value: Data | null): Uint8Array | null {',
    '  if (value === null) return null;',
    "  return typeof value === 'string' ? Buffer.from(value, 'utf8') : Buffer.from(value);",
    '}',
    'export function copies(key: Uint8Array, value: Data): Uint8Array[] {',
    '  return [new Uint8Array(key), Uint8Array.from(key), key.slice(), new Uint8Array(key.buffer, 0, 1), key.subarray(0)];',
    '}',
    'export function more(key: Buffer, keys: Buffer[]): Buffer[] {',
    '  const out = Buffer.alloc(key.length);',
    '  out.set(key);',
    '  key.copy(out);',
    "  out.set([1, 2]);",
    '  return [Buffer.concat([key]), Buffer.concat(keys), out];',
    '}',
    'void copies;',
    'void more;',
    '',
  ].join('\n');
  const r = runN(RULE, { 'src/producer/record.ts': RECORD_TS });
  const why = 'copies bytes the caller handed over: pass the Buffer or Uint8Array itself (the addon borrows it; a string is encoded once)';
  assert.deepEqual(findings(r), [
    `src/producer/record.ts:11: Buffer.from(value) ${why}`,
    `src/producer/record.ts:14: new Uint8Array(key) ${why}`,
    `src/producer/record.ts:14: Uint8Array.from(key) ${why}`,
    `src/producer/record.ts:14: key.slice() ${why}`,
    `src/producer/record.ts:18: out.set(key) ${why}`,
    `src/producer/record.ts:19: key.copy() ${why}`,
    `src/producer/record.ts:21: Buffer.concat([key]) ${why}`,
    `src/producer/record.ts:21: Buffer.concat(keys) ${why}`,
  ]);
});

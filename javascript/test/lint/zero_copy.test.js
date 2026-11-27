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

// check-zero-copy (scripts/lint/rules/zero_copy.js).

const assert = require('node:assert/strict');

const { findings } = require('./_project');
const { HEADER, LISTS, RECORD_C, PRODUCER_C } = require('./_c_fixture');
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
    'native/record.c': RECORD_C.replace(
      '    out->bytes.len = (int32_t)length;',
      '    out->bytes.len = (int32_t)length;\n    memcpy(out->copy, data, length);',
    ),
    'native/producer.c': PRODUCER_C.replace(CTX, `${CTX}\n    void* buf = malloc(8);`),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:31: js_send calls malloc on the send path, which no entry lists',
    'native/record.c:9: js_bytes calls memcpy on the send path: no key or value is copied',
  ]);
  r = runN(RULE, { 'native/record.c': RECORD_C.replace('    out->bytes.data = data;\n', '') });
  assert.deepEqual(findings(r), [
    'native/record.c, native/producer.c, native/mock_values.c: no function reads a key or value with napi_get_typedarray_info / napi_get_buffer_info into a kafka_Bytes_t',
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
  let r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    release_buffers(NULL, opaque);\n',
      '    if (error != NULL) release_buffers(NULL, opaque);\n',
    ),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:34: on_handover does not always drop key_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
    'native/producer.c:34: on_handover does not always drop value_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
  ]);
  // In a block, or behind &&: conditional too.
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    release_buffers(NULL, opaque);\n',
      '    error != NULL && release_buffers(NULL, opaque);\n',
    ),
  });
  assert.equal(findings(r).length, 2, r.text);
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    delete_ref(env, &ctx->key_ref);\n',
      '    if (ctx) {\n        delete_ref(env, &ctx->key_ref);\n    }\n',
    ),
  });
  assert.deepEqual(findings(r), [
    'native/producer.c:36: on_handover does not always drop key_ref: the FFI borrows the key and value only until the hand-over, taken or refused',
  ]);
  // The delivery callback releasing them holds an accepted record's Buffers until delivery.
  r = runN(RULE, {
    'native/producer.c': PRODUCER_C.replace(
      '    call_ref(self_, metadata, error);\n',
      '    call_ref(self_, metadata, error);\n    release_buffers(NULL, self_);\n',
    ).replace(
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
  const r = runN(
    RULE,
    {},
    {
      ALLOWED: [
        { key: 'native/producer.c:js_send:calloc#1', reason: "the send's context" },
        { key: 'native/record.c:gone:malloc#1', reason: 'the test' },
      ],
    },
  );
  assert.deepEqual(findings(r), [
    'native/record.c:gone:malloc#1: is allowed as the test, but nothing matches it any more: delete the entry',
  ]);
});

test('cannot run without the C sources', () => {
  const r = runN(RULE, { 'native/producer.c': undefined, 'native/callback_queue.c': undefined, 'native/record.c': undefined });
  assert.equal(r.err[1], '❌ check-zero-copy: cannot run, no C sources under native/');
});

test("src/ copies none of the caller's bytes; encoding a string is no copy", () => {
  const RECORD_TS = [
    "// Node's Buffer, as @types/node declares the two calls used here.",
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
    '  out.set([1, 2]);',
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

// The receive path: a header with a consumed record's key and value readers,
// and a consumer that hands each to an external Buffer over the batch's
// bytes, copying only after the runtime refused one.
const RECEIVE_HEADER = `${HEADER}
const void *kafka_consumer_ConsumerRecord_key_err(const kafka_consumer_ConsumerRecord_t *self_, kafka_common_Error_t **out_err);

const void *kafka_consumer_ConsumerRecord_value_err(const kafka_consumer_ConsumerRecord_t *self_, kafka_common_Error_t **out_err);
`;
const CONSUMER_C = `#include "util.h"

static napi_value bytes_to_js(napi_env env, Batch* b, const kafka_Bytes_t* bytes, int* copied) {
    napi_value v = NULL;
    if (!*copied) {
        napi_status status = napi_create_external_buffer(env, bytes->len, (void*)bytes->data, finalize, b, &v);
        if (status == napi_ok) return v;
        if (status != napi_no_external_buffers_allowed) return NULL;
        *copied = 1;
    }
    napi_create_buffer_copy(env, bytes->len, bytes->data, NULL, &v);
    return v;
}

static napi_value record_to_js(napi_env env, Batch* b, const kafka_consumer_ConsumerRecord_t* r, int* copied) {
    kafka_common_Error_t* ffi_err = NULL;
    const kafka_Bytes_t* key = NULL;
    const kafka_Bytes_t* value = NULL;
    kafka_common_Error_t* err = FFI_SET(key, kafka_consumer_ConsumerRecord_key_err(r, &ffi_err));
    if (err == NULL) err = FFI_SET(value, kafka_consumer_ConsumerRecord_value_err(r, &ffi_err));
    napi_value k = bytes_to_js(env, b, key, copied);
    napi_value v = bytes_to_js(env, b, value, copied);
    return k != NULL ? v : NULL;
}

static Batch* batch_new(void) {
    return calloc(1, 8);
}
`;
const RECEIVE_LISTS = {
  ALLOWED: [...LISTS['check-zero-copy'].ALLOWED, { key: 'native/consumer.c:batch_new:calloc#1', reason: "a poll's batch" }],
};
/**
 * The rule over the fixture with the receive path, `consumer` in place of its
 * native/consumer.c.
 *
 * @param {string} [consumer]
 * @param {Record<string, unknown>} [lists]
 */
function runReceive(consumer = CONSUMER_C, lists = RECEIVE_LISTS) {
  return runN(RULE, { 'confluent_kafka_bindings.h': RECEIVE_HEADER, 'native/consumer.c': consumer }, lists);
}

test('the receive path: each consumed key and value an external Buffer, a copy only after the refusal, the listed allocations only', () => {
  const r = runReceive();
  assert.equal(r.status, 0, r.text);
  // The send path's 7, the key's and the value's reads, the refused copy and the batch's allocation.
  assert.ok(r.out.includes('✅ check-zero-copy: 11 item(s) checked'), r.text);
  // Without the readers in the header, there is no receive path to check.
  const without = runN(RULE, { 'native/consumer.c': CONSUMER_C }, RECEIVE_LISTS);
  assert.deepEqual(findings(without), [
    "native/consumer.c:batch_new:calloc#1: is allowed as a poll's batch, but nothing matches it any more: delete the entry",
  ]);
});

test('a consumed key or value handed to no external Buffer, or never read, is a finding', () => {
  // The external Buffer turned off: every key and value copied.
  let r = runReceive(
    CONSUMER_C.replace(
      'napi_status status = napi_create_external_buffer(env, bytes->len, (void*)bytes->data, finalize, b, &v);',
      'napi_status status = napi_no_external_buffers_allowed;',
    ),
  );
  assert.deepEqual(findings(r), [
    "native/consumer.c:19: record_to_js reads a consumed record's key into key but hands it to no function that makes an external Buffer over its bytes: a fetched key is not copied",
    "native/consumer.c:20: record_to_js reads a consumed record's value into value but hands it to no function that makes an external Buffer over its bytes: a fetched value is not copied",
    'native/consumer.c:11: bytes_to_js calls napi_create_buffer_copy on the receive path, where no external Buffer was refused: a fetched key or value is copied only where the runtime refuses external buffers (napi_no_external_buffers_allowed)',
  ]);
  // The value built by another function, which copies it.
  r = runReceive(
    CONSUMER_C.replace('    napi_value v = bytes_to_js(env, b, value, copied);\n', '    napi_value v = copy_to_js(env, value);\n').replace(
      'static napi_value record_to_js(',
      'static napi_value copy_to_js(napi_env env, const kafka_Bytes_t* bytes) {\n    napi_value v = NULL;\n    napi_create_buffer_copy(env, bytes->len, bytes->data, NULL, &v);\n    return v;\n}\n\nstatic napi_value record_to_js(',
    ),
  );
  assert.deepEqual(findings(r), [
    "native/consumer.c:26: record_to_js reads a consumed record's value into value but hands it to no function that makes an external Buffer over its bytes: a fetched value is not copied",
    'native/consumer.c:17: copy_to_js calls napi_create_buffer_copy on the receive path, where no external Buffer was refused: a fetched key or value is copied only where the runtime refuses external buffers (napi_no_external_buffers_allowed)',
  ]);
  // No function reads the value.
  r = runReceive(
    CONSUMER_C.replace('    if (err == NULL) err = FFI_SET(value, kafka_consumer_ConsumerRecord_value_err(r, &ffi_err));\n', ''),
  );
  assert.deepEqual(findings(r), [
    "native/consumer.c: no function reads a consumed record's value (kafka_consumer_ConsumerRecord_value_err)",
  ]);
});

test("a copy before the refusal, a memcpy or an unlisted allocation of a key's bytes is a finding", () => {
  let r = runReceive(
    CONSUMER_C.replace(
      '    napi_value v = NULL;\n    if (!*copied) {',
      '    napi_value v = NULL;\n    napi_create_buffer_copy(env, bytes->len, bytes->data, NULL, &v);\n    if (!*copied) {',
    ),
  );
  assert.deepEqual(findings(r), [
    'native/consumer.c:5: bytes_to_js calls napi_create_buffer_copy on the receive path, where no external Buffer was refused: a fetched key or value is copied only where the runtime refuses external buffers (napi_no_external_buffers_allowed)',
  ]);
  r = runReceive(
    CONSUMER_C.replace(
      '    napi_value v = NULL;\n    if (!*copied) {',
      '    napi_value v = NULL;\n    void* dup = malloc(bytes->len);\n    memcpy(dup, bytes->data, bytes->len);\n    if (!*copied) {',
    ),
  );
  assert.deepEqual(findings(r), [
    'native/consumer.c:5: bytes_to_js calls malloc on the receive path, which no entry lists',
    'native/consumer.c:6: bytes_to_js calls memcpy on the receive path: no fetched key or value is copied',
  ]);
  // A reader listed by its call site (a mock's record, whose key is no bytes) is allowed.
  r = runReceive(CONSUMER_C.replace('    napi_value k = bytes_to_js(env, b, key, copied);\n', '    napi_value k = NULL;\n'), {
    ALLOWED: [
      ...RECEIVE_LISTS.ALLOWED,
      { key: 'native/consumer.c:record_to_js:kafka_consumer_ConsumerRecord_key_err#1', reason: "a mock's key" },
    ],
  });
  assert.equal(r.status, 0, r.text);
});

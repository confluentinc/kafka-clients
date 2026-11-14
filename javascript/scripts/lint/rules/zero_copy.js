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

// N7 check-zero-copy (javascript/CLAUDE.md boundary rule 8: no copy of the
// app's key and value; the addon borrows each Buffer until the hand-over). A
// heuristic (PLAN.md §8 decision 13), on the send path (native/record.c,
// native/producer.c):
//   - the key and value cross from JavaScript through
//     `napi_get_typedarray_info` / `napi_get_buffer_info` into a
//     `kafka_Bytes_t` (a function that reads one of them sets a `.data`
//     member);
//   - no copy (`memcpy`, `memmove`, `strdup`, `napi_create_buffer_copy`) and
//     no allocation (`malloc`, `calloc`, `realloc`) is made there, but those
//     listed with what they hold, none of which is a key or a value (a C
//     string of a header key, a context). An entry that matches nothing is a
//     finding;
//   - the function that hands the record over (the call of a header
//     function `..._send_cb` / `..._send_with_callback_cb`) takes a
//     reference (`napi_create_reference`) into the context's `key_ref` and
//     `value_ref`, which keep the borrowed Buffers alive, and the hand-over
//     completion it passes (its second-to-last argument) drops both
//     (`delete_ref` / `napi_delete_reference` of `key_ref` and `value_ref`),
//     in its own body or in a function of native/ it calls directly (a
//     release reached only through the context's final free would not be at
//     the hand-over): the FFI borrows the bytes only until that completion;
//   - in src/ (every hand-written module: the conversion of a record to the
//     native arguments, and whatever else hands key, value or header bytes
//     to the addon), no copy of a value typed as bytes (a `Uint8Array`, a
//     `Buffer`, or a union holding one, such as `Data`): no `Buffer.from(x)`,
//     `Buffer.copyBytesFrom(x)`, `new Uint8Array(x)`, `Uint8Array.from(x)` or
//     `x.slice()`. Encoding a `string` (src/internal/encode.ts) is no copy of
//     the caller's bytes.

const { cModel, applyAllowlist, siteKey } = require('../c_model');

const SEND_PATH = ['native/record.c', 'native/producer.c'];

/** The mark of a file `cargo xtask generate-error-codes` writes. */
const GENERATED = /GENERATED, DO NOT EDIT/;
const BYTES = new Set(['Uint8Array', 'Buffer', 'Uint8ClampedArray']);

/**
 * The copies of a bytes-typed value in src/'s hand-written modules:
 * `{findings, checked}`, one finding per copy, `checked` the calls read.
 */
function jsCopies(ctx) {
  const { ts, checker, sourceFiles } = ctx.program();
  const findings = [];
  let checked = 0;
  // Whether `node`'s type is bytes, or a union with a bytes member.
  const isBytes = (node) => {
    const type = checker.getNonNullableType(checker.getTypeAtLocation(node));
    return (type.isUnion() ? type.types : [type]).some((t) => t.symbol && BYTES.has(t.symbol.name));
  };
  const named = (e, object, method) =>
    ts.isPropertyAccessExpression(e) && ts.isIdentifier(e.expression) && e.expression.text === object && e.name.text === method;
  for (const sf of sourceFiles) {
    const rel = ctx.rel(sf.fileName);
    if (!rel.startsWith('src/') || GENERATED.test(sf.text.slice(0, 2000))) continue;
    const visit = (n) => {
      let copied = null;
      if (ts.isCallExpression(n)) {
        checked += 1;
        const e = n.expression;
        const first = n.arguments[0];
        if ((named(e, 'Buffer', 'from') || named(e, 'Buffer', 'copyBytesFrom') || named(e, 'Uint8Array', 'from')) && first && isBytes(first)) {
          copied = `${e.getText(sf)}(${first.getText(sf)})`;
        } else if (ts.isPropertyAccessExpression(e) && e.name.text === 'slice' && isBytes(e.expression)) {
          copied = `${e.getText(sf)}()`;
        }
      } else if (ts.isNewExpression(n) && ts.isIdentifier(n.expression) && n.expression.text === 'Uint8Array') {
        checked += 1;
        const first = (n.arguments || [])[0];
        if (first && isBytes(first)) copied = `new Uint8Array(${first.getText(sf)})`;
      }
      if (copied !== null) {
        findings.push({
          kind: 'copy',
          subject: `${rel}:${sf.getLineAndCharacterOfPosition(n.getStart(sf)).line + 1}`,
          message: `${copied} copies bytes the caller handed over: pass the Buffer or Uint8Array itself (the addon borrows it; a string is encoded once)`,
        });
      }
      ts.forEachChild(n, visit);
    };
    visit(sf);
  }
  return { findings, checked };
}
const COPIES = new Set(['memcpy', 'memmove', 'strdup', 'strndup', 'napi_create_buffer_copy']);
const ALLOCATIONS = new Set(['malloc', 'calloc', 'realloc']);
const BORROWS = new Set(['napi_get_typedarray_info', 'napi_get_buffer_info']);
/** A header function that hands a record over, with its completion as its second-to-last argument. */
const SEND = /_send(_with_callback)?_cb$/;
const RELEASES = new Set(['delete_ref', 'napi_delete_reference']);
const BORROWED = ['key_ref', 'value_ref'];

/** The copies and allocations allowed on the send path, by call site (`<file>:<function>:<callee>#<n>`). */
const ALLOWED = [
  { key: 'native/record.c:js_string_dup:malloc#1', reason: 'the C string of a topic, which the C API reads as one' },
  {
    key: 'native/record.c:js_bytes_cstring:malloc#1',
    reason: "the C string of a header key, or of a configuration key or value, from encodeLikeJava's bytes",
  },
  {
    key: 'native/record.c:js_bytes_cstring:memcpy#1',
    reason: "a header key's, or a configuration key's or value's, bytes into their C string (no key or value of a record)",
  },
  { key: 'native/producer.c:string_or_null:malloc#1', reason: 'the message a user callback threw or rejected with' },
  { key: 'native/producer.c:js_producer_send:calloc#1', reason: "the send's context" },
  { key: 'native/producer.c:js_producer_flush:calloc#1', reason: "the flush's context" },
  { key: 'native/producer.c:js_producer_close:calloc#1', reason: "the close's context" },
  { key: 'native/producer.c:new_kafka_producer:calloc#1', reason: "the configuration's string array" },
  { key: 'native/producer.c:js_producer_create:calloc#1', reason: 'the producer struct' },
];

/** The text of each argument naming `field` (`&ctx->key_ref`) among the calls of `names` in `fns`. */
function touches(calls, fns, names, field) {
  const re = new RegExp(`\\b${field}\\b`);
  return calls.some((c) => fns.has(`${c.file}:${c.function}`) && names.has(c.name) && (c.args || []).some((a) => re.test(a)));
}

/** The functions of native/ `start` reaches, itself included, up to `depth` calls deep (`file:name` keys). */
function reached(model, start, depth) {
  const out = new Set();
  let frontier = start ? [start] : [];
  for (let d = 0; d <= depth && frontier.length > 0; d++) {
    const next = [];
    for (const b of frontier) {
      const key = `${b.file}:${b.name}`;
      if (out.has(key)) continue;
      out.add(key);
      for (const c of model.calls) {
        if (c.file !== b.file || c.function !== b.name) continue;
        const callee = model.resolve(c.name, c.file);
        if (callee) next.push(callee);
      }
    }
    frontier = next;
  }
  return out;
}

module.exports = {
  name: 'check-zero-copy',
  kinds: ['bytes-not-borrowed', 'copy', 'allocation', 'not-held', 'not-released', 'copy-unused-allowance'],
  needs: ['program', 'native', 'header'],
  hint:
    "Borrow the app's key and value, never copy them: read each as a kafka_Bytes_t over its own memory, hold " +
    'a napi_ref on its Buffer and drop it in the hand-over completion (javascript/CLAUDE.md boundary rule 8).',
  ALLOWED,
  check(ctx) {
    const model = cModel(ctx);
    const { bodies, calls } = model;
    const raw = [];
    let checked = 0;
    const onPath = calls.filter((c) => SEND_PATH.includes(c.file) && !c.directive);
    const borrows = onPath.filter((c) => BORROWS.has(c.name));
    const into = borrows.filter((c) => c.function && /(\.|->)data\s*=/.test(bodies.get(`${c.file}:${c.function}`).body));
    checked += 1;
    if (into.length === 0) {
      raw.push({
        kind: 'bytes-not-borrowed',
        subject: SEND_PATH.join(', '),
        message: 'no function reads a key or value with napi_get_typedarray_info / napi_get_buffer_info into a kafka_Bytes_t',
      });
    }
    for (const c of onPath) {
      if (!COPIES.has(c.name) && !ALLOCATIONS.has(c.name)) continue;
      checked += 1;
      const copy = COPIES.has(c.name);
      raw.push({
        kind: copy ? 'copy' : 'allocation',
        subject: `${c.file}:${c.line}`,
        message: `${c.function} calls ${c.name} on the send path${copy ? ': no key or value is copied' : ', which no entry lists'}`,
        // A memcpy may be listed (a header key's C string); another copy never.
        allow: copy && c.name !== 'memcpy' ? undefined : siteKey(c),
      });
    }
    for (const c of onPath) {
      if (!SEND.test(c.name) || !c.function) continue;
      checked += 1;
      const sender = new Set([`${c.file}:${c.function}`]);
      for (const field of BORROWED) {
        if (!touches(calls, sender, new Set(['napi_create_reference']), field)) {
          raw.push({
            kind: 'not-held',
            subject: `${c.file}:${c.line}`,
            message: `${c.function} hands the record over without a napi_create_reference into ${field}: the borrowed Buffer may be collected while the core reads it`,
          });
        }
      }
      const args = c.args || [];
      const completion = args.length >= 2 ? args[args.length - 2].trim() : '';
      const fns = reached(model, model.resolve(completion, c.file), 1);
      for (const field of BORROWED) {
        if (!touches(calls, fns, RELEASES, field)) {
          raw.push({
            kind: 'not-released',
            subject: `${c.file}:${c.line}`,
            message: `${completion || 'the hand-over completion'} does not drop ${field}: the FFI borrows the key and value only until the hand-over`,
          });
        }
      }
    }
    const js = jsCopies(ctx);
    checked += js.checked;
    raw.push(...js.findings);
    const { kept, unused } = applyAllowlist(raw, this.ALLOWED, 'copy-unused-allowance');
    return { findings: [...kept, ...unused], checked };
  },
};

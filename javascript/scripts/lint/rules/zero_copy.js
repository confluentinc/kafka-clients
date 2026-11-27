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

// check-zero-copy (javascript/CLAUDE.md boundary rule 8: no copy of the
// app's key and value; the addon borrows each Buffer until the hand-over). A
// heuristic (PLAN.md §8 decision 13), on the send path (native/record.c,
// native/producer.c, and native/mock_values.c, where a mock's send keeps its
// record):
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
//     the hand-over), unconditionally (at the top level of the completion's
//     body, and of the function it calls, not under an `if`, a loop, `&&`,
//     `||` or `?`), whether the record was taken or refused: the FFI borrows
//     the bytes only until that completion;
//   - the delivery callback (the `_fn_t` handed to a `..._Callback_new`) does
//     not drop `key_ref` or `value_ref`, in its own body or a function it
//     calls directly: an accepted record's Buffers are released at the
//     hand-over, never held until delivery;
//   - in src/ (every hand-written module: the conversion of a record to the
//     native arguments, and whatever else hands key, value or header bytes
//     to the addon), no copy of a value typed as bytes (a `Uint8Array`, a
//     `Buffer`, or a union holding one, such as `Data`): no `Buffer.from(x)`,
//     `Buffer.copyBytesFrom(x)`, `new Uint8Array(x)`, `Uint8Array.from(x)`,
//     `x.slice()`, `Buffer.concat([..., x, ...])` (or of an array of bytes),
//     `x.copy(...)`, or `target.set(x)` (so `Buffer.alloc(n)` filled with
//     `.set` or `.copy` is caught at the fill). Encoding a `string`
//     (src/internal/encode.ts) is no copy of the caller's bytes.
//
// And on the receive path (native/consumer.c; the JS conventions,
// Serialization: a consumed key or value is a Buffer over the record's own
// bytes, a copy only where the runtime refuses external buffers), once the
// bindings header declares a consumed record's key and value readers:
//   - each of a record's key and value, read with
//     `kafka_consumer_ConsumerRecord_key_err` / `_value_err` into a variable,
//     is handed (in the reading function) to a function of native/ that makes
//     an external Buffer over its bytes (`napi_create_external_buffer` or
//     `napi_create_external_arraybuffer` of a `->data` / `.data`), or the
//     reading function makes it itself (`not-external`);
//   - a `napi_create_buffer_copy` there is made only after the refusal:
//     in a function that makes an external Buffer, after a test of
//     `napi_no_external_buffers_allowed` (`copy`), which the addon's test
//     switch stands for too;
//   - no other copy (`memcpy`, `memmove`, `strdup`) is made there, and no
//     allocation but those listed with what they hold (a call's context, the
//     consumer struct, the batch's count), none of which is a key's or a
//     value's bytes. Header values stay copies, by design (native/headers.c).

const { cModel, applyAllowlist, siteKey } = require('../c_model');
const { scan } = require('../c_source');

const SEND_PATH = ['native/record.c', 'native/producer.c', 'native/mock_values.c'];
/** The receive path: the file that turns a consumed record's key and value into Buffers. */
const RECEIVE_PATH = ['native/consumer.c'];
/** A consumed record's key and value readers of the bindings header, by part. */
const RECORD_BYTES = { key: 'kafka_consumer_ConsumerRecord_key_err', value: 'kafka_consumer_ConsumerRecord_value_err' };
const EXTERNAL = new Set(['napi_create_external_buffer', 'napi_create_external_arraybuffer']);
/** Node-API's answer where external buffers are refused (Electron). */
const REFUSED = 'napi_no_external_buffers_allowed';

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
        if (
          (named(e, 'Buffer', 'from') || named(e, 'Buffer', 'copyBytesFrom') || named(e, 'Uint8Array', 'from')) &&
          first &&
          isBytes(first)
        ) {
          copied = `${e.getText(sf)}(${first.getText(sf)})`;
        } else if (ts.isPropertyAccessExpression(e) && ['slice', 'copy'].includes(e.name.text) && isBytes(e.expression)) {
          copied = `${e.getText(sf)}()`;
        } else if (ts.isPropertyAccessExpression(e) && e.name.text === 'set' && isBytes(e.expression) && first && isBytes(first)) {
          copied = `${e.getText(sf)}(${first.getText(sf)})`;
        } else if (named(e, 'Buffer', 'concat') && first) {
          const items = ts.isArrayLiteralExpression(first) ? first.elements : [];
          const elementType = checker.getIndexTypeOfType(checker.getTypeAtLocation(first), ts.IndexKind.Number);
          const ofBytes = (t) => t !== undefined && (t.isUnion() ? t.types : [t]).some((u) => u.symbol && BYTES.has(u.symbol.name));
          if (items.some((x) => isBytes(x)) || ofBytes(elementType)) copied = `${e.getText(sf)}(${first.getText(sf)})`;
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
  { key: 'native/producer.c:js_producer_partitions_for:calloc#1', reason: "one partitionsFor call's context, not a record's" },
  {
    key: 'native/producer.c:slot_take:realloc#1',
    reason: "the producer's table of record futures, grown by doubling: a context pointer per slot",
  },
  { key: 'native/producer.c:slot_take:realloc#2', reason: 'the free list of that table, grown with it' },
  { key: 'native/producer.c:js_record_future_get:calloc#1', reason: "one wait's context on a record's future" },
  { key: 'native/producer.c:js_producer_close:calloc#1', reason: "the close's context" },
  { key: 'native/producer.c:producer_handle_new:calloc#1', reason: 'the producer struct' },
  { key: 'native/producer.c:js_mock_producer_send:calloc#1', reason: "a mock's send's context" },
  {
    key: 'native/mock_values.c:mock_record_new:calloc#1',
    reason: "the record a mock keeps: a reference to the app's record, no copy of its key or value",
  },
  {
    key: 'native/mock_values.c:injected_error_carry:calloc#1',
    reason: "an error JS gives a mock, or a callback's thrown value: a reference to it, no record",
  },
  {
    key: 'native/consumer.c:mock_record_to_js:kafka_consumer_ConsumerRecord_key_err#1',
    reason: "a mock's record, whose key is the address of the MockRecord holding the app's own record, not fetched bytes",
  },
  { key: 'native/consumer.c:deferred_new:calloc#1', reason: "a callback's pending report, waiting for its Promise" },
  { key: 'native/consumer.c:listener_new:calloc#1', reason: "a rebalance listener's registration" },
  { key: 'native/consumer.c:commit_callback_new:calloc#1', reason: "a commit callback's registration" },
  { key: 'native/consumer.c:batch_to_js:calloc#1', reason: "a poll's batch, which counts what still needs the records" },
  { key: 'native/consumer.c:batch_to_js:calloc#2', reason: "the batch's header sources, one per record: a pointer and a count, no bytes" },
  { key: 'native/consumer.c:js_consumer_poll:calloc#1', reason: "a poll's context" },
  { key: 'native/consumer.c:call_ctx_new:calloc#1', reason: "a waiting call's context" },
  { key: 'native/consumer.c:string_list_from_js:calloc#1', reason: "the array of a subscription's topic strings, no record's bytes" },
  { key: 'native/consumer.c:subscribe_ctx_new:calloc#1', reason: "a subscribe's context" },
  { key: 'native/consumer.c:js_consumer_commit_async:calloc#1', reason: "a commit's context, holding its callback" },
  { key: 'native/consumer.c:js_consumer_close:calloc#1', reason: "the close's context" },
  { key: 'native/consumer.c:consumer_handle_new:calloc#1', reason: 'the consumer struct' },
];

/** The text of each argument naming `field` (`&ctx->key_ref`) among the calls of `names` in `fns`. */
function touches(calls, fns, names, field) {
  const re = new RegExp(`\\b${field}\\b`);
  return calls.some((c) => fns.has(`${c.file}:${c.function}`) && names.has(c.name) && (c.args || []).some((a) => re.test(a)));
}

/**
 * Whether call `c` runs whenever its function does: at the top level of the
 * body (no enclosing block), in a statement that does not start with `if`,
 * `else`, a loop or `switch`, and holds no `&&`, `||` or `?` before it.
 */
function unconditional(model, c) {
  const body = model.bodies.get(`${c.file}:${c.function}`);
  if (!body) return false;
  const before = scan(body.body).code.slice(0, c.start - body.bodyStart);
  let depth = 0;
  for (const ch of before) {
    if (ch === '{') depth++;
    else if (ch === '}') depth--;
  }
  if (depth !== 0) return false;
  const statement = before.slice(Math.max(before.lastIndexOf(';'), before.lastIndexOf('{'), before.lastIndexOf('}')) + 1).trim();
  return !/^(if|else|for|while|do|switch|case)\b/.test(statement) && !/&&|\|\||\?/.test(statement);
}

/**
 * Whether function `start` drops `field` unconditionally: a release call of
 * it at its top level, or a top-level call of a function of native/ that
 * does so in its own body.
 */
function dropsAlways(model, start, field) {
  if (!start) return false;
  const re = new RegExp(`\\b${field}\\b`);
  const own = (b) => model.calls.filter((c) => c.file === b.file && c.function === b.name && unconditional(model, c));
  for (const c of own(start)) {
    if (RELEASES.has(c.name) && (c.args || []).some((a) => re.test(a))) return true;
    const callee = model.resolve(c.name, c.file);
    if (callee && own(callee).some((d) => RELEASES.has(d.name) && (d.args || []).some((a) => re.test(a)))) return true;
  }
  return false;
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

/**
 * The variable call `c` (a record's key or value reader) is read into: the
 * first argument of the `FFI_SET` around it, or the target of an assignment
 * the statement makes. `null` when it is read into neither.
 */
function readInto(model, c) {
  const around = model.calls.find((d) => d.name === 'FFI_SET' && d.file === c.file && d.start < c.start && d.end > c.end);
  if (around && around.args && around.args[0]) return around.args[0].trim();
  const body = model.bodies.get(`${c.file}:${c.function}`);
  if (!body) return null;
  const before = scan(body.body).code.slice(0, c.start - body.bodyStart);
  const statement = before.slice(Math.max(before.lastIndexOf(';'), before.lastIndexOf('{'), before.lastIndexOf('}')) + 1);
  const assigned = /(\w+)\s*=[^=]*$/.exec(statement);
  return assigned ? assigned[1] : null;
}

/** Whether the function `b` makes an external Buffer over a `->data` / `.data`. */
function makesExternal(model, b) {
  return model.calls.some(
    (c) => c.file === b.file && c.function === b.name && EXTERNAL.has(c.name) && (c.args || []).some((a) => /(\.|->)data\b/.test(a)),
  );
}

/** The receive path's findings and the items checked: `{raw, checked}`. */
function receivePath(model) {
  const raw = [];
  let checked = 0;
  if (!model.header.has(RECORD_BYTES.key) || !model.header.has(RECORD_BYTES.value)) return { raw, checked };
  const onPath = model.calls.filter((c) => RECEIVE_PATH.includes(c.file) && !c.directive);
  for (const [part, reader] of Object.entries(RECORD_BYTES)) {
    checked += 1;
    const reads = onPath.filter((c) => c.name === reader && c.function);
    if (reads.length === 0) {
      raw.push({
        kind: 'not-external',
        subject: RECEIVE_PATH.join(', '),
        message: `no function reads a consumed record's ${part} (${reader})`,
      });
      continue;
    }
    for (const c of reads) {
      const variable = readInto(model, c);
      const re = variable ? new RegExp(`\\b${variable}\\b`) : null;
      const own = model.bodies.get(`${c.file}:${c.function}`);
      const handed =
        re !== null &&
        onPath.some((d) => {
          if (d.file !== c.file || d.function !== c.function || d.name === reader || d.name === 'FFI_SET') return false;
          if (!(d.args || []).some((a) => re.test(a))) return false;
          if (EXTERNAL.has(d.name)) return true;
          const callee = model.resolve(d.name, d.file);
          return callee !== undefined && makesExternal(model, callee);
        });
      if (!handed && !(own && makesExternal(model, own) && re !== null)) {
        raw.push({
          kind: 'not-external',
          subject: `${c.file}:${c.line}`,
          message: `${c.function} reads a consumed record's ${part}${variable ? ` into ${variable}` : ''} but hands it to no function that makes an external Buffer over its bytes: a fetched ${part} is not copied`,
          allow: siteKey(c),
        });
      }
    }
  }
  for (const c of onPath) {
    if (!COPIES.has(c.name) && !ALLOCATIONS.has(c.name)) continue;
    checked += 1;
    if (c.name === 'napi_create_buffer_copy') {
      const b = model.bodies.get(`${c.file}:${c.function}`);
      const refusedAt = b ? scan(b.body).code.indexOf(REFUSED) : -1;
      if (b && makesExternal(model, b) && refusedAt >= 0 && refusedAt < c.start - b.bodyStart) continue;
      raw.push({
        kind: 'copy',
        subject: `${c.file}:${c.line}`,
        message: `${c.function} calls napi_create_buffer_copy on the receive path, where no external Buffer was refused: a fetched key or value is copied only where the runtime refuses external buffers (${REFUSED})`,
      });
      continue;
    }
    const copy = COPIES.has(c.name);
    raw.push({
      kind: copy ? 'copy' : 'allocation',
      subject: `${c.file}:${c.line}`,
      message: `${c.function} calls ${c.name} on the receive path${copy ? ': no fetched key or value is copied' : ', which no entry lists'}`,
      allow: copy && c.name !== 'memcpy' ? undefined : siteKey(c),
    });
  }
  return { raw, checked };
}

module.exports = {
  name: 'check-zero-copy',
  kinds: [
    'bytes-not-borrowed',
    'copy',
    'allocation',
    'not-held',
    'not-released',
    'released-at-delivery',
    'not-external',
    'copy-unused-allowance',
  ],
  needs: ['program', 'native', 'header'],
  hint:
    "Borrow the app's key and value, never copy them: read each as a kafka_Bytes_t over its own memory, hold " +
    'a napi_ref on its Buffer and drop it in the hand-over completion (javascript/CLAUDE.md boundary rule 8); ' +
    "hand a consumed key or value to JavaScript as an external Buffer over the batch's bytes, a copy only where " +
    'external buffers are refused (JS conventions, Serialization).',
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
      const handover = model.resolve(completion, c.file);
      for (const field of BORROWED) {
        if (!dropsAlways(model, handover, field)) {
          raw.push({
            kind: 'not-released',
            subject: `${c.file}:${c.line}`,
            message: `${completion || 'the hand-over completion'} does not always drop ${field}: the FFI borrows the key and value only until the hand-over, taken or refused`,
          });
        }
      }
    }
    // The delivery callback: the `_fn_t` argument of a `..._Callback_new`.
    for (const c of onPath) {
      if (!/_Callback_new(_err)?$/.test(c.name) || !model.header.has(c.name)) continue;
      const params = model.header.get(c.name).params;
      params.forEach((param, i) => {
        if (!/_fn_t$/.test(param.type) || !c.args[i]) return;
        const delivery = model.resolve(c.args[i].trim(), c.file);
        if (!delivery) return;
        checked += 1;
        const fns = reached(model, delivery, 1);
        for (const field of BORROWED) {
          if (touches(calls, fns, RELEASES, field)) {
            raw.push({
              kind: 'released-at-delivery',
              subject: `${c.file}:${c.line}`,
              message: `${delivery.name}, the delivery callback, drops ${field}: an accepted record's Buffers are released at the hand-over, not held until delivery`,
            });
          }
        }
      });
    }
    const receive = receivePath(model);
    checked += receive.checked;
    raw.push(...receive.raw);
    const js = jsCopies(ctx);
    checked += js.checked;
    raw.push(...js.findings);
    const { kept, unused } = applyAllowlist(raw, this.ALLOWED, 'copy-unused-allowance');
    return { findings: [...kept, ...unused], checked };
  },
};

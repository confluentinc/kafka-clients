// Copyright 2025 Confluent Inc.
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

// Every kafka_* function the addon calls is declared in the bindings header,
// and none is a plain name that has an `_err` twin (javascript/CLAUDE.md;
// Python's test_ffi_twins.py, .NET's NativeAbiConformanceTests).

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { headerPath, stripComments, declaredFunctions, errorEnumerators } = require('../../scripts/header');
const { calledFunctions: calledIn } = require('../../scripts/lint/c_source');

const NATIVE = path.join(__dirname, '..', '..', 'native');
const header = fs.readFileSync(headerPath(), 'utf8');
const declared = declaredFunctions(header);

/** The kafka_* functions a C source file calls (an identifier followed by `(`). */
function calledFunctions(file) {
  return calledIn(fs.readFileSync(file, 'utf8'));
}

// The .h files too: a macro's body makes its calls wherever it is used.
const sources = fs.readdirSync(NATIVE).filter((f) => f.endsWith('.c') || f.endsWith('.h'));

test('the addon has C sources to check', () => {
  assert.ok(sources.length >= 4, sources.join(', '));
});

for (const source of sources) {
  test(`${source} calls only functions the bindings header declares`, () => {
    for (const name of calledFunctions(path.join(NATIVE, source))) {
      assert.ok(declared.has(name), `${name} is not declared in confluent_kafka_bindings.h`);
      assert.ok(!declared.has(`${name}_err`), `${name} has an _err twin, which must be called instead`);
    }
  });
}

test('the producer uses the functions of the plan', () => {
  const called = new Set(sources.flatMap((s) => [...calledFunctions(path.join(NATIVE, s))]));
  for (const name of [
    'kafka_producer_Producer_send_with_callback_cb',
    'kafka_producer_Producer_flush_cb',
    'kafka_producer_Producer_close_cb',
    'kafka_producer_Producer_close_with_timeout_cb',
    'kafka_producer_Producer__execute_callbacks_err',
    'kafka_producer_Producer__set_callbacks_notify_err',
    'kafka_producer_Producer__set_callback_result_err',
    'kafka_producer_KafkaProducer_destroy_err',
    'kafka_common_KafkaFuture_destroy_err',
  ]) {
    assert.ok(called.has(name), `${name} is not called`);
  }
  // Rules 4 and 5 (no blocking twin, never read the future) are the rule
  // linter's check-cb-twin-only (make lint-rules), which forbids the blocking
  // form of every function with a `_cb` twin, and every
  // kafka_common_KafkaFuture_get* call, the `_cb` forms included.
});

test('no blocking close: every close is a _cb twin, and no finalizer closes a producer (rule 4)', () => {
  for (const name of ['kafka_producer_Producer_close', 'kafka_producer_Producer_close_with_timeout']) {
    for (const source of sources) {
      const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
      const calls = [...code.matchAll(new RegExp(`\\b${name}(_err)?\\s*\\(`, 'g'))];
      assert.equal(calls.length, 0, `${source} calls ${name}, the blocking form`);
    }
  }
});

test("the generated FFI_NAMES table is the header's kafka_common_Error_e, both ways", () => {
  // src/internal/error_registry.ts is generated from rust/src/ffi/common.rs;
  // the addon is built against the header cbindgen generates from it. Read from the
  // source (no build): the `[value, 'NAME'],` rows of its FFI_NAMES literal.
  const registry = fs.readFileSync(path.join(__dirname, '..', '..', 'src', 'internal', 'error_registry.ts'), 'utf8');
  const start = registry.indexOf('export const FFI_NAMES');
  assert.ok(start >= 0, 'src/internal/error_registry.ts has no FFI_NAMES');
  const body = registry.slice(start, registry.indexOf(']);', start));
  const FFI_NAMES = [...body.matchAll(/^ {2}\[(-?\d+), '([A-Z0-9_]+)'\],$/gm)].map((m) => [Number(m[1]), m[2]]);
  assert.ok(FFI_NAMES.length > 0, 'FFI_NAMES has no rows');
  const values = new Map(errorEnumerators(header));
  assert.deepEqual([...FFI_NAMES].sort((a, b) => a[0] - b[0]), [...values].map(([n, v]) => [v, n]).sort((a, b) => a[0] - b[0]));
  assert.equal(values.get('LOCAL_ILLEGAL_ARGUMENT'), -3);
  assert.equal(values.get('LOCAL_ILLEGAL_STATE'), -4);
});

// The ABI check, as .NET's NativeAbiConformanceTests checks its P/Invokes
// against the same header: the addon's calls against the header's
// prototypes. The addon includes the header itself (native/util.h), so the C
// compiler checks each argument's and the return's type, and native/binding.gyp
// makes every warning (an incompatible pointer, a wrong integer width) an
// error; this test checks what the compiler cannot see once the header
// changes under a stale build or a macro, and that the build keeps -Werror.

/**
 * The text between the parenthesis at `open` (its index in `code`) and its
 * match, and the index after the match.
 */
function balanced(code, open) {
  let depth = 0;
  for (let i = open; i < code.length; i++) {
    const c = code[i];
    if (c === '(') depth += 1;
    else if (c === ')') {
      depth -= 1;
      if (depth === 0) return { inner: code.slice(open + 1, i), end: i + 1 };
    } else if (c === '"' || c === "'") {
      // A string or character literal: skip it, escapes included.
      for (i += 1; i < code.length && code[i] !== c; i++) if (code[i] === '\\') i += 1;
    }
  }
  throw new Error(`unbalanced parenthesis at ${open}`);
}

/** The top-level comma-separated parts of an argument or parameter list. */
function topLevelParts(list) {
  const parts = [];
  let depth = 0;
  let start = 0;
  for (let i = 0; i < list.length; i++) {
    const c = list[i];
    if (c === '(' || c === '[' || c === '{') depth += 1;
    else if (c === ')' || c === ']' || c === '}') depth -= 1;
    else if (c === '"' || c === "'") {
      for (i += 1; i < list.length && list[i] !== c; i++) if (list[i] === '\\') i += 1;
    } else if (c === ',' && depth === 0) {
      parts.push(list.slice(start, i).trim());
      start = i + 1;
    }
  }
  const last = list.slice(start).trim();
  if (last !== '' || parts.length > 0) parts.push(last);
  return parts;
}

/** {name: [parameter, ...]} of every function prototype in the bindings header. */
function prototypes(text) {
  const code = stripComments(text);
  const out = new Map();
  const re = /^[A-Za-z_][\w \t*]*?\b(kafka_\w+)\s*\(/gm;
  let m;
  while ((m = re.exec(code)) !== null) {
    const { inner, end } = balanced(code, m.index + m[0].length - 1);
    // A prototype ends with `;`; a typedef of a function pointer is not one.
    if (!/^\s*;/.test(code.slice(end))) continue;
    const params = topLevelParts(inner);
    out.set(m[1], params.length === 1 && params[0] === 'void' ? [] : params);
  }
  return out;
}

/** [{name, args, at}] of every call of a kafka_* function in C source text. */
function calls(text) {
  const code = stripComments(text);
  const out = [];
  const re = /\b(kafka_\w+)\s*\(/g;
  let m;
  while ((m = re.exec(code)) !== null) {
    const { inner } = balanced(code, m.index + m[0].length - 1);
    out.push({ name: m[1], args: topLevelParts(inner), at: code.slice(0, m.index).split('\n').length });
  }
  return out;
}

test('every call of a kafka_* function passes the arguments of its header prototype', () => {
  const protos = prototypes(header);
  assert.ok(protos.size > 100, `the header has ${protos.size} prototypes`);
  let checked = 0;
  let withOutErr = 0;
  for (const source of sources) {
    for (const call of calls(fs.readFileSync(path.join(NATIVE, source), 'utf8'))) {
      const params = protos.get(call.name);
      assert.ok(params !== undefined, `${source}:${call.at}: ${call.name} has no prototype in confluent_kafka_bindings.h`);
      assert.equal(call.args.length, params.length, `${source}:${call.at}: ${call.name} takes ${params.length} arguments (${params.join(', ')}), not ${call.args.length}`);
      // A fallible function's trailing out_err is the call's error slot.
      if (/\*\s*\*\s*out_err$/.test(params[params.length - 1] ?? '')) {
        assert.match(call.args[call.args.length - 1], /^&?\w*err\w*$/, `${source}:${call.at}: ${call.name}'s out_err is ${call.args[call.args.length - 1]}`);
        withOutErr += 1;
      }
      checked += 1;
    }
  }
  assert.ok(checked > 40 && withOutErr > 20, `checked ${checked} calls, ${withOutErr} with an out_err`);
});

test('the addon declares no prototype of its own for a kafka_* function, and builds with -Werror', () => {
  for (const source of sources) {
    const code = stripComments(fs.readFileSync(path.join(NATIVE, source), 'utf8'));
    // A declaration is at file scope, at column 0; a statement is indented.
    assert.doesNotMatch(code, /^(?!(?:else|return|if|while|for|do|case)\b)(?:extern\s+)?[A-Za-z_][\w \t*]*\bkafka_\w+\s*\([^;{]*\)\s*;/m, `${source} redeclares a function of the bindings header`);
  }
  const gyp = fs.readFileSync(path.join(NATIVE, 'binding.gyp'), 'utf8');
  for (const flag of ['"-Wall"', '"-Wextra"', '"-Werror"']) {
    assert.ok(gyp.split(flag).length - 1 >= 2, `native/binding.gyp does not pass ${flag} on both platforms`);
  }
});

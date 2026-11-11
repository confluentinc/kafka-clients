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

// H2 check-exception-word (scripts/lint/rules/exception_word.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { project, lint, findings } = require('./_project');

const RULE = 'check-exception-word';

const KEPT =
  'export class F { isCompletedExceptionally(): boolean { return false; } }\n' +
  'export type C = (options: { exception: unknown }) => void;\n' +
  'export class M { sendException = 0; flushException = 0; closeException = 0; }\n';

test('identifiers without the word pass; comments and strings may name Java classes', () => {
  const root = project({
    'src/a.ts': `// IllegalStateException is Java's\n/** Throws an Exception. */\nexport const msg = 'KafkaException';\nexport const t = \`\${msg} Exception\`;\n${KEPT}`,
    'native/a.c': '// an Exception\nconst char* s = "Exception";\nint f(void) { return napi_is_exception_pending(0) + node_api_exception_x(); }\n',
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 0, r.text);
  // src: msg, t, msg, F, isCompletedExceptionally, C, options, exception, M, sendException,
  // flushException, closeException; native (every word token): const, char, s, int, f, void, return,
  // and the two Node-API calls.
  assert.ok(r.out.includes('✅ check-exception-word: 21 item(s) checked'), r.text);
});

test('an identifier with the word, in any case, is a finding: TypeScript and C', () => {
  const root = project({
    'src/a.ts': `export class AnException {}\nexport function f(exceptionHandler: number, x = { EXCEPTION_KEY: 1 }) { return exceptionHandler + x.EXCEPTION_KEY; }\nclass G { #lastException = 0; }\n${KEPT}`,
    'native/a.c': '#define MY_EXCEPTION 1\nstatic int last_exception;\n',
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/a.ts:1: identifier `AnException` contains "exception"',
    'src/a.ts:2: identifier `exceptionHandler` contains "exception"',
    'src/a.ts:2: identifier `EXCEPTION_KEY` contains "exception"',
    'src/a.ts:2: identifier `exceptionHandler` contains "exception"',
    'src/a.ts:2: identifier `EXCEPTION_KEY` contains "exception"',
    'src/a.ts:3: identifier `#lastException` contains "exception"',
    'native/a.c:1: identifier `MY_EXCEPTION` contains "exception"',
    'native/a.c:2: identifier `last_exception` contains "exception"',
  ]);
  assert.ok(r.err.includes('❌ check-exception-word: 8 finding(s).'));
  assert.ok(
    r.err.includes(
      'Rename it after the Error: the word "exception" appears only in comments and strings about the Java client (root CLAUDE.md §2, §4).',
    ),
  );
});

test('`exception` is exempt only as a property name: a local, a parameter or a catch binding is a finding', () => {
  const root = project({
    'src/a.ts':
      'export function f(o: { exception: number }) { const { exception: e } = o; const { exception } = o; return { exception, e, x: o.exception }; }\n' +
      'export function g(exception: number) { try { return exception; } catch (exception) { return 0; } }\n' +
      'export const exception = 1;\n' +
      KEPT,
    'native/a.c': 'int a;\n',
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/a.ts:2: identifier `exception` contains "exception"',
    'src/a.ts:2: identifier `exception` contains "exception"',
    'src/a.ts:2: identifier `exception` contains "exception"',
    'src/a.ts:3: identifier `exception` contains "exception"',
  ]);
});

test('an exemption no identifier uses is a finding', () => {
  const root = project({ 'src/a.ts': 'export const a = 1;\n', 'native/a.c': 'int a;\n' });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'isCompletedExceptionally: exempt as Java\'s KafkaFuture#isCompletedExceptionally, which javascript/CLAUDE.md keeps ("KafkaFuture is Java\'s"), but no src/ identifier is named so: delete the exemption',
    ...['sendException', 'flushException', 'closeException'].map(
      (name) =>
        `${name}: exempt as Java's public field MockProducer#${name}, which the TypeScript MockProducer keeps as a field of the same name, but no src/ identifier is named so: delete the exemption`,
    ),
    'exception: exempt as the parameter of Java\'s Callback#onCompletion, a named parameter of the send callback\'s one object (javascript/CLAUDE.md "Java shape, JS idiom": parameters are named as Java names them, as the Python client\'s are), as a property name only, but no src/ identifier is named so: delete the exemption',
  ]);
});

test('a baseline line waives a finding', () => {
  const root = project({
    'src/a.ts': `export const anException = 1;\n${KEPT}`,
    'native/a.c': 'int a;\n',
    'eslint-baseline.txt': 'exception-word src/a.ts:1\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without C sources, or without the typescript package', () => {
  let r = lint(project({ 'src/a.ts': KEPT }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(r.err.slice(0, 2), ['', '❌ check-exception-word: cannot run, no C sources under native/']);
  r = lint(project({ 'src/a.ts': KEPT, 'native/a.c': 'int a;\n' }), [RULE], {
    loadTypescript: () => {
      throw new Error("Cannot find module 'typescript'");
    },
  });
  assert.deepEqual(r.err.slice(0, 2), [
    '',
    "❌ check-exception-word: cannot run, cannot load the typescript package (Cannot find module 'typescript'): run `npm ci` in javascript/",
  ]);
  assert.equal(r.status, 1);
});

test('sources without identifiers fail: 0 items checked', () => {
  const r = lint(
    project({
      'src/a.ts': '// nothing\n',
      'native/a.c': '/* nothing */\n',
      // The unused exemption is waived, so only the count is left.
      'eslint-baseline.txt':
        'unused-exemption isCompletedExceptionally\nunused-exemption exception\n' +
        'unused-exemption sendException\nunused-exemption flushException\nunused-exemption closeException\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-exception-word: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

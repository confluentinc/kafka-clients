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

// check-summaries (scripts/lint/rules/summaries.js): the summaries of the
// classes and interfaces, and of the files of native/.

const assert = require('node:assert/strict');

const { project, lint, findings } = require('./_project');

const RULE = 'check-summaries';

/** The licence header of a file, in its comment syntax (`//`, ` *` or `#`). */
const HEADER = (/** @type {string} */ mark) =>
  ['Copyright 2026 Confluent Inc.', '', 'Licensed under the Apache License, Version 2.0 (the "License");', 'limitations under the License.']
    .map((l) => `${mark}${l === '' ? '' : ` ${l}`}`)
    .join('\n');

test('every class, class expression and interface has a summary; tags alone are none', () => {
  const root = project({
    'src/a.ts': [
      '/** A record. */',
      'export class A {}',
      'class B {}',
      '/** @internal */',
      'export interface C {}',
      'export namespace D {',
      '  /** The function. */',
      '  export interface Fn {}',
      '  export interface Bare {}',
      '}',
      '/** The box. */',
      'const E = class {};',
      'void E;',
      'void new (/** An inline one. */ class {})();',
      'void new (class {})();',
      'void new /** Before its parentheses. */ (class {})();',
      '',
    ].join('\n'),
    'test/t.test.js':
      'class Sub extends Error {}\n/**\n * A sub.\n *\n * @augments Error\n */\nclass Sub2 extends Error {}\nvoid [Sub, Sub2];\n',
    'tools/x/X.java': '/** Writes the vectors. */\npublic final class X {\n  static Class<?> k = String.class;\n  interface Y {}\n}\n',
    'examples/e.js': 'class OutOfScope {}\n',
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/a.ts:B: the class `B` (line 3) has no TSDoc summary: give it one concise sentence of what it does',
    'src/a.ts:C: the interface `C` (line 5) has no TSDoc summary, only tags: give it one concise sentence of what it does',
    'src/a.ts:Bare: the interface `Bare` (line 9) has no TSDoc summary: give it one concise sentence of what it does',
    'src/a.ts:(anonymous): the class `(anonymous)` (line 15) has no TSDoc summary: give it one concise sentence of what it does',
    'test/t.test.js:Sub: the class `Sub` (line 1) has no TSDoc summary: give it one concise sentence of what it does',
    'tools/x/X.java:Y: the interface `Y` (line 4) has no Javadoc summary: give it one concise sentence of what it does',
  ]);
  assert.ok(r.err.includes('❌ check-summaries: 6 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      'Give every class and interface one concise TSDoc summary of what it does, and every file of native/ one after its licence header (javascript/CLAUDE.md, Comments, point 2).',
    ),
  );
});

test('every file of native/ opens, after its licence header, with a comment saying what it does', () => {
  const root = project({
    'native/good.c': `${HEADER('//')}\n\n// The good file.\n\n#include "util.h"\n`,
    'native/block.h': `/*\n${HEADER(' *')}\n *\n * The header, in the licence's block.\n */\n#define X 1\n`,
    'native/after.c': `${HEADER('//')}\n\n/*\n * Its summary, in a block of its own.\n */\nint x;\n`,
    'native/bare.c': `${HEADER('//')}\n\n#include "util.h"\n`,
    'native/empty.h': `${HEADER('//')}\n//\n`,
    'native/none.c': 'int x;\n',
    'native/binding.gyp': `${HEADER('#')}\n\n# The addon's build.\n{ 'targets': [] }\n`,
    'native/bare.gyp': `${HEADER('#')}\n\n{ 'targets': [] }\n`,
    'native/a.map': `/*\n${HEADER(' *')}\n *\n * The linker version script.\n */\n{ local: *; };\n`,
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'native/bare.c: does not open, after its licence header, with a comment saying what the file does: add one',
    'native/bare.gyp: does not open, after its licence header, with a comment saying what the file does: add one',
    'native/empty.h: does not open, after its licence header, with a comment saying what the file does: add one',
    'native/none.c: has no licence header to open with a summary after: give it its header, then a comment saying what the file does',
  ]);
});

test('a clean project passes; nothing in scope fails', () => {
  let r = lint(
    project({
      'src/a.ts': '/** A. */\nexport class A {}\n',
      'native/a.c': `${HEADER('//')}\n\n// The file.\nint a;\n`,
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-summaries: 2 item(s) checked'), r.text);
  r = lint(project({ 'README.md': 'x\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-summaries: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

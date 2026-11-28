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

// check-source-line (scripts/lint/rules/source_line.js) and the reference
// forms of scripts/lint/source_line.js. The references are spelt in pieces
// (`j`), so this file's own comments and strings stay clean.

const assert = require('node:assert/strict');

const { sourceLineRefs, sourceLineRefsRun } = require('../../scripts/lint/source_line');
const { project, lint, findings } = require('./_project');

const RULE = 'check-source-line';

/** `parts` joined. */
const j = (/** @type {string[]} */ ...parts) => parts.join('');

test('a file name and a colon, each extension, with a line or a range, in a code span too', () => {
  assert.deepEqual(sourceLineRefs(j('see rust/src/ffi/consumer/mod', '.rs:120 for the check')), [j('mod', '.rs:120')]);
  assert.deepEqual(
    sourceLineRefs(
      j('as producer', '.rs:12-34, conftest', '.py:3, stubs', '.pyi:9, Grpc', '.cs:7, Test', '.scala:5–8 and node_api', '.cc:1180'),
    ),
    [
      j('producer', '.rs:12-34'),
      j('conftest', '.py:3'),
      j('stubs', '.pyi:9'),
      j('Grpc', '.cs:7'),
      j('Test', '.scala:5–8'),
      j('node_api', '.cc:1180'),
    ],
  );
  assert.deepEqual(sourceLineRefs(j('`mod', '.rs`:42 and `the-file', '.py`:7')), [j('mod', '.rs:42'), j('the-file', '.py:7')]);
});

test('a file name, then a range, or the word line and a line or range', () => {
  assert.deepEqual(sourceLineRefs(j('`x', '.scala` 12-14 and x', '.rs 3/5')), [j('x', '.scala 12-14'), j('x', '.rs 3/5')]);
  assert.deepEqual(sourceLineRefs(j('producer', '.rs line 2675, sender', '.rs, line 12, record', '.rs (lines 3-5)')), [
    j('producer', '.rs line 2675'),
    j('sender', '.rs line 12'),
    j('record', '.rs line 3-5'),
  ]);
  // A single number after a space is a count, not a line.
  assert.deepEqual(sourceLineRefs(j('conftest', '.py 3 times; v1', '.rs 2 tests')), []);
});

test('the word line or at, when the line or the one above names such a file or language', () => {
  assert.deepEqual(sourceLineRefs(j('`testFoo` (producer_perf_test', '.rs closes at line 944)')), ['line 944']);
  assert.deepEqual(sourceLineRefs("Rust's `poll()` (lines 797-812)"), ['line 797-812']);
  assert.deepEqual(sourceLineRefs(j('`SenderTest` in sender', '.rs, at 12-14')), ['at 12-14']);
  assert.deepEqual(sourceLineRefsRun(['// as the Rust core', '// does at line 12']), [[1, 'line 12']]);
  assert.deepEqual(sourceLineRefs('the broker at localhost:9092 on line 3 of the config'), []);
  assert.deepEqual(sourceLineRefs('one line per record, at 3-5 ms'), []);
  assert.deepEqual(sourceLineRefs('Python 3.10 and Rust 1.95 are versions'), []);
});

test('no line: names alone, other extensions, Java (check-java-line reads it), a wrapped reference', () => {
  assert.deepEqual(sourceLineRefs(j('a', '.rs, b', '.py and c', '.cs: names only')), []);
  assert.deepEqual(sourceLineRefs(j('app', '.rsx:12 and lib', '.rs.bak:3 and Foo', '.java:12 and a', '.ts:4')), []);
  assert.deepEqual(sourceLineRefsRun([j('// the check in mod', '.rs, line'), '// 120, reads it']), [[0, j('mod', '.rs line 120')]]);
  assert.deepEqual(sourceLineRefsRun([j('# see producer', '.rs:12')]), [[0, j('producer', '.rs:12')]]);
});

test('the rule: one finding per file of the scope, comments only', () => {
  const root = project({
    'src/a.ts': j('export const a = 1;\n// as the core', "'s mod", '.rs:120 does\n', "const s = 'mod", ".rs:1';\n"),
    'native/a.c': j('/* see producer', '.rs:7 */\nconst char* s = "x', '.py:1";\n'),
    'grpc-server/g.js': j('// conftest', '.py:3 and Grpc', '.cs:4\n'),
    'verifiable_clients/v.js': '// clean\n',
    'test/tsconfig.json': j('{\n  // Test', '.scala:5\n}\n'),
    'examples/e.js': j('// mod', '.rs:1 is out of scope\n'),
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  const rest = ': name the class, member or file instead';
  assert.deepEqual(findings(r), [
    j('src/a.ts: 1 Python, C#, Rust, Scala or C++ file:line reference(s) in comments, the first `mod', '.rs:120` at line 2', rest),
    j(
      'grpc-server/g.js: 2 Python, C#, Rust, Scala or C++ file:line reference(s) in comments, the first `conftest',
      '.py:3` at line 1',
      rest,
    ),
    j('native/a.c: 1 Python, C#, Rust, Scala or C++ file:line reference(s) in comments, the first `producer', '.rs:7` at line 1', rest),
    j(
      'test/tsconfig.json: 1 Python, C#, Rust, Scala or C++ file:line reference(s) in comments, the first `Test',
      '.scala:5` at line 2',
      rest,
    ),
  ]);
  assert.ok(r.err.includes('❌ check-source-line: 4 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      'Name the class, member or file, not a line of it: lines move with every change and cause needless edits (javascript/CLAUDE.md, Comments, point 4).',
    ),
  );
});

test('a clean project passes; nothing in scope fails', () => {
  let r = lint(project({ 'src/a.ts': '// the core names `Sender::run`.\nexport {};\n', 'native/a.c': '// fine\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-source-line: 2 item(s) checked'), r.text);
  r = lint(project({ 'README.md': 'x\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-source-line: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

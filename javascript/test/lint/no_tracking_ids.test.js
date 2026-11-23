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

// check-no-tracking-ids (scripts/lint/rules/no_tracking_ids.js) and the ID
// forms of scripts/lint/tracking_ids.js. The IDs live in strings only, so this
// file's own comments stay clean.

const assert = require('node:assert/strict');

const { trackingIds } = require('../../scripts/lint/tracking_ids');
const { project, lint, findings } = require('./_project');

const RULE = 'check-no-tracking-ids';

/**
 * The IDs `trackingIds` finds in `text`.
 *
 * @param {string} text
 * @returns {string[]}
 */
const ids = (text) => trackingIds(text).map((hit) => hit.id);

test('question IDs, beside a rule group or alone', () => {
  assert.deepEqual(ids('(JavaScript Binding Conventions, Implementation over the FFI, Q62)'), ['Q62']);
  assert.deepEqual(ids("Q21 and Q23, Q25; Q21's rule; (Q31): x"), ['Q21', 'Q23', 'Q25', 'Q21', 'Q31']);
});

test('phase, group and plan labels', () => {
  assert.deepEqual(ids('A1 check-waiting-call-promise, J2 and J3, N1 to N8, D4, D13, H1'), [
    'A1',
    'J2',
    'J3',
    'N1',
    'N8',
    'D4',
    'D13',
    'H1',
  ]);
  assert.deepEqual(ids('C122-5 found three mutants; the gap (X1); M5/P8a; B5, C3 and A8'), ['C122', 'X1', 'M5', 'P8a', 'B5', 'C3', 'A8']);
});

test('review IDs and the review files', () => {
  assert.deepEqual(ids("Critic 140, Actor 12, critic 3, Critics 41; the Critic's forms and an Actor are none"), [
    'Critic 140',
    'Actor 12',
    'critic 3',
    'Critics 41',
  ]);
  assert.deepEqual(ids('see COMMENTS.140.md, COMMENTS.DONE.7 and COMMENTS.FP.md; COMMENTS alone is none'), [
    'COMMENTS.140',
    'COMMENTS.DONE.7',
    'COMMENTS.FP',
  ]);
});

test("a test plan's row labels", () => {
  assert.deepEqual(ids('rows SND-01, CB-05, TOOL-02, PKG-10, SOAK-04, ADM-01, TX1-01'), [
    'SND-01',
    'CB-05',
    'TOOL-02',
    'PKG-10',
    'SOAK-04',
    'ADM-01',
    'TX1-01',
  ]);
});

test('no ID: real names, identifiers, hex, colours, code points, versions and words', () => {
  assert.deepEqual(ids('V8 answers, C11 locks, C99 and C89; Node 22, ES2022, Q&A, X509Certificate'), []);
  assert.deepEqual(ids('0xA1, #A1, U+D800, \\uD800, $A1, +A1, x.A1, kA1, ARG_A1, A1_B, kafka_A1_t, BA1'), []);
  assert.deepEqual(ids('UTF-16, UTF-32, CRC-32, KIP-848, SHA-256, LICENSE-2.0, ISO-8859-1, SND-1, SND-123'), []);
});

test('the rule: one finding per ID on its line, in comments of every kind of file, never in strings', () => {
  const root = project({
    'src/a.ts': [
      'export const a = 1;',
      '// JS conventions, Signatures (Q15).',
      "const s = 'Q21 in a string';",
      '/**',
      ' * The rows SND-01 and CB-02, as Critic',
      ' * 140 asked.',
      ' */',
      'export const b = `A1 in a template`;',
      '',
    ].join('\n'),
    'native/a.c': '/* N8 check-no-binding-queue */\nconst char* s = "Q1";\n',
    'native/binding.gyp': "# the gyp file (Q73)\n{ 'x': 'a#Q2' }\n",
    'scripts/s.js': '// V8 and C11 are names.\n',
    'verifiable_clients/v.js': '// see COMMENTS.12.md\n',
    'examples/e.js': '// Q9 is out of scope\n',
  });
  const r = lint(root, [RULE]);
  assert.equal(r.status, 1);
  // ESLint's half (TypeScript and JavaScript) first, then the runner's.
  assert.deepEqual(findings(r), [
    'src/a.ts:2: `Q15`, a question ID, in a comment: drop it',
    "src/a.ts:5: `SND-01`, a test plan's row label, in a comment: drop it",
    "src/a.ts:5: `CB-02`, a test plan's row label, in a comment: drop it",
    // A review ID wrapped onto the next line, reported on its first.
    'src/a.ts:5: `Critic 140`, a review ID, in a comment: drop it',
    'verifiable_clients/v.js:1: `COMMENTS.12`, a review ID, in a comment: drop it',
    'native/a.c:1: `N8`, a phase or plan label, in a comment: drop it',
    'native/binding.gyp:1: `Q73`, a question ID, in a comment: drop it',
  ]);
  assert.ok(r.err.includes('❌ check-no-tracking-ids: 7 finding(s).'), r.text);
  assert.ok(
    r.err.includes(
      "Drop the ID, keeping the rule group's or the decision's name: the design notes and the rules hold the IDs (javascript/CLAUDE.md, Comments, point 5).",
    ),
  );
});

test('a clean project passes; nothing in scope fails', () => {
  let r = lint(project({ 'src/a.ts': '// JS conventions, Signatures.\nexport {};\n', 'native/a.c': '// V8 and C11.\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-no-tracking-ids: 2 item(s) checked'), r.text);
  r = lint(project({ 'README.md': 'Q21\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-no-tracking-ids: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

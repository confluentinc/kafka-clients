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

// D1 check-tsdoc-present (scripts/lint/rules/tsdoc_present.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');

const RULE = 'check-tsdoc-present';

const REC = `/** A record. */
export class Rec {
  /** Unknown. */
  static readonly UNKNOWN: number = -1;
  /** Builds one. */
  constructor() {}
  /** The topic. */
  get topic(): string { return ''; }
  set topic(v: string) { void v; }
  /** Closes it. */
  close(): void;
  close(timeoutMs: number): void;
  close(timeoutMs?: number): void { void timeoutMs; }
  private hidden(): void {}
  #state = 1;
}

/** Makes one. */
export function make(): Rec;
export function make(n: number): Rec;
export function make(n?: number): Rec { void n; return new Rec(); }

/** A mode. */
export type Mode = 'on' | 'off';

/** The options. */
export interface Options {
  /** The timeout. */
  readonly timeoutMs?: number;
}

/** The answer. */
export const ANSWER = 42;
`;

test('every export and public member documented passes; an accessor pair or an overload set needs one comment', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'lib/producer/rec.ts': REC }), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec, UNKNOWN, constructor, topic, close; make; Mode; Options, timeoutMs; ANSWER.
  assert.ok(r.out.includes('✅ check-tsdoc-present: 10 item(s) checked'), r.text);
});

test('an export or public member without a TSDoc comment is reported', () => {
  const files = { 'tsconfig.json': TSCONFIG };
  const cases = [
    ['/** A record. */\nexport class Rec', 'export class Rec', 'lib/producer/rec.ts:Rec: has no TSDoc comment'],
    ['  /** The topic. */\n', '', 'Rec.topic: has no TSDoc comment'],
    ['  /** Closes it. */\n', '', 'Rec.close: has no TSDoc comment'],
    ['  /** Unknown. */\n', '', 'Rec.UNKNOWN: has no TSDoc comment'],
    ['  /** Builds one. */\n', '', 'Rec.constructor: has no TSDoc comment'],
    ['/** Makes one. */\n', '', 'lib/producer/rec.ts:make: has no TSDoc comment'],
    ['/** A mode. */\n', '', 'lib/producer/rec.ts:Mode: has no TSDoc comment'],
    ['  /** The timeout. */\n', '', 'Options.timeoutMs: has no TSDoc comment'],
    ['/** The answer. */\n', '', 'lib/producer/rec.ts:ANSWER: has no TSDoc comment'],
    // A line comment is not TSDoc.
    ['/** The answer. */\n', '// The answer.\n', 'lib/producer/rec.ts:ANSWER: has no TSDoc comment'],
  ];
  for (const [from, to, want] of cases) {
    const r = lint(project({ ...files, 'lib/producer/rec.ts': REC.replace(from, to) }), [RULE]);
    assert.deepEqual(findings(r), [want], from);
    assert.ok(r.err.includes('❌ check-tsdoc-present: 1 finding(s).'), r.text);
  }
});

test('a comment with no description, empty or tags only, is doc-empty', () => {
  const files = { 'tsconfig.json': TSCONFIG };
  for (const [from, to] of [
    ['/** A record. */\nexport class Rec', '/** */\nexport class Rec'],
    ['/** A record. */\nexport class Rec', '/**\n *\n */\nexport class Rec'],
    ['  /** Closes it. */\n', '  /** @param timeoutMs - the timeout */\n'],
  ]) {
    const r = lint(project({ ...files, 'lib/producer/rec.ts': REC.replace(from, to) }), [RULE]);
    const subject = from.includes('Closes') ? 'Rec.close' : 'lib/producer/rec.ts:Rec';
    assert.deepEqual(findings(r), [`${subject}: has a TSDoc comment with no description (an empty comment, or tags only)`], to);
  }
});

test('private members and modules whose path starts with _ are not checked', () => {
  const r = lint(
    project({ 'tsconfig.json': TSCONFIG, 'lib/producer/rec.ts': REC, 'lib/_internal.ts': 'export function helper(): void {}\n' }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
  assert.ok(r.out.includes('✅ check-tsdoc-present: 10 item(s) checked'), r.text);
});

test('cannot run without the TypeScript project', () => {
  const r = lint(project({ 'lib/producer/rec.ts': REC }), [RULE]);
  assert.equal(r.status, 1);
  assert.match(r.err[1], /^❌ check-tsdoc-present: cannot run, /);
});

test('nothing exported fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'lib/producer/rec.ts': 'const x = 1;\nvoid x;\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-tsdoc-present: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

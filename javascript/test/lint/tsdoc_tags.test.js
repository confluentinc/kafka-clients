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

// D2 check-tsdoc-tags (scripts/lint/rules/tsdoc_tags.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { javaClass, javaIndex, method } = require('./_java_fixture');

const RULE = 'check-tsdoc-tags';

const throwing = (...classes) => ({
  javadoc: { text: 'x', params: [], returns: null, throws: classes.map((c) => ({ class: `org.apache.kafka.${c}`, text: 'when' })), words: 1 },
});

const INDEX = javaIndex([
  javaClass('clients.producer.Rec', {
    overloads: [
      method('Rec', ['String']),
      method('send', ['String'], throwing('common.errors.FooException')),
      method('close', ['Duration'], throwing('common.errors.FooException')),
      method('close', [], { deprecated: true, ...throwing('common.errors.BarException') }),
      method('size'),
    ],
  }),
  javaClass('common.errors.FooException'),
]);

const REC = `/** A record. */
export class Rec {
  /**
   * Builds one.
   *
   * @param topic - the topic
   */
  constructor(topic: string) { void topic; }
  /**
   * Sends it.
   *
   * @param topic - the topic
   * @returns the offset
   * @throws \`FooError\` when it fails
   */
  send(topic: string): number { void topic; return 0; }
  /**
   * Closes it.
   *
   * @param options - \`timeoutMs\`: how long to wait; \`drain\`: whether to drain
   * @throws \`FooError\` when it fails
   * @throws \`TypeError\` when \`timeoutMs\` is not a number
   */
  close(options?: {
    timeoutMs?: number;
    drain?: boolean;
    /** Why. */
    reason?: string;
  }): Promise<void> {
    void options;
    return Promise.resolve();
  }
  /** The size, a getter: no @returns. */
  get size(): number { return 0; }
}
`;

const FOO = `/** Foo. */
export class FooError extends Error {}
`;

const files = (rec = REC) => ({
  'tsconfig.json': TSCONFIG,
  'build/java-index.json': INDEX,
  'lib/producer/rec.ts': rec,
  'lib/common/errors/foo_error.ts': FOO,
  'lib/_error_registry.ts': "export const ERRORS = [\n  ['./common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],\n];\n",
});

test('every parameter, option, return and Java throw tagged passes', () => {
  const r = lint(project(files()), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec's constructor, send and close.
  assert.ok(r.out.includes('✅ check-tsdoc-tags: 3 item(s) checked'), r.text);
});

test('a missing, hyphenless or unknown @param is reported', () => {
  const cases = [
    ['   * @param topic - the topic\n   * @returns', '   * @returns', ['Rec.send/topic: has no @param topic -']],
    ['   * @param topic - the topic\n   * @returns', '   * @param topic the topic\n   * @returns', ['Rec.send/topic: @param topic lacks the TSDoc hyphen (@param topic - text)']],
    [
      '   * @param topic - the topic\n   * @returns',
      '   * @param topic - the topic\n   * @param other - none\n   * @returns',
      ['Rec.send/other: @param other names no parameter'],
    ],
  ];
  for (const [from, to, want] of cases) {
    const r = lint(project(files(REC.replace(from, to))), [RULE]);
    assert.deepEqual(findings(r), want, to);
  }
});

test("an options object's property is documented in its type or named in the @param text", () => {
  // timeoutMs and drain are named in backquotes, reason has its own TSDoc.
  const drain = 'Rec.close/options.drain: option drain of @param options is documented neither in its type nor in backquotes in the @param text';
  let r = lint(project(files(REC.replace('; `drain`: whether to drain', ''))), [RULE]);
  assert.deepEqual(findings(r), [drain]);
  // The bare word does not count: any sentence may use it.
  r = lint(project(files(REC.replace('`drain`: whether to drain', 'drain whether to drain'))), [RULE]);
  assert.deepEqual(findings(r), [drain]);
  r = lint(project(files(REC.replace('    /** Why. */\n', ''))), [RULE]);
  assert.deepEqual(findings(r), ['Rec.close/options.reason: option reason of @param options is documented neither in its type nor in backquotes in the @param text']);
});

test("a library mapped type over an object literal is an options object; a library interface is not", () => {
  for (const wrap of ['Readonly', 'Partial', 'Required']) {
    const r = lint(project(files(REC.replace('  close(options?: {', `  close(options?: ${wrap}<{`).replace('  }): Promise<void> {', '  }>): Promise<void> {').replace('    /** Why. */\n', ''))), [RULE]);
    assert.deepEqual(findings(r), ['Rec.close/options.reason: option reason of @param options is documented neither in its type nor in backquotes in the @param text'], wrap);
  }
});

test('a non-void return needs @returns; void, undefined and Promise<void> do not', () => {
  const r = lint(project(files(REC.replace('   * @returns the offset\n', ''))), [RULE]);
  assert.deepEqual(findings(r), ['Rec.send: returns number but has no @returns']);
});

test("each class a live Java overload's @throws names needs its @throws, named as J1 names it", () => {
  let r = lint(project(files(REC.replace('   * @returns the offset\n   * @throws `FooError` when it fails\n', '   * @returns the offset\n'))), [RULE]);
  assert.deepEqual(findings(r), ["Rec.send/FooError: Java's send throws FooException: document it as @throws `FooError`"]);
  // A deprecated overload's throws (BarException) are not asked for.
  r = lint(project(files()), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('a @throws must name a class lib/ exports or a JavaScript error, in backquotes', () => {
  let r = lint(project(files(REC.replace('`TypeError`', '`NoSuchError`'))), [RULE]);
  assert.deepEqual(findings(r), ['Rec.close/NoSuchError: @throws `NoSuchError` names no class lib/ exports nor a JavaScript error']);
  r = lint(project(files(REC.replace('`TypeError` when', 'TypeError when'))), [RULE]);
  assert.deepEqual(findings(r), ['Rec.close/TypeError: @throws TypeError does not start with the error class in backquotes (@throws `XError` text)']);
});

test("a function type's parameters need their @param: a type alias and a function-typed property", () => {
  const callback = `/**
 * A callback.
 *
 * @param metadata - the metadata
 * @param error - the error, or null
 */
export type Callback = (metadata: unknown, error: Error | null) => void;

/** Hooks. */
export interface Hooks {
  /**
   * Runs on close.
   *
   * @param reason - why
   */
  readonly onClose: (reason: string) => void;
}
`;
  let r = lint(project({ ...files(), 'lib/producer/callback.ts': callback }), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec's three, Callback and Hooks.onClose; an Error parameter is no options object.
  assert.ok(r.out.includes('✅ check-tsdoc-tags: 5 item(s) checked'), r.text);
  r = lint(project({ ...files(), 'lib/producer/callback.ts': callback.replace(' * @param error - the error, or null\n', '').replace('   * @param reason - why\n', '') }), [RULE]);
  assert.deepEqual(findings(r), ['lib/producer/callback.ts:Callback/error: has no @param error -', 'Hooks.onClose/reason: has no @param reason -']);
});

test('no JSDoc tag carries a type, anywhere in lib/', () => {
  const r = lint(project({ ...files(REC.replace('@param topic - the topic\n   * @returns', '@param {string} topic - the topic\n   * @returns')), 'lib/_util.ts': '/** @returns {number} one */\nexport function one(): number { return 1; }\n' }), [RULE]);
  assert.deepEqual(findings(r), [
    'lib/_util.ts:1: @returns carries the JSDoc type {number}: TypeScript gives the type',
    'lib/producer/rec.ts:12: @param carries the JSDoc type {string}: TypeScript gives the type',
  ]);
});

test('cannot run without the Java index', () => {
  const f = files();
  delete f['build/java-index.json'];
  const r = lint(project(f), [RULE]);
  assert.equal(r.status, 1);
  assert.match(r.err[1], /^❌ check-tsdoc-tags: cannot run, no Java index/);
});

test('no documented function fails: 0 items checked', () => {
  const r = lint(project({ ...files('/** A record. */\nexport class Rec {}\n') }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-tsdoc-tags: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

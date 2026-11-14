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

// J3 check-java-completeness (scripts/lint/rules/java_completeness.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaIndex } = require('./_java_fixture');

const RULE = 'check-java-completeness';

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(CLASSES), ...LIB, ...files });

test('a class translating every public, live Java member passes', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec: UNKNOWN, Rec, topic, offset, getSize, isOpen, hasKey, close, toString, equals, hashCode
  // (not old, hidden, mode, LEGACY, SECRET); Mode: ON, OFF, forName, java.lang.Enum's values, valueOf, name, ordinal, and toJSON;
  // ProducerConfig: A_CONFIG (its constants only); the interface Header: key, value; the abstract
  // Future: get, not its constructor. The error and the function type ask nothing.
  assert.ok(r.out.includes('✅ check-java-completeness: 23 item(s) checked'), r.text);
});

test('equals and hashCode are asked for as any member is (root Class family, Q29)', () => {
  const rec = LIB['src/producer/rec.ts']
    .replace('  equals(options: { o: unknown }): boolean;\n  equals(o: unknown): boolean;\n', '')
    .replace('  equals(options: unknown): boolean { void options; return false; }\n', '')
    .replace('  hashCode(): number { return 0; }\n', '');
  assert.deepEqual(findings(lint(fixture({ 'src/producer/rec.ts': rec }), [RULE])), [
    'clients.producer.Rec#equals: Rec does not translate it',
    'clients.producer.Rec#hashCode: Rec does not translate it',
  ]);
});

test("an enum missing java.lang.Enum's members, or toJSON, is a finding, listed or not in the Java index", () => {
  const mode = LIB['src/producer/mode.ts']
    .replace('  static valueOf(n: string): Mode { void n; return Mode.ON; }\n', '')
    .replace('  name(): string { return this.tag; }\n', '');
  let r = lint(fixture({ 'src/producer/mode.ts': mode }), [RULE]);
  assert.deepEqual(findings(r), [
    'clients.producer.Mode#valueOf: Mode does not translate it',
    'clients.producer.Mode#name: Mode does not translate it',
  ]);
  r = lint(fixture({ 'src/producer/mode.ts': LIB['src/producer/mode.ts'].replace('  toJSON(): string { return this.tag; }\n', '') }), [RULE]);
  assert.deepEqual(findings(r), ['clients.producer.Mode#toJSON: Mode does not translate it']);
  // An index that lists none of them asks for them all the same.
  const bare = CLASSES.map((c) =>
    c.fqn.endsWith('.Mode') ? { ...c, overloads: c.overloads.filter((o) => !['values', 'valueOf', 'name', 'ordinal'].includes(o.name)) } : c,
  );
  r = lint(fixture({ 'java-index.json': javaIndex(bare), 'src/producer/mode.ts': mode.replace('  ordinal(): number { return 0; }\n', '') }), [RULE]);
  assert.deepEqual(findings(r), [
    'clients.producer.Mode#valueOf: Mode does not translate it',
    'clients.producer.Mode#name: Mode does not translate it',
    'clients.producer.Mode#ordinal: Mode does not translate it',
  ]);
});

test('a member, the constructor and a constant left out are each a finding', () => {
  const r = lint(
    fixture({
      'src/producer/rec.ts': LIB['src/producer/rec.ts']
        .replace('  static readonly UNKNOWN: number = -1;\n', '')
        .replace('  constructor(options: { p0: string; p1?: number });\n  constructor(p0: string);\n  constructor(options: unknown) { void options; }\n', '  private constructor() {}\n')
        .replace('  get size(): number { return 0; }\n', ''),
      'src/producer/producer_config.ts': 'export class ProducerConfig {}\n',
      'src/common/header/header.ts': 'export type Header = { readonly key: string };\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'common.header.Header#value: Header does not translate it',
    'clients.producer.ProducerConfig#A_CONFIG: ProducerConfig does not translate it',
    'clients.producer.Rec#UNKNOWN: Rec does not translate it',
    'clients.producer.Rec#Rec: Rec has no public constructor',
    'clients.producer.Rec#getSize: Rec does not translate it',
  ]);
});

test('the baseline lists what is not translated yet', () => {
  const root = fixture({
    'src/producer/rec.ts': LIB['src/producer/rec.ts'].replace('  close(options?: { p0?: number }): void;\n  close(p0: number): void;\n  close(options?: unknown): void { void options; }\n', ''),
    'eslint-baseline.txt': '# not yet\nmissing clients.producer.Rec#close\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.equal(r.status, 1);
  assert.match(r.err[1], /^❌ check-java-completeness: cannot run, no Java index/);
});

test('no class to complete fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(CLASSES), 'src/producer/callback.ts': LIB['src/producer/callback.ts'] }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-java-completeness: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

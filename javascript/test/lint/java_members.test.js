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

// J2 check-java-members (scripts/lint/rules/java_members.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaIndex } = require('./_java_fixture');

const RULE = 'check-java-members';

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(CLASSES), ...LIB, ...files });

test('members named as Java\'s pass: getters as properties, supertype methods, constants, the constructor, tags', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec: 11 members (the @jsOnly one, the private and # ones not counted); Mode: 6, its toString
  // java.lang.Enum's; ProducerConfig: 2; Header: 2 (an interface); Future: get (its constructor
  // private); FooError: its 2 fields; Callback is a function type.
  assert.ok(r.out.includes('✅ check-java-members: 24 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'lib/producer/rec.ts': LIB['lib/producer/rec.ts']
        .replace('get offset(): number { return 0; }', 'getOffset(): number { return 0; }')
        .replace('get isOpen(): boolean { return true; }', 'get open(): boolean { return true; }')
        .replace('flush(): void {}', 'static flush(): void {}')
        .replace(
          'extra(): void {}',
          'extra(): void {}\n  bogus(): void {}\n  static readonly mode: string = "m";\n  static readonly counter: number = 0;',
        ),
      'lib/producer/mode.ts': LIB['lib/producer/mode.ts'].replace('static forName(', 'forName('),
      'lib/common/header/header.ts': 'export interface Header {\n  readonly key: string;\n  value(): unknown;\n  extra: number;\n}\n',
      'lib/common/errors/foo_error.ts': 'export class FooError extends Error {\n  readonly payload!: string;\n}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'Header.extra: no Java member of common.header.Header',
    'Mode.forName: is an instance member, but clients.producer.Mode#forName is static',
    "Rec.getOffset: no Java member of clients.producer.Rec; Java's getter offset() is the property offset",
    "Rec.open: no Java member of clients.producer.Rec; Java's isOpen() is the property isOpen",
    'Rec.flush: is static, but clients.producer.Base#flush is an instance member',
    'Rec.bogus: no Java member of clients.producer.Rec',
    'Rec.mode: is static, but clients.producer.Rec#mode is an instance member',
    'Rec.counter: is static, but clients.producer.Rec#counter is not a static final field',
    'FooError.payload: is not one of the error fields (constructor, name, code, errorName, retriable, apiError, message, cause): an error class keeps the fields javascript/CLAUDE.md "Errors" lists, not Java\'s members',
  ]);
});

test('toString maps to java.lang.Enum only on an enum', () => {
  const r = lint(fixture({ 'lib/producer/rec.ts': LIB['lib/producer/rec.ts'].replace('  toString(): string { return \'\'; }\n', '') + '' }), [RULE]);
  assert.equal(r.status, 0, r.text);
  const classes = CLASSES.map((c) => (c.fqn.endsWith('.Rec') ? { ...c, overloads: c.overloads.filter((o) => o.name !== 'toString') } : c));
  const root = project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), ...LIB });
  assert.deepEqual(findings(lint(root, [RULE])), ['Rec.toString: no Java member of clients.producer.Rec']);
});

test('a member tagged @java maps to the member it names', () => {
  const r = lint(
    fixture({
      'lib/producer/rec.ts': LIB['lib/producer/rec.ts'].replace(
        'extra(): void {}',
        'extra(): void {}\n  /** @java org.apache.kafka.clients.producer.Rec#hidden */\n  conceal(): void {}',
      ),
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.equal(r.status, 1);
  assert.match(r.err[1], /^❌ check-java-members: cannot run, no Java index at build\/java-index\.json/);
});

test('no exported class with a Java class fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(CLASSES), 'lib/producer/callback.ts': LIB['lib/producer/callback.ts'] }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-java-members: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

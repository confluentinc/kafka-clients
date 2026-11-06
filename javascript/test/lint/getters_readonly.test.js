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

// J8 check-getters-readonly (scripts/lint/rules/getters_readonly.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaIndex, method } = require('./_java_fixture');

const INDEX = javaIndex(CLASSES);

const RULE = 'check-getters-readonly';

test('get accessors and static readonly constants pass; an error class keeps its fields', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, ...LIB }), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec.UNKNOWN, Mode.ON / OFF, ProducerConfig.A_CONFIG, FooError.code / retriable.
  assert.ok(r.out.includes('✅ check-getters-readonly: 6 item(s) checked'), r.text);
});

const PROTO = `export class Proto {
  readonly id: number;
  readonly name: string;
  constructor() {
    this.id = 1;
    this.name = 'x';
    Object.freeze(this);
  }
}
`;

test("a readonly property of a frozen object may translate a Java public final field, never a getter", () => {
  const files = { 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX };
  // Frozen: id (Java's public final field) passes; name (Java's name() method) does not.
  let r = lint(project({ ...files, 'lib/producer/proto.ts': PROTO }), [RULE]);
  assert.deepEqual(findings(r), ['Proto.name: is a readonly data property, not a get accessor']);
  // Not frozen: neither passes.
  r = lint(project({ ...files, 'lib/producer/proto.ts': PROTO.replace('    Object.freeze(this);\n', '') }), [RULE]);
  assert.deepEqual(findings(r), [
    'Proto.id: is a readonly data property, not a get accessor',
    'Proto.name: is a readonly data property, not a get accessor',
  ]);
  // A freeze that may not run, or runs on another `this`, does not count.
  for (const freeze of [
    '    if (this.id < 0) Object.freeze(this);\n',
    '    const lock = function (this: object): void { Object.freeze(this); };\n    void lock;\n',
  ]) {
    r = lint(project({ ...files, 'lib/producer/proto.ts': PROTO.replace('    Object.freeze(this);\n', freeze) }), [RULE]);
    assert.deepEqual(findings(r), [
      'Proto.id: is a readonly data property, not a get accessor',
      'Proto.name: is a readonly data property, not a get accessor',
    ]);
  }
  // A class with no Java class: not either.
  r = lint(project({ ...files, 'lib/producer/proto2.ts': PROTO.replace('class Proto', 'class Proto2') }), [RULE]);
  assert.deepEqual(findings(r), [
    'Proto2.id: is a readonly data property, not a get accessor',
    'Proto2.name: is a readonly data property, not a get accessor',
  ]);
});

test("on a frozen enum, name passes as java.lang.Enum's name(); on a frozen class it does not", () => {
  const files = { 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX };
  const mode = `export class Mode {
  readonly name: string;
  private constructor(name: string) {
    this.name = name;
    Object.freeze(this);
  }
}
`;
  let r = lint(project({ ...files, 'lib/producer/mode.ts': mode }), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(project({ ...files, 'lib/producer/mode.ts': mode.replace('    Object.freeze(this);\n', '') }), [RULE]);
  assert.deepEqual(findings(r), ['Mode.name: is a readonly data property, not a get accessor']);
  // Rec is a class: its name() would be a getter, not Enum's.
  const classes = CLASSES.map((c) => (c.fqn.endsWith('.Rec') ? { ...c, overloads: [...c.overloads, method('name')] } : c));
  r = lint(
    project({ ...files, 'build/java-index.json': javaIndex(classes), 'lib/producer/rec.ts': mode.replace(/Mode/g, 'Rec').replace('private constructor', 'constructor') }),
    [RULE],
  );
  assert.deepEqual(findings(r), ['Rec.name: is a readonly data property, not a get accessor']);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'build/java-index.json': INDEX,
      'lib/a.ts': `export class A {
  count = 1;
  readonly id: number = 2;
  static total = 0;
  constructor(public readonly owner: string) {}
  get size(): number { return 1; }
  set size(v: number) { void v; }
  private hidden = 3;
  #own = 4;
}
`,
      'lib/_private.ts': 'export class P { open = 1; }\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'A.count: is a public mutable field: make it a get accessor',
    'A.id: is a readonly data property, not a get accessor',
    'A.total: is a static property that is not readonly',
    'A.owner: is a readonly data property, not a get accessor',
    'A.size: is a set accessor: the property is read-only',
  ]);
});

test('cannot run without tsconfig.json, or without the Java index', () => {
  let r = lint(project({ 'build/java-index.json': INDEX, 'lib/a.ts': 'export class A {}\n' }), [RULE]);
  assert.deepEqual(r.err.slice(0, 2), ['', '❌ check-getters-readonly: cannot run, no tsconfig.json']);
  r = lint(project({ 'tsconfig.json': TSCONFIG, 'lib/a.ts': 'export class A {}\n' }), [RULE]);
  assert.match(r.err[1], /^❌ check-getters-readonly: cannot run, no Java index/);
});

test('no property to check fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, 'lib/a.ts': 'export class A { m(): void {} }\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-getters-readonly: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

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

// J8 check-data-objects (scripts/lint/rules/data_objects.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, javaIndex, method, field, javaClass } = require('./_java_fixture');
const rule = require('../../scripts/lint/rules/data_objects');

const RULE = 'check-data-objects';

// Rec is the fixture's data class: its Java getters are topic(), offset(),
// isOpen(), hasKey() (getSize() keeps its name, a method).
const DATA = new Set(['org.apache.kafka.clients.producer.Rec']);
const withData = (data = DATA) => ({ rules: [Object.assign(Object.create(rule), { DATA_CLASSES: data })] });
// Rec's close() returns nothing: not a getter.
const VOID_CLOSE = CLASSES.map((c) =>
  c.fqn.endsWith('.Rec') ? { ...c, overloads: c.overloads.map((o) => (o.name === 'close' ? { ...o, return_type: 'void' } : o)) } : c,
);
const INDEX = javaIndex(VOID_CLOSE);
const FILES = { 'tsconfig.json': TSCONFIG, 'java-index.json': INDEX };

const REC = `export class Rec {
  static readonly UNKNOWN: number = -1;
  readonly topic: string;
  readonly offset: number;
  readonly isOpen: boolean;
  readonly hasKey: boolean;
  constructor(topic: string) {
    this.topic = topic;
    this.offset = 0;
    this.isOpen = true;
    this.hasKey = false;
  }
  getSize(): number { return 0; }
  close(): void {}
  toString(): string { return this.topic; }
}
`;

const FUTURE = `export class Future {
  private constructor() {}
  get(): unknown { return null; }
  isDone(): boolean { return false; }
}
`;

const PROTO = `export class Proto {
  readonly id: number;
  #name: string;
  constructor() {
    this.id = 1;
    this.#name = 'x';
    Object.freeze(this);
  }
  name(): string { return this.#name; }
}
`;

test('a data class with readonly fields, and classes with methods, pass', () => {
  const r = lint(
    project({ ...FILES, 'src/producer/rec.ts': REC, 'src/common/future.ts': FUTURE, 'src/producer/proto.ts': PROTO }),
    [RULE],
    withData(),
  );
  assert.equal(r.status, 0, r.text);
  // Rec: UNKNOWN and its 4 getter fields; Proto.id; the one data class listed.
  assert.ok(r.out.includes('✅ check-data-objects: 7 item(s) checked'), r.text);
});

test("the shipped list names the data classes the client translates, not Header (the root's list does not)", () => {
  assert.deepEqual([...rule.DATA_CLASSES].sort(), [
    'org.apache.kafka.clients.producer.ProducerRecord',
    'org.apache.kafka.clients.producer.RecordMetadata',
    'org.apache.kafka.common.Node',
    'org.apache.kafka.common.TopicPartition',
  ]);
});

test('on a data class, a Java getter as a get accessor, a method or a mutable field is a finding', () => {
  const bad = REC.replace('  readonly topic: string;\n', '  #topic = "";\n')
    .replace('    this.topic = topic;\n', '    this.#topic = topic;\n')
    .replace('  getSize(): number { return 0; }\n', '  getSize(): number { return 0; }\n  get topic(): string { return this.#topic; }\n')
    .replace('  readonly offset: number;\n', '  offset: number;\n')
    .replace('  readonly hasKey: boolean;\n', '')
    .replace('    this.hasKey = false;\n', '')
    .replace('  close(): void {}\n', '  close(): void {}\n  hasKey(): boolean { return false; }\n  readonly extra: number = 1;\n  get size(): number { return 0; }\n');
  const r = lint(project({ ...FILES, 'src/producer/rec.ts': bad }), [RULE], withData());
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    "Rec.offset: is a public mutable field: make it readonly",
    "Rec.topic: is a get accessor: on a data class, Java's topic() is a public readonly field",
    "Rec.hasKey: is a method: on a data class, Java's hasKey() is a public readonly field",
    'Rec.extra: is a readonly field translating no Java getter of the data class',
    'Rec.size: is a get accessor: a data class has fields, not accessors',
  ]);
});

test('a class that is not a data class keeps methods: a get accessor or a readonly field is a finding', () => {
  // The same Rec, not listed as a data class.
  let r = lint(project({ ...FILES, 'src/producer/rec.ts': REC }), [RULE], withData(new Set()));
  assert.deepEqual(findings(r), [
    'Rec.topic: is a readonly field of a class that is not a data class: make it a method',
    'Rec.offset: is a readonly field of a class that is not a data class: make it a method',
    'Rec.isOpen: is a readonly field of a class that is not a data class: make it a method',
    'Rec.hasKey: is a readonly field of a class that is not a data class: make it a method',
  ]);
  r = lint(
    project({ ...FILES, 'src/common/future.ts': FUTURE.replace('  isDone(): boolean { return false; }\n', '  get isDone(): boolean { return false; }\n') }),
    [RULE],
    withData(new Set()),
  );
  assert.deepEqual(findings(r), ['Future.isDone: is a get accessor: on a class that is not a data class, a Java getter is a method']);
});

test("a readonly field of a frozen object may translate a Java public final field, never a getter", () => {
  const files = FILES;
  // Frozen: id (Java's public final field) passes.
  let r = lint(project({ ...files, 'src/producer/proto.ts': PROTO }), [RULE], withData(new Set()));
  assert.equal(r.status, 0, r.text);
  // name as a field: Java's name() is a method on a class that is not a data class.
  const asField = PROTO.replace('  #name: string;\n', '  readonly name: string;\n').replace("this.#name = 'x';", "this.name = 'x';").replace('  name(): string { return this.#name; }\n', '');
  r = lint(project({ ...files, 'src/producer/proto.ts': asField }), [RULE], withData(new Set()));
  assert.deepEqual(findings(r), ['Proto.name: is a readonly field of a class that is not a data class: make it a method']);
  // Not frozen, or a freeze that may not run or runs on another `this`: id fails too.
  for (const freeze of [
    '',
    '    if (this.id < 0) Object.freeze(this);\n',
    '    const lock = function (this: object): void { Object.freeze(this); };\n    void lock;\n',
  ]) {
    r = lint(project({ ...files, 'src/producer/proto.ts': PROTO.replace('    Object.freeze(this);\n', freeze) }), [RULE], withData(new Set()));
    assert.deepEqual(findings(r), ['Proto.id: is a readonly field of a class that is not a data class: make it a method']);
  }
  // A class with no Java class: no Java field to translate.
  r = lint(project({ ...files, 'src/producer/proto2.ts': PROTO.replace('class Proto', 'class Proto2') }), [RULE], withData(new Set()));
  assert.deepEqual(findings(r), ['Proto2.id: is a readonly field of a class that is not a data class: make it a method']);
});

test('a mutable field may translate a Java public mutable field only (MockProducer.sendException)', () => {
  const mutable = (fields) => {
    const classes = [...VOID_CLOSE.filter((c) => !c.fqn.endsWith('.Proto')), javaClass('clients.producer.Proto', { overloads: [method('name')], fields })];
    return { 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(classes) };
  };
  const source = 'export class Proto {\n  injected: Error | null = null;\n  name(): string { return "x"; }\n}\n';
  let r = lint(project({ ...mutable([field('injected', { type: 'RuntimeException', static: false, final: false })]), 'src/producer/proto.ts': source }), [RULE], withData(new Set()));
  assert.equal(r.status, 0, r.text);
  // A Java final, private or static field is no such field.
  for (const extra of [{ final: true }, { visibility: 'private' }, { static: true }]) {
    r = lint(
      project({ ...mutable([field('injected', { type: 'RuntimeException', static: false, final: false, ...extra })]), 'src/producer/proto.ts': source }),
      [RULE],
      withData(new Set()),
    );
    assert.deepEqual(findings(r), ['Proto.injected: is a public mutable field: make it readonly'], JSON.stringify(extra));
  }
});

test('each other finding kind, with its exact text', () => {
  const r = lint(
    project({
      ...FILES,
      'src/a.ts': `export class A {
  count = 1;
  static total = 0;
  constructor(public readonly owner: string) {}
  get size(): number { return 1; }
  set size(v: number) { void v; }
  private hidden = 3;
  #own = 4;
}
`,
      'src/internal/private.ts': 'export class P { open = 1; }\n',
    }),
    [RULE],
    withData(new Set(['org.apache.kafka.clients.producer.Gone'])),
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'A.count: is a public mutable field: make it readonly',
    'A.total: is a static property that is not readonly',
    'A.owner: is a readonly field of a class that is not a data class: make it a method',
    'A.size: is a get accessor: on a class that is not a data class, a Java getter is a method',
    'A.size: is a set accessor: the property is read-only',
    'org.apache.kafka.clients.producer.Gone: is listed as a data class, but no exported class translates it: delete the entry',
  ]);
});

test('an error class keeps its fields, and no code or retriable flag', () => {
  const classes = [...CLASSES, javaClass('common.errors.BarException', { fields: [field('x')], overloads: [method('m')] })];
  const run = (body) =>
    lint(
      project({
        'tsconfig.json': TSCONFIG,
        'java-index.json': javaIndex(classes),
        'src/common/errors/foo_error.ts': `export class FooError extends Error {\n${body}}\n`,
        'src/internal/error_registry.ts': "export const ERRORS = [\n  ['../common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],\n];\n",
      }),
      [RULE],
      withData(new Set()),
    );
  const kept = run('  declare readonly cause: unknown;\n');
  assert.equal(kept.status, 0, kept.text);
  assert.ok(kept.out.includes('✅ check-data-objects: 1 item(s) checked'), kept.text);
  // The FFI id is private and a retriable error is an instanceof RetriableError.
  const gone = run('  declare readonly cause: unknown;\n  readonly code!: number;\n  readonly retriable!: boolean;\n');
  assert.equal(gone.status, 1);
  assert.deepEqual(findings(gone), [
    'FooError.code: is a readonly field of a class that is not a data class: make it a method',
    'FooError.retriable: is a readonly field of a class that is not a data class: make it a method',
  ]);
});

test('cannot run without tsconfig.json, or without the Java index', () => {
  let r = lint(project({ 'java-index.json': INDEX, 'src/a.ts': 'export class A {}\n' }), [RULE]);
  assert.deepEqual(r.err.slice(0, 2), ['', '❌ check-data-objects: cannot run, no tsconfig.json']);
  r = lint(project({ 'tsconfig.json': TSCONFIG, 'src/a.ts': 'export class A {}\n' }), [RULE]);
  assert.match(r.err[1], /^❌ check-data-objects: cannot run, no Java index/);
});

test('no property to check fails: 0 items checked', () => {
  const r = lint(project({ ...FILES, 'src/a.ts': 'export class A { m(): void {} }\n' }), [RULE], withData(new Set()));
  assert.ok(r.err.includes('❌ check-data-objects: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

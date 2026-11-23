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

// check-java-kind (scripts/lint/rules/java_kind.js).

const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { javaClass, javaIndex, method } = require('./_java_fixture');

const RULE = 'check-java-kind';

const INDEX = javaIndex([
  javaClass('clients.producer.Rec'),
  javaClass('clients.producer.Base', { kind: 'interface', overloads: [method('flush')] }),
  javaClass('clients.producer.Station', { kind: 'interface', overloads: [method('go'), method('create', [], { static: true })] }),
  javaClass('clients.producer.Mode', { kind: 'enum' }),
  javaClass('clients.producer.Point', { kind: 'record' }),
  javaClass('clients.producer.Callback', { kind: 'interface', overloads: [method('onCompletion', ['RecordMetadata'])] }),
  javaClass('clients.producer.Partitioner', { kind: 'interface' }),
  javaClass('common.header.Header', { kind: 'interface', overloads: [method('key')] }),
  javaClass('common.Fut', {
    abstract: true,
    overloads: [method('get', [], { abstract: true }), method('join'), method('Fut')],
  }),
]);

/** A translation of each class above that the rule accepts. */
const LIB = {
  'src/producer/rec.ts': '/** A record. */\nexport class Rec {}\n',
  'src/producer/base.ts': '/** A base. */\nexport interface Base {\n  flush(): void;\n}\n',
  // An interface and the constant holding its statics (`Admin.create`).
  'src/producer/station.ts':
    '/** A station. */\nexport interface Station {\n  go(): void;\n}\n' +
    '/** Its statics. */\nexport const Station = { create(): Station { return { go(): void {} }; } };\n',
  'src/producer/mode.ts': '/** A mode. */\nexport class Mode {\n  static readonly ON: Mode = new Mode();\n  private constructor() {}\n}\n',
  'src/producer/point.ts': '/** A point. */\nexport class Point {}\n',
  'src/producer/callback.ts': '/** A callback. */\nexport type Callback = (options: { metadata: unknown }) => void;\n',
  'src/producer/partitioner.ts': '/** A placeholder. */\nexport type Partitioner = unknown;\n',
  'src/common/header/header.ts': '/** A header. */\nexport class Header {\n  key(): string { return ""; }\n}\n',
  'src/common/fut.ts': `/** A future. */
export abstract class Fut<T> {
  constructor() {
    if (new.target === Fut && arguments.length === 0) {
      throw new TypeError('Fut cannot be constructed directly');
    }
  }
  abstract get(): T;
  join(): T | null { return null; }
}
`,
};

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'java-index.json': INDEX, ...LIB, ...files });

test("each export in its Java kind's form passes, the forms the JS conventions give otherwise included", () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // The nine exports, and Fut's one abstract method.
  assert.ok(r.out.includes('✅ check-java-kind: 10 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/acks.ts': "/** The acks. */\nexport enum Acks { ALL = 'all' }\n",
      'src/producer/rec.ts': '/** A record. */\nexport interface Rec {\n  x: number;\n}\n',
      'src/producer/base.ts': '/** A base. */\nexport class Base {\n  flush(): void {}\n}\n',
      'src/producer/point.ts': '/** A point. */\nexport abstract class Point {}\n',
      'src/producer/callback.ts': '/** A callback. */\nexport interface Callback {\n  onCompletion(): void;\n}\n',
      'src/producer/partitioner.ts': '/** A placeholder. */\nexport type Partitioner = { x: number };\n',
      'src/common/header/header.ts': '/** A header. */\nexport interface Header {\n  key(): string;\n}\n',
      'src/common/fut.ts': `/** A future. */
export abstract class Fut<T> {
  constructor() {
    if (new.target === Object) throw new TypeError('no');
  }
  get(): T | null { return null; }
  join(): T | null { return null; }
}
`,
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  const shape = 'javascript/CLAUDE.md, JavaScript Binding Conventions';
  assert.deepEqual(findings(r), [
    `src/producer/acks.ts:Acks: exports Acks as a TypeScript enum, which no Java type translates to: an enum is a class with frozen instances (${shape}, Class family)`,
    `src/common/fut.ts:Fut: translates the Java abstract class common.Fut, but no constructor statement throws TypeError when new.target is Fut itself: plain JavaScript can construct it (${shape}, Class family)`,
    `Fut.get: translates common.Fut#get, which is abstract: make it abstract, as Java's abstract methods stay abstract (${shape}, Class family)`,
    `src/common/header/header.ts:Header: translates the Java interface common.header.Header as an interface: ${shape}, Serialization: Header wraps the C RecordHeader too`,
    `src/producer/base.ts:Base: translates the Java interface clients.producer.Base as a class: a Java interface is a TypeScript interface (${shape}, Class family)`,
    `src/producer/callback.ts:Callback: translates the Java interface clients.producer.Callback as an interface: a callback is a function type (${shape}, Class family)`,
    `src/producer/partitioner.ts:Partitioner: translates the Java interface clients.producer.Partitioner as a type alias: a type that is not translated is a placeholder, \`type X = unknown\` (${shape}, Scope)`,
    'src/producer/point.ts:Point: is abstract, but the Java record clients.producer.Point is not: make it a class',
    `src/producer/rec.ts:Rec: translates the Java class clients.producer.Rec as an interface: a Java class is a TypeScript class (${shape}, Class family)`,
  ]);
  assert.ok(r.err.includes('❌ check-java-kind: 9 finding(s).'), r.text);
});

test("a Java abstract class translated as a plain class, and a class Java's mapping lists, are findings too", () => {
  const r = lint(
    fixture({
      'src/common/fut.ts': '/** A future. */\nexport class Fut<T> {\n  get(): T | null { return null; }\n}\n',
      'src/producer/mode.ts': '/** A mode. */\nexport interface Mode {\n  x: number;\n}\n',
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'src/common/fut.ts:Fut: is not abstract, but the Java class common.Fut is: make it an abstract class, so tsc refuses new (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)',
    'src/producer/mode.ts:Mode: translates the Java enum clients.producer.Mode as an interface: an enum is a class with frozen instances (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family)',
  ]);
});

test('an interface beside its constant passes, the constant declared first too', () => {
  const both =
    '/** Its statics. */\nexport const Station = { create(): Station { return { go(): void {} }; } };\n' +
    '/** A station. */\nexport interface Station {\n  go(): void;\n}\n';
  assert.equal(lint(fixture({ 'src/producer/station.ts': both }), [RULE]).status, 0);
});

test("a constant beside an interface whose Java type has no static method is a finding: it holds Java's statics", () => {
  const r = lint(
    fixture({
      'src/producer/base.ts':
        '/** A base. */\nexport interface Base {\n  flush(): void;\n}\n' +
        '/** Invented. */\nexport const Base = { inventedHelper(): void {} };\n',
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    "src/producer/base.ts:Base: translates the Java interface clients.producer.Base as an interface and a constant: a constant of an interface's name is the runtime object holding the Java interface's public static methods, and it has none (javascript/CLAUDE.md, JavaScript Binding Conventions, Names and Class family: `Admin.create`)",
  ]);
});

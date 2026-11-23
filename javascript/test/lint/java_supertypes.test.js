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

// check-java-supertypes (scripts/lint/rules/java_supertypes.js).

const assert = require('node:assert/strict');

const { project, lint, findings } = require('./_project');
const { javaClass, javaIndex, method, mapping } = require('./_java_fixture');

const RULE = 'check-java-supertypes';

/** The fixture's tsconfig.json: TSCONFIG's, with the lib that declares `Symbol.asyncDispose`. */
const TSCONFIG = JSON.stringify({
  compilerOptions: { strict: true, target: 'es2022', lib: ['es2022', 'esnext.disposable'], module: 'node16', rootDir: 'src', noEmit: true },
  include: ['src/**/*.ts'],
});

const P = 'org.apache.kafka.';

const INDEX = javaIndex([
  javaClass('common.KafkaException', { supertypes: ['RuntimeException'], supertype_fqns: [null] }),
  javaClass('common.errors.FooException', { supertypes: ['KafkaException'], supertype_fqns: [`${P}common.KafkaException`] }),
  javaClass('common.errors.BarException', { supertypes: ['IllegalStateException'], supertype_fqns: [null] }),
  javaClass('clients.producer.Producer', {
    kind: 'interface',
    supertypes: ['Closeable'],
    supertype_fqns: [null],
    overloads: [method('close')],
  }),
  javaClass('clients.producer.KafkaProducer', { supertypes: ['Producer'], supertype_fqns: [`${P}clients.producer.Producer`] }),
  javaClass('common.header.Headers', { kind: 'interface', supertypes: ['Iterable'], supertype_fqns: [null] }),
  javaClass('common.Uid', { supertypes: ['Comparable'], supertype_fqns: [null], overloads: [method('compareTo', ['Uid'])] }),
  javaClass('common.serialization.Serializer', { kind: 'interface', supertypes: ['Closeable'], supertype_fqns: [null] }),
  javaClass('common.config.AbstractConfig'),
  javaClass('clients.producer.Cfg', { supertypes: ['AbstractConfig'], supertype_fqns: [`${P}common.config.AbstractConfig`] }),
  javaClass('common.TopicPartition', { supertypes: ['Serializable'], supertype_fqns: [null] }),
  javaClass('clients.producer.Weird', { supertypes: ['Cloneable'], supertype_fqns: [null] }),
  // A nested supertype the index's resolution leaves null: found in the enclosing class.
  javaClass('clients.producer.Outer$Shape', { kind: 'interface' }),
  javaClass('clients.producer.Outer$Square', { supertypes: ['Shape'], supertype_fqns: [null] }),
]);

const REGISTRY = `export const ERRORS = [
  ['../common/kafka_error', 'KafkaError', 'org.apache.kafka.common.KafkaException'],
  ['../common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],
  ['../common/errors/bar_error', 'BarError', 'org.apache.kafka.common.errors.BarException'],
  ['../illegal_state_error', 'IllegalStateError', 'java.lang.IllegalStateException'],
];
`;

/** A translation of each class above that the rule accepts. */
const LIB = {
  'src/internal/error_registry.ts': REGISTRY,
  'src/common/kafka_error.ts': 'export class KafkaError extends Error {}\n',
  'src/common/errors/foo_error.ts': "import { KafkaError } from '../kafka_error';\nexport class FooError extends KafkaError {}\n",
  'src/common/errors/bar_error.ts':
    "import { IllegalStateError } from '../../illegal_state_error';\nexport class BarError extends IllegalStateError {}\n",
  'src/illegal_state_error.ts': 'export class IllegalStateError extends Error {}\n',
  'src/producer/producer.ts': 'export interface Producer {\n  close(): Promise<void>;\n  [Symbol.asyncDispose](): Promise<void>;\n}\n',
  'src/producer/kafka_producer.ts':
    "import type { Producer } from './producer';\n" +
    'export class KafkaProducer implements Producer {\n  close(): Promise<void> { return Promise.resolve(); }\n' +
    '  [Symbol.asyncDispose](): Promise<void> { return this.close(); }\n}\n',
  'src/common/header/headers.ts':
    'export class Headers implements Iterable<string> {\n  [Symbol.iterator](): Iterator<string> { return [][Symbol.iterator](); }\n}\n',
  'src/common/uid.ts': 'export class Uid {\n  compareTo(other: Uid): number { void other; return 0; }\n}\n',
  // A serde's Closeable is an optional close().
  'src/common/serialization/serializer.ts': 'export interface Serializer {\n  close?(): void;\n}\n',
  'src/producer/cfg.ts': 'export class Cfg {}\n',
  'src/common/topic_partition.ts': 'export class TopicPartition {}\n',
  'src/producer/outer.ts': 'export class Outer {}\n',
};

const fixture = (files = {}) =>
  project({
    'tsconfig.json': TSCONFIG,
    'java-index.json': javaIndex([...JSON.parse(INDEX).classes, javaClass('clients.producer.Outer')]),
    ...LIB,
    ...files,
  });

test("the translations of the Java supertypes pass: the client's types, the JDK ones by the table, the ones not translated as nothing", () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Every export but the root's IllegalStateError, whose Java class the index does not hold.
  assert.ok(r.out.includes('✅ check-java-supertypes: 11 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/common/kafka_error.ts': 'export class KafkaError {}\n',
      'src/common/errors/foo_error.ts': "import { KafkaError } from '../kafka_error';\nexport class FooError implements KafkaError {}\n",
      'src/common/errors/bar_error.ts': 'export class BarError extends Error {}\n',
      'src/producer/producer.ts': 'export interface Producer {\n  close(): void;\n}\n',
      'src/producer/kafka_producer.ts': 'export class KafkaProducer {\n  close(): Promise<void> { return Promise.resolve(); }\n}\n',
      'src/common/header/headers.ts': 'export class Headers {}\n',
      'src/common/uid.ts': 'export class Uid {}\n',
      'src/common/serialization/serializer.ts': 'export interface Serializer {\n  serialize(): void;\n}\n',
      'src/producer/cfg.ts': 'class Base {}\nexport class Cfg extends Base {}\n',
      'src/common/topic_partition.ts':
        'export class TopicPartition implements Iterable<number> {\n  [Symbol.iterator](): Iterator<number> { return [][Symbol.iterator](); }\n}\n',
      'src/producer/weird.ts': 'export class Weird {}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  const table = 'javascript/CLAUDE.md, JavaScript Binding Conventions, Class family';
  const errors = 'javascript/CLAUDE.md, JavaScript Binding Conventions, Errors';
  assert.deepEqual(findings(r), [
    `src/common/errors/bar_error.ts:BarError: does not extend IllegalStateError, the root's class of Java's IllegalStateException, a direct supertype of Java's common.errors.BarException (${errors})`,
    "src/common/errors/bar_error.ts:BarError: extends Error, which translates no direct supertype of Java's common.errors.BarException (IllegalStateException)",
    'src/common/errors/foo_error.ts:FooError: implements KafkaError, the class translating common.KafkaException: extends it',
    `src/common/header/headers.ts:Headers: has no [Symbol.iterator], for Java's Iterable (${table}: Iterable<T> becomes [Symbol.iterator]())`,
    `src/common/kafka_error.ts:KafkaError: does not extend Error, for Java's RuntimeException (${errors}: KafkaError extends Error heads the error tree)`,
    `src/common/serialization/serializer.ts:Serializer: has no close, for Java's Closeable (${table}: Closeable becomes close(): Promise<void> plus [Symbol.asyncDispose])`,
    "src/common/topic_partition.ts:TopicPartition: implements Iterable, which translates no direct supertype of Java's common.TopicPartition (Serializable)",
    `src/common/uid.ts:Uid: has no compareTo, for Java's Comparable (${table}: Comparable<T> keeps compareTo())`,
    "src/producer/cfg.ts:Cfg: extends Base, which translates no direct supertype of Java's clients.producer.Cfg (AbstractConfig)",
    "src/producer/kafka_producer.ts:KafkaProducer: does not implement Producer, which translates clients.producer.Producer, a direct supertype of Java's clients.producer.KafkaProducer",
    `src/producer/producer.ts:Producer: has a close() that does not return a Promise, for Java's Closeable (${table}: Closeable becomes close(): Promise<void> plus [Symbol.asyncDispose])`,
    `src/producer/producer.ts:Producer: has no [Symbol.asyncDispose], for Java's Closeable (${table}: Closeable becomes close(): Promise<void> plus [Symbol.asyncDispose])`,
    `src/producer/weird.ts:Weird: Java's clients.producer.Weird has the supertype Cloneable, which the JS conventions' JDK table does not translate: it needs a row (${table}, Q29)`,
  ]);
});

test('a supertype the index leaves unresolved is looked up in the classes enclosing the type', () => {
  const nested = (/** @type {string} */ square) => ({
    'src/producer/outer.ts': 'export class Outer {}\n',
    'src/producer/shape.ts': 'export interface Shape {}\n',
    'src/producer/square.ts': `import type { Shape } from './shape';\n${square}`,
    'lint-mapping.json': mapping({
      javaTargets: {
        'src/producer/shape.ts:Shape': 'org.apache.kafka.clients.producer.Outer$Shape',
        'src/producer/square.ts:Square': 'org.apache.kafka.clients.producer.Outer$Square',
      },
    }),
  });
  assert.equal(lint(fixture(nested('export class Square implements Shape {}\n')), [RULE]).status, 0);
  assert.deepEqual(findings(lint(fixture(nested('export class Square {}\nvoid (null as unknown as Shape);\n')), [RULE])), [
    "src/producer/square.ts:Square: does not implement Shape, which translates clients.producer.Outer$Shape, a direct supertype of Java's clients.producer.Outer$Square",
  ]);
});

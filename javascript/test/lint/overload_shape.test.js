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

// J6 check-overload-shape (scripts/lint/rules/overload_shape.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaIndex, method } = require('./_java_fixture');

const RULE = 'check-overload-shape';
const REC = LIB['lib/producer/rec.ts'];

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(CLASSES), ...LIB, ...files });

test('overload sets taken as one options object pass: the constructor, close({p0Ms}?)', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Every mapped member but ProducerConfig's (its constants only): Rec 11, Mode 6, Header 2, Future 1.
  assert.ok(r.out.includes('✅ check-overload-shape: 20 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'lib/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', 'constructor(options?: { p0?: string; p1: number; q?: boolean })')
        .replace('close(options?: { p0Ms?: number })', 'close(options: { timeout?: number })')
        .replace('get size(): number { return 0; }', 'get size(): number { return 0; }\n  getSize(): number { return 0; }')
        .replace('flush(): void {}', 'flush(): void {}\n  static forName(): void {}'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    '{ p0?: string | undefined; p1: number; q?: boolean | undefined; }.p0: is optional but every Java form of clients.producer.Rec#Rec takes p0',
    '{ p0?: string | undefined; p1: number; q?: boolean | undefined; }.p1: is required but Rec(String p0) does not take it',
    '{ p0?: string | undefined; p1: number; q?: boolean | undefined; }.q: no Java form of clients.producer.Rec#Rec names it (p0, p1)',
    'Rec.constructor: takes its options as optional, but every Java form of clients.producer.Rec#Rec takes a parameter',
    'Rec.getSize: and Rec.size both translate clients.producer.Rec#getSize',
    '{ timeout?: number | undefined; }.p0Ms: is missing, though clients.producer.Rec#close names it (p0Ms)',
    '{ timeout?: number | undefined; }.timeout: no Java form of clients.producer.Rec#close names it (p0Ms)',
    'Rec.close: requires its options, but clients.producer.Rec#close() takes none',
  ]);
});

test('forms differing only by trailing parameters may take them optional (decision 9); otherwise not-options', () => {
  let r = lint(fixture({ 'lib/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', 'constructor(p0: string, p1?: number)') }), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(fixture({ 'lib/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', 'constructor(p0: string, p1: number)') }), [RULE]);
  assert.deepEqual(findings(r), [
    'Rec.constructor: translates 2 Java forms of clients.producer.Rec#Rec (Rec(String p0); Rec(String p0, int p1)) but takes (p0: string, p1: number), not one options object',
  ]);
});

test('a Java form the positional parameters leave out is its own finding, keyed by the form', () => {
  const { javaClass } = require('./_java_fixture');
  const index = javaIndex([
    javaClass('common.KafkaFuture', {
      overloads: [method('get', [], { param_names: [] }), method('get', ['long', 'TimeUnit'], { param_names: ['timeout', 'unit'] })],
    }),
  ]);
  const root = (signature) =>
    project({
      'tsconfig.json': TSCONFIG,
      'build/java-index.json': index,
      'lib/common/kafka_future.ts': `export class KafkaFuture {\n  ${signature} { return Promise.resolve(); }\n}\n`,
    });
  const missing = "KafkaFuture.get/get(long,TimeUnit): common.KafkaFuture#get's form get(long timeout, TimeUnit unit) has no translation: KafkaFuture.get takes ()";
  let r = lint(root('get(): Promise<void>'), [RULE]);
  assert.deepEqual(findings(r), [missing]);
  // Baselined, the missing form waives nothing else: a positional timeout is a wrong shape.
  const baseline = { 'eslint-baseline.txt': 'overload-missing KafkaFuture.get/get(long,TimeUnit)\n' };
  r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': index, ...baseline, 'lib/common/kafka_future.ts': 'export class KafkaFuture {\n  get(): Promise<void> { return Promise.resolve(); }\n}\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'build/java-index.json': index,
      ...baseline,
      'lib/common/kafka_future.ts': 'export class KafkaFuture {\n  get(timeoutMs?: number): Promise<void> { void timeoutMs; return Promise.resolve(); }\n}\n',
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'KafkaFuture.get: translates 2 Java forms of common.KafkaFuture#get (get(); get(long timeout, TimeUnit unit)) but takes (timeoutMs?: number), not one options object',
  ]);
});

test('an option whose Java parameter may be null may be left out (decision 10); same-position parameters of one host type merge', () => {
  const javadoc = (text) => ({ text: '', params: [{ name: 'label', text }], returns: null, throws: [], words: 0 });
  const classesWith = (labelDoc) =>
    CLASSES.map((c) =>
      c.fqn.endsWith('.Rec')
        ? {
            ...c,
            overloads: c.overloads
              .filter((o) => o.name !== 'Rec')
              .concat([
                method('Rec', ['Map', 'V', 'String'], { param_names: ['configs', 'value', 'label'], javadoc: javadoc(labelDoc) }),
                method('Rec', ['Properties', 'V', 'String', 'Integer'], { param_names: ['properties', 'value', 'label', 'count'] }),
              ]),
          }
        : c,
    );
  const root = (options, labelDoc = 'the label, or null') =>
    project({
      'tsconfig.json': TSCONFIG,
      'build/java-index.json': javaIndex(classesWith(labelDoc)),
      ...LIB,
      'lib/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', `constructor(options: ${options})`),
    });
  const OPTIONS = '{ configs: object; value?: unknown; label?: string; count?: number }';
  // value (V) and label (its Javadoc says null) may be left out, though every form takes them;
  // Map and Properties, at one position, merge as configs.
  let r = lint(root(OPTIONS), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Without the Javadoc's null, label may not.
  r = lint(root(OPTIONS, 'the label'), [RULE]);
  assert.deepEqual(findings(r), [
    '{ configs: object; value?: unknown; label?: string | undefined; count?: number | undefined; }.label: is optional but every Java form of clients.producer.Rec#Rec takes label',
  ]);
  r = lint(root('{ configs: object; properties?: object; value?: unknown; label?: string; count?: number }'), [RULE]);
  assert.deepEqual(findings(r), [
    '{ configs: object; properties?: object | undefined; value?: unknown; label?: string | undefined; count?: number | undefined; }.properties: no Java form of clients.producer.Rec#Rec names it (configs, value, label, count)',
  ]);
});

test("ProducerRecord's six forms: K and V keep their own kinds, so key and value stay apart", () => {
  const { javaClass } = require('./_java_fixture');
  const forms = [
    [['String', 'Integer', 'Long', 'K', 'V', 'Iterable'], ['topic', 'partition', 'timestamp', 'key', 'value', 'headers']],
    [['String', 'Integer', 'Long', 'K', 'V'], ['topic', 'partition', 'timestamp', 'key', 'value']],
    [['String', 'Integer', 'K', 'V', 'Iterable'], ['topic', 'partition', 'key', 'value', 'headers']],
    [['String', 'Integer', 'K', 'V'], ['topic', 'partition', 'key', 'value']],
    [['String', 'K', 'V'], ['topic', 'key', 'value']],
    [['String', 'V'], ['topic', 'value']],
  ];
  const index = javaIndex([
    javaClass('clients.producer.ProducerRecord', {
      overloads: forms.map(([params, names]) => method('ProducerRecord', params, { param_names: names })),
    }),
  ]);
  const root = (options) =>
    project({
      'tsconfig.json': TSCONFIG,
      'build/java-index.json': index,
      'lib/producer/producer_record.ts': `export class ProducerRecord {\n  constructor(options: ${options}) { void options; }\n}\n`,
    });
  const T = (o) => `{ ${o}; }`;
  const rest = 'partition?: number; timestamp?: number; headers?: unknown[]';
  // topic required (every form, a String); value in every form but a V, so it may be left out;
  // key optional (not in (topic, value)); partition, timestamp and headers optional.
  for (const value of ['value?: unknown', 'value: unknown']) {
    const r = lint(root(T(`topic: string; key?: unknown; ${value}; ${rest}`)), [RULE]);
    assert.equal(r.status, 0, r.text);
  }
  let r = lint(root(T(`topic: string; key: unknown; value?: unknown; ${rest}`)), [RULE]);
  assert.deepEqual(findings(r), [
    '{ topic: string; key: unknown; value?: unknown; partition?: number | undefined; timestamp?: number | undefined; headers?: unknown[] | undefined; }.key: is required but ProducerRecord(String topic, V value) does not take it',
  ]);
  r = lint(root(T(`topic?: string; key?: unknown; value?: unknown; ${rest}`)), [RULE]);
  assert.deepEqual(findings(r), [
    '{ topic?: string | undefined; key?: unknown; value?: unknown; partition?: number | undefined; timestamp?: number | undefined; headers?: unknown[] | undefined; }.topic: is optional but every Java form of clients.producer.ProducerRecord#ProducerRecord takes topic',
  ]);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-overload-shape: cannot run, no Java index/);
});

test('nothing mapped fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(CLASSES), 'lib/a.ts': 'const a = 1;\nvoid a;\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-overload-shape: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('decision 10 reads a positive nullability statement only', () => {
  const { mayBeNull } = require('../../scripts/lint/rules/overload_shape');
  const form = (type, text) => ({ params: [type], param_names: ['x'], javadoc: { params: [{ name: 'x', text }] } });
  // The seven parameters every form takes that Java documents as non-null.
  for (const text of [
    'the non-null config name',
    'The name of the metric; may not be null',
    'The name of the group; may not be null',
    'The description of the metric; may not be null',
    'the set of metric tag names, which can/should be a set that maintains order; may not be null',
    'the definition of the configurations; may not be null',
    'the name-value pairs of the configuration; may not be null',
  ]) {
    assert.equal(mayBeNull(form('String', text), 0), false, text);
  }
  for (const text of ['the key; may be null', 'can be null', 'If null, all topics', 'null means the default', 'the label, or null', 'a nullable name']) {
    assert.equal(mayBeNull(form('String', text), 0), true, text);
  }
  assert.equal(mayBeNull(form('String', 'the label; must not be null, or null in tests'), 0), false);
  // The Javadoc as rendered: {@code null} is null.
  for (const text of ['the data; may be {@code null}', 'a callback (null indicates no callback)', 'the label, or {@code\n   null}']) {
    assert.equal(mayBeNull(form('String', text), 0), true, text);
  }
  assert.equal(mayBeNull(form('String', 'the name; must not be {@code null}'), 0), false);
  assert.equal(mayBeNull(form('Integer', 'the partition'), 0), true);
  assert.equal(mayBeNull(form('V', 'the value'), 0), true);
  assert.equal(mayBeNull(form('String', 'the topic'), 0), false);
});

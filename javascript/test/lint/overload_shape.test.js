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
const REC = LIB['src/producer/rec.ts'];

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(CLASSES), ...LIB, ...files });

/** The fixture's Rec with one overload signature per Java form: Rec(p0), Rec(p0, p1); close(), close(p0). */
const REC_PER_FORM = REC.replace(
  'constructor(options: { p0: string; p1?: number });',
  'constructor(options: { p0: string; p1?: never });\n  constructor(options: { p0: string; p1: number });',
).replace('close(options?: { p0?: number }): void;', 'close(): void;\n  close(options: { p0: number }): void;');

/** The findings but `signature-per-form`'s, which its own test covers: the tests of one options object's names. */
const shapeFindings = (r) => findings(r).filter((f) => !f.includes('with one options signature: give each form its own overload signature'));

test('overload sets taken as one options object, one signature per form, and the bare value of a one-parameter form, pass: the constructor, close(), close({p0}), close(p0)', () => {
  const r = lint(fixture({ 'src/producer/rec.ts': REC_PER_FORM }), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Every mapped member but ProducerConfig's (its constants only): Rec 13, Mode 6, Header 2, Future 1.
  assert.ok(r.out.includes('✅ check-overload-shape: 24 item(s) checked'), r.text);
});

test('one options signature for several Java forms is signature-per-form: each form has its own overload signature', () => {
  // The fixture's Rec: one options signature for Rec(p0) and Rec(p0, p1), and
  // one, optional, for close() and close(p0).
  const r = lint(fixture(), [RULE]);
  assert.deepEqual(findings(r), [
    'Rec.constructor: translates 2 Java forms of clients.producer.Rec#Rec (Rec(String p0); Rec(String p0, int p1)) with one options signature: give each form its own overload signature',
    'Rec.close: translates 2 Java forms of clients.producer.Rec#close (close(); close(Duration p0)) with one options signature: give each form its own overload signature',
  ]);
  // Forms whose parameters have the same names are one: Java's Map and Properties constructors.
  const { javaClass } = require('./_java_fixture');
  const index = javaIndex([
    javaClass('clients.producer.KafkaProducer', {
      overloads: [method('KafkaProducer', ['Map<String, Object>'], { param_names: ['configs'] }), method('KafkaProducer', ['Properties'], { param_names: ['properties'] })],
    }),
  ]);
  const one = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': index,
      'src/producer/kafka_producer.ts':
        'export class KafkaProducer {\n  constructor(options: { configs: object });\n  constructor(configs: object);\n  constructor(options: unknown) { void options; }\n}\n',
    }),
    [RULE],
  );
  assert.equal(one.status, 0, one.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', 'constructor(options?: { p0?: string; p1: number; q?: boolean })')
        .replace('close(options?: { p0?: number })', 'close(options: { p0?: number; timeout?: number })')
        .replace('get size(): number { return 0; }', 'get size(): number { return 0; }\n  getSize(): number { return 0; }')
        .replace('flush(): void {}', 'flush(): void {}\n  static forName(): void {}'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(shapeFindings(r), [
    '{ p0?: string | undefined; p1: number; q?: boolean | undefined; }.p0: is optional but every Java form of clients.producer.Rec#Rec takes p0',
    '{ p0?: string | undefined; p1: number; q?: boolean | undefined; }.p1: is required but Rec(String p0) does not take it',
    '{ p0?: string | undefined; p1: number; q?: boolean | undefined; }.q: no Java form of clients.producer.Rec#Rec names it (p0, p1)',
    'Rec.constructor: takes its options as optional, but every Java form of clients.producer.Rec#Rec takes a parameter',
    'Rec.getSize: and Rec.size both translate clients.producer.Rec#getSize',
    '{ p0?: number | undefined; timeout?: number | undefined; }.timeout: no Java form of clients.producer.Rec#close names it (p0)',
    'Rec.close: requires its options, but clients.producer.Rec#close() takes none',
  ]);
});

test('positional parameters are not-options, even for forms differing only by trailing parameters', () => {
  for (const signature of ['constructor(p0: string, p1?: number)', 'constructor(p0: string, p1: number)']) {
    const r = lint(fixture({ 'src/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', signature) }), [RULE]);
    assert.deepEqual(shapeFindings(r), [
      `Rec.constructor: translates 2 Java forms of clients.producer.Rec#Rec (Rec(String p0); Rec(String p0, int p1)) but takes (${signature.slice(12, -1)}), not one options object`,
    ]);
  }
  // A bare value with no options object is no options object either.
  const r = lint(fixture({ 'src/producer/rec.ts': REC.replace('  constructor(options: { p0: string; p1?: number });\n', '') }), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    'Rec.constructor: translates 2 Java forms of clients.producer.Rec#Rec (Rec(String p0); Rec(String p0, int p1)) but takes (p0: string), not one options object',
  ]);
});

test('a one-parameter form needs its bare value, a signature taking that parameter by its name', () => {
  let r = lint(fixture({ 'src/producer/rec.ts': REC.replace('  close(p0: number): void;\n', '') }), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    "Rec.close: takes no bare value, though clients.producer.Rec#close's form close(Duration p0) has one parameter: add a signature taking p0 on its own",
  ]);
  // Named otherwise, it is a positional parameter.
  r = lint(fixture({ 'src/producer/rec.ts': REC.replace('  close(p0: number): void;\n', '  close(timeout: number): void;\n') }), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    'Rec.close: translates 2 Java forms of clients.producer.Rec#close (close(); close(Duration p0)) but takes (timeout: number), not one options object',
  ]);
});

test("one signature per Java form: each names exactly its form's parameters", () => {
  const { javaClass } = require('./_java_fixture');
  const index = javaIndex([
    javaClass('clients.producer.ProducerRecord', {
      overloads: [
        method('ProducerRecord', ['String', 'Integer', 'K', 'V'], { param_names: ['topic', 'partition', 'key', 'value'] }),
        method('ProducerRecord', ['String', 'K', 'V'], { param_names: ['topic', 'key', 'value'] }),
        method('ProducerRecord', ['String', 'V'], { param_names: ['topic', 'value'] }),
      ],
    }),
  ]);
  const root = (signatures) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': index,
      'src/producer/producer_record.ts': `export class ProducerRecord {\n${signatures.map((t) => `  constructor(options: ${t});\n`).join('')}  constructor(options: unknown) { void options; }\n}\n`,
    });
  const FOUR = '{ topic: string; partition: number | null; key: unknown; value: unknown }';
  const THREE = '{ topic: string; partition?: never; key: unknown; value: unknown }';
  const TWO = '{ topic: string; partition?: never; key?: never; value: unknown }';
  let r = lint(root([FOUR, THREE, TWO]), [RULE]);
  assert.deepEqual(shapeFindings(r), [], r.text);
  // A form without a signature; a signature of no form; an optional parameter.
  r = lint(root([FOUR, '{ topic: string; partition: number | null; key?: never; value: unknown }', '{ topic: string; partition?: never; key?: unknown; value: unknown }']), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    'ProducerRecord.constructor/(topic,partition,value): takes (topic, partition, value), which is no Java form of clients.producer.ProducerRecord#ProducerRecord',
    '{ topic: string; partition?: undefined; key?: unknown; value: unknown; }.key: is optional, but a signature of one Java form of clients.producer.ProducerRecord#ProducerRecord takes its form\'s parameters',
    "ProducerRecord.constructor/ProducerRecord(String,K,V): clients.producer.ProducerRecord#ProducerRecord's form ProducerRecord(String topic, K key, V value) has no translation: ProducerRecord.constructor takes (options: { topic: string; partition: number | null; key: unknown; value: unknown })",
  ]);
  // A name no form has.
  r = lint(root([FOUR, THREE, '{ topic: string; partition?: never; key?: never; value: unknown; offset?: never }']), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ topic: string; partition?: undefined; key?: undefined; value: unknown; offset?: undefined; }.offset: no Java form of clients.producer.ProducerRecord#ProducerRecord names it (topic, partition, key, value)',
  ]);
  // A name another form takes, neither taken nor closed: a value holding it
  // would match this signature.
  r = lint(root([FOUR, THREE, '{ topic: string; partition?: never; value: unknown }']), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ topic: string; partition?: undefined; value: unknown; }.key: neither takes nor closes key (`key?: never`), which another Java form of clients.producer.ProducerRecord#ProducerRecord takes',
  ]);
});

test('a Java form the options object does not name is its own finding, keyed by the form', () => {
  const { javaClass } = require('./_java_fixture');
  const index = javaIndex([
    javaClass('clients.producer.KafkaProducer', {
      overloads: [
        method('KafkaProducer', ['Map<String, Object>'], { param_names: ['configs'] }),
        method('KafkaProducer', ['Map<String, Object>', 'Serializer<K>', 'Serializer<V>'], { param_names: ['configs', 'keySerializer', 'valueSerializer'] }),
        method('KafkaProducer', ['Properties'], { param_names: ['properties'] }),
      ],
    }),
  ]);
  const root = (options) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': index,
      'src/producer/kafka_producer.ts': `export class KafkaProducer {\n  constructor(options: ${options});\n  constructor(configs: object);\n  constructor(options: unknown) { void options; }\n}\n`,
    });
  const missing =
    "KafkaProducer.constructor/KafkaProducer(Map,Serializer,Serializer): clients.producer.KafkaProducer#KafkaProducer's form " +
    'KafkaProducer(Map<String, Object> configs, Serializer<K> keySerializer, Serializer<V> valueSerializer) has no translation: ' +
    'KafkaProducer.constructor takes (options: { configs: object })';
  // The Map and Properties forms are one configs; the serializer form is not translated.
  let r = lint(root('{ configs: object }'), [RULE]);
  assert.deepEqual(shapeFindings(r), [missing]);
  // Checked against the forms it translates: configs, in each, is required.
  r = lint(root('{ configs?: object }'), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    missing.replace('{ configs: object }', '{ configs?: object }'),
    '{ configs?: object | undefined; }.configs: is optional but every Java form of clients.producer.KafkaProducer#KafkaProducer takes configs',
  ]);
  // Every form named: nothing is missing, and the serializers are optional.
  r = lint(root('{ configs: object; keySerializer?: unknown; valueSerializer?: unknown }'), [RULE]);
  assert.deepEqual(shapeFindings(r), [], r.text);
  // No form translated: the options are checked against every form.
  r = lint(root('{ config: object }'), [RULE]);
  const union = '(configs, keySerializer, valueSerializer)';
  assert.deepEqual(shapeFindings(r), [
    `{ config: object; }.configs: is missing, though clients.producer.KafkaProducer#KafkaProducer names it ${union}`,
    `{ config: object; }.keySerializer: is missing, though clients.producer.KafkaProducer#KafkaProducer names it ${union}`,
    `{ config: object; }.valueSerializer: is missing, though clients.producer.KafkaProducer#KafkaProducer names it ${union}`,
    `{ config: object; }.config: no Java form of clients.producer.KafkaProducer#KafkaProducer names it ${union}`,
  ]);
});

test('a Java form a function taking no parameter leaves out is its own finding, keyed by the form', () => {
  const { javaClass } = require('./_java_fixture');
  const index = javaIndex([
    javaClass('common.KafkaFuture', {
      overloads: [method('get', [], { param_names: [] }), method('get', ['long', 'TimeUnit'], { param_names: ['timeout', 'unit'] })],
    }),
  ]);
  const root = (signature) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': index,
      'src/common/kafka_future.ts': `export class KafkaFuture {\n  ${signature} { return Promise.resolve(); }\n}\n`,
    });
  const missing = "KafkaFuture.get/get(long,TimeUnit): common.KafkaFuture#get's form get(long timeout, TimeUnit unit) has no translation: KafkaFuture.get takes ()";
  let r = lint(root('get(): Promise<void>'), [RULE]);
  assert.deepEqual(shapeFindings(r), [missing]);
  // Baselined, the missing form waives nothing else: a positional timeout is a
  // wrong shape, of the whole set (so the baselined form is no longer the
  // finding, and its line goes stale).
  const baseline = { 'eslint-baseline.txt': 'overload-missing KafkaFuture.get/get(long,TimeUnit)\n' };
  r = lint(project({ 'tsconfig.json': TSCONFIG, 'java-index.json': index, ...baseline, 'src/common/kafka_future.ts': 'export class KafkaFuture {\n  get(): Promise<void> { return Promise.resolve(); }\n}\n' }), [RULE]);
  assert.deepEqual(shapeFindings(r), [], r.text);
  r = lint(
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': index,
      ...baseline,
      'src/common/kafka_future.ts': 'export class KafkaFuture {\n  get(timeoutMs?: number): Promise<void> { void timeoutMs; return Promise.resolve(); }\n}\n',
    }),
    [RULE],
  );
  assert.deepEqual(shapeFindings(r), [
    'KafkaFuture.get: translates 2 Java forms of common.KafkaFuture#get (get(); get(long timeout, TimeUnit unit)) but takes (timeoutMs?: number), not one options object',
    'eslint-baseline.txt: overload-missing KafkaFuture.get/get(long,TimeUnit): stale entry: no finding matches it any more; delete the line',
  ]);
  // Translated: the TimeUnit is merged with the long it qualifies into one
  // `timeout`, an option of get() and get's bare value.
  r = lint(
    root('get(options?: { timeout?: number }): Promise<void>;\n  get(timeout: number): Promise<void>;\n  get(options?: unknown): Promise<void>'),
    [RULE],
  );
  assert.deepEqual(shapeFindings(r), [], r.text);
  // Not merged into a `unit` of its own.
  r = lint(
    root('get(options?: { timeout?: number; unit?: string }): Promise<void>;\n  get(timeout: number): Promise<void>;\n  get(options?: unknown): Promise<void>'),
    [RULE],
  );
  assert.deepEqual(shapeFindings(r), [
    '{ timeout?: number | undefined; unit?: string | undefined; }.unit: no Java form of common.KafkaFuture#get names it (timeout)',
  ]);
});

test('an option every form takes is required, even one Java may take as null; same-position parameters of one host type merge', () => {
  const classesWith = () =>
    CLASSES.map((c) =>
      c.fqn.endsWith('.Rec')
        ? {
            ...c,
            overloads: c.overloads
              .filter((o) => o.name !== 'Rec')
              .concat([
                method('Rec', ['Map', 'V', 'String'], { param_names: ['configs', 'value', 'label'] }),
                method('Rec', ['Properties', 'V', 'String', 'Integer'], { param_names: ['properties', 'value', 'label', 'count'] }),
              ]),
          }
        : c,
    );
  const root = (options) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex(classesWith()),
      ...LIB,
      // The forms have no one parameter: no bare value.
      'src/producer/rec.ts': REC.replace('constructor(options: { p0: string; p1?: number })', `constructor(options: ${options})`).replace(
        '  constructor(p0: string);\n',
        '',
      ),
    });
  // Map and Properties, at one position, merge as configs; value (a V, which
  // Java may take as null) and label are in every form, so given (as null).
  let r = lint(root('{ configs: object; value: unknown; label: string | null; count?: number }'), [RULE]);
  assert.deepEqual(shapeFindings(r), [], r.text);
  // A form is given whole: a nullable value is no reason to leave it out.
  r = lint(root('{ configs: object; value?: unknown; label: string | null; count?: number }'), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ configs: object; value?: unknown; label: string | null; count?: number | undefined; }.value: is optional but every Java form of clients.producer.Rec#Rec takes value',
  ]);
  r = lint(root('{ configs: object; properties?: object; value: unknown; label: string; count?: number }'), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ configs: object; properties?: object | undefined; value: unknown; label: string; count?: number | undefined; }.properties: no Java form of clients.producer.Rec#Rec names it (configs, value, label, count)',
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
      'java-index.json': index,
      'src/producer/producer_record.ts': `export class ProducerRecord {\n  constructor(options: ${options}) { void options; }\n}\n`,
    });
  const T = (o) => `{ ${o}; }`;
  const rest = 'partition?: number; timestamp?: number; headers?: unknown[]';
  // topic and value required (every form takes them); key optional (not in
  // (topic, value)); partition, timestamp and headers optional.
  let r = lint(root(T(`topic: string; key?: unknown; value: unknown; ${rest}`)), [RULE]);
  assert.deepEqual(shapeFindings(r), [], r.text);
  r = lint(root(T(`topic: string; key?: unknown; value?: unknown; ${rest}`)), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ topic: string; key?: unknown; value?: unknown; partition?: number | undefined; timestamp?: number | undefined; headers?: unknown[] | undefined; }.value: is optional but every Java form of clients.producer.ProducerRecord#ProducerRecord takes value',
  ]);
  r = lint(root(T(`topic: string; key: unknown; value: unknown; ${rest}`)), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ topic: string; key: unknown; value: unknown; partition?: number | undefined; timestamp?: number | undefined; headers?: unknown[] | undefined; }.key: is required but ProducerRecord(String topic, V value) does not take it',
  ]);
  r = lint(root(T(`topic?: string; key?: unknown; value: unknown; ${rest}`)), [RULE]);
  assert.deepEqual(shapeFindings(r), [
    '{ topic?: string | undefined; key?: unknown; value: unknown; partition?: number | undefined; timestamp?: number | undefined; headers?: unknown[] | undefined; }.topic: is optional but every Java form of clients.producer.ProducerRecord#ProducerRecord takes topic',
  ]);
});

test("Java's long is a kind of its own, bigint: a long and an int at one position stay two options", () => {
  const { formOptionNames } = require('../../scripts/lint/rules/overload_shape');
  const form = (params, names) => ({ params, param_names: names });
  // seek(long offset) / seek(int index): a bigint and a number, two names.
  assert.deepEqual(formOptionNames([form(['long'], ['offset']), form(['int'], ['index'])]), [['offset'], ['index']]);
  // Two longs at one position are one option, named after the first form.
  assert.deepEqual(formOptionNames([form(['long'], ['offset']), form(['Long'], ['position'])]), [['offset'], ['offset']]);
  // An int and a double are both numbers: one option.
  assert.deepEqual(formOptionNames([form(['int'], ['n']), form(['double'], ['x'])]), [['n'], ['n']]);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-overload-shape: cannot run, no Java index/);
});

test('nothing mapped fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(CLASSES), 'src/a.ts': 'const a = 1;\nvoid a;\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-overload-shape: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

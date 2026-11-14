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

// J7 check-type-mapping (scripts/lint/rules/type_mapping.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, javaIndex, method, field } = require('./_java_fixture');
const { parseJava } = require('../../scripts/lint/rules/type_mapping');
const { FILES } = require('./_native_fixture');

const RULE = 'check-type-mapping';

/** A Java class with one member of each row of the table, and its translation. */
const TYPED = javaClass('clients.producer.Typed', {
  overloads: [
    method('count', [], { return_type: 'long' }),
    method('ready', [], { return_type: 'Boolean' }),
    method('label', [], { return_type: 'String' }),
    method('key', [], { return_type: 'K' }),
    method('bytes', [], { return_type: 'byte[]' }),
    method('future', [], { return_type: 'Future<Rec>' }),
    method('names', [], { return_type: 'List<String>' }),
    method('modes', [], { return_type: 'Mode[]' }),
    method('wait', ['Duration'], { return_type: 'void', param_names: ['timeout'] }),
    method('fetch', [], { return_type: 'Object' }),
    method('flush', [], { return_type: 'void' }),
    method('pause', ['Duration'], { return_type: 'void', param_names: ['timeout'] }),
    method('pick', ['String'], { return_type: 'void', param_names: ['x'] }),
    method('pick', ['int'], { return_type: 'void', param_names: ['x'] }),
    method('widget', [], { return_type: 'Widget' }),
    method('seek', ['long'], { return_type: 'void', param_names: ['offset'] }),
  ],
  fields: [field('ON', { type: 'Mode' })],
});
const PAUSE =
  "  pause(timeout: number | { readonly [Symbol.toStringTag]: 'Temporal.Duration'; total(options: { unit: 'millisecond' }): number }): void { void timeout; }";
const TYPED_TS = `import { Rec } from './rec';
import { Mode } from './mode';
import { KafkaFuture } from '../common/kafka_future';
declare class Buffer extends Uint8Array {}
export class Typed<K = never> {
  static readonly ON: Mode = Mode.ON;
  get count(): bigint { return 1n; }
  get ready(): boolean | null { return null; }
  get label(): string { return ''; }
  get key(): K | null { return null; }
  get bytes(): Buffer { return new Uint8Array() as Buffer; }
  future(): KafkaFuture<Rec> { return KafkaFuture.of(); }
  names(): string[] { return []; }
  modes(): readonly Mode[] { return []; }
  wait(timeout: number): void { void timeout; }
  fetch(): unknown { return null; }
  flush(): void {}
${PAUSE}
  pick(options: { x: string | number }): void { void options; }
  seek(offset: bigint | number): void { void offset; }
}
`;
const FUTURE_TS =
  'export class KafkaFuture<T> {\n  private constructor(private readonly v: T) {}\n' +
  '  static of<T>(): KafkaFuture<T> { return null as unknown as KafkaFuture<T>; }\n' +
  '  get(): Promise<T> { return Promise.resolve(this.v); }\n}\n';
const classes = [...CLASSES, TYPED, javaClass('common.KafkaFuture', { abstract: true, overloads: [method('get', [], { return_type: 'T' })] })];
const fixture = (files = {}) =>
  project({
    'tsconfig.json': TSCONFIG,
    'java-index.json': javaIndex(classes),
    ...FILES,
    ...LIB,
    'src/producer/typed.ts': TYPED_TS,
    'src/common/kafka_future.ts': FUTURE_TS,
    ...files,
  });

test('parseJava reads a Java type as written', () => {
  assert.deepEqual(parseJava('Map<String, ? extends List<T>>[]'), {
    name: 'Map',
    args: [
      { name: 'String', args: [], dims: 0 },
      { name: 'List', args: [{ name: 'T', args: [], dims: 0 }], dims: 0 },
    ],
    dims: 1,
  });
  assert.deepEqual(parseJava('java.util.Optional<Long>'), { name: 'Optional', args: [{ name: 'Long', args: [], dims: 0 }], dims: 0 });
});

test('every row of the table maps: numbers, booleans, strings, a type variable, KafkaFuture, arrays, Duration, void and Promise', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('Java byte[] is bytes only: a string, or bytes or a string, is a type-mismatch; the client returns a Buffer', () => {
  for (const [t, shown] of [
    ['string', 'string'],
    ['Buffer | string', 'string | Buffer'],
    // Returned (a return, a field), byte[] is a Buffer, not any Uint8Array.
    ['Uint8Array', 'Uint8Array<ArrayBufferLike>'],
  ]) {
    const r = lint(fixture({ 'src/producer/typed.ts': TYPED_TS.replace('get bytes(): Buffer {', `get bytes(): ${t} {`) }), [RULE]);
    assert.deepEqual(findings(r), [`Typed.bytes: type: Java byte[] is not TypeScript ${shown}`]);
  }
});

test("Java's long is a bigint returned, and a bigint or a number taken", () => {
  for (const [from, to, finding] of [
    ['get count(): bigint { return 1n; }', 'get count(): number { return 1; }', 'Typed.count: type: Java long is not TypeScript number'],
    ['get count(): bigint { return 1n; }', 'get count(): bigint | number { return 1; }', 'Typed.count: type: Java long is not TypeScript number | bigint'],
    ['seek(offset: bigint | number)', 'seek(offset: number)', 'Typed.seek: parameter offset: Java long is not TypeScript number'],
    ['seek(offset: bigint | number)', 'seek(offset: bigint)', 'Typed.seek: parameter offset: Java long is not TypeScript bigint'],
  ]) {
    const r = lint(fixture({ 'src/producer/typed.ts': TYPED_TS.replace(from, to) }), [RULE]);
    assert.deepEqual(findings(r), [finding], to);
  }
});

test('a type off the table is a type-mismatch; a Java type with no row is unmapped', () => {
  const r = lint(
    fixture({
      'src/producer/typed.ts': TYPED_TS.replace('get count(): bigint { return 1n; }', 'get count(): string { return ""; }')
        .replace('future(): KafkaFuture<Rec>', 'future(): KafkaFuture<string>')
        .replace('wait(timeout: number): void { void timeout; }', 'wait(timeout: string): void { void timeout; }')
        .replace('flush(): void {}', 'widget(): unknown { return null; }\n  flush(): Promise<void> { return Promise.resolve(); }')
        .replace(PAUSE, '  pause(timeoutMs: number): void { void timeoutMs; }')
        .replace('pick(options: { x: string | number })', 'pick(options: { x: string })'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'Typed.count: type: Java long is not TypeScript string',
    'Typed.future: return: Java Future<Rec> is not TypeScript KafkaFuture<string>',
    'Typed.wait: parameter timeout: Java Duration is not TypeScript string',
    'Typed.widget: return: Java Widget has no row in the table (TypeScript unknown)',
    'Typed.flush: return: Java void is not TypeScript Promise<void>',
    'Typed.pause: parameter timeoutMs: it translates Java\'s Duration timeout, so it is named timeout',
    'Typed.pick: option x: Java String | int is not TypeScript string',
  ]);
});

test('a Duration is a number of milliseconds or a Temporal.Duration, nothing else', () => {
  const pause = (type) => TYPED_TS.replace(PAUSE, `  pause(timeout: ${type}): void { void timeout; }`);
  // A Temporal.Duration alone, without a number, is not Java's Duration as the client takes it.
  let r = lint(fixture({ 'src/producer/typed.ts': pause('{ total(options: { unit: "millisecond" }): number }') }), [RULE]);
  assert.deepEqual(findings(r), ['Typed.pause: parameter timeout: Java Duration is not TypeScript { total(options: { unit: "millisecond"; }): number; }']);
  r = lint(fixture({ 'src/producer/typed.ts': pause('number | { other(): void }') }), [RULE]);
  assert.deepEqual(findings(r), ['Typed.pause: parameter timeout: Java Duration is not TypeScript number | { other(): void; }']);
});

test('a union holding the translated class and more is a mismatch', () => {
  const r = lint(fixture({ 'src/producer/typed.ts': TYPED_TS.replace('future(): KafkaFuture<Rec>', 'future(): KafkaFuture<Rec | string>') }), [RULE]);
  assert.deepEqual(findings(r), ['Typed.future: return: Java Future<Rec> is not TypeScript KafkaFuture<string | Rec>']);
});

test("KafkaFuture's rows: CompletionStage, its nested function types, Throwable, a nullable or narrowed T, ? and get(long, TimeUnit)", () => {
  const futureClass = javaClass('common.KafkaFuture', {
    abstract: true,
    overloads: [
      method('get', [], { return_type: 'T' }),
      method('get', ['long', 'TimeUnit'], { return_type: 'T', param_names: ['timeout', 'unit'] }),
      method('toCompletionStage', [], { return_type: 'CompletionStage<T>' }),
      // As the real index has them: the erased type, and the type as written.
      method('thenApply', ['BaseFunction'], { param_types: ['BaseFunction<T, R>'], return_type: 'KafkaFuture<R>', param_names: ['function'] }),
      method('whenComplete', ['BiConsumer'], {
        param_types: ['BiConsumer<? super T, ? super Throwable>'],
        return_type: 'KafkaFuture<T>',
        param_names: ['action'],
      }),
      method('getNow', ['T'], { return_type: 'T', param_names: ['valueIfAbsent'] }),
      // A bare form with the type-level check, `...check`, which is no Java parameter.
      method('completedFuture', ['U'], { return_type: 'KafkaFuture<U>', param_names: ['value'], static: true }),
      method('allOf', ['KafkaFuture[]'], { param_types: ['KafkaFuture<?>[]'], return_type: 'KafkaFuture<Void>', param_names: ['futures'], static: true }),
    ],
  });
  const DURATION = "number | { readonly [Symbol.toStringTag]: 'Temporal.Duration'; total(options: { unit: 'millisecond' }): number }";
  const ts = (edit = (x) => x) =>
    edit(
      'type BareValue = string | number | null;\n' +
        'type BareCheck<U> = [U] extends [BareValue] ? [] : [never];\n' +
        'export class KafkaFuture<T> {\n  private constructor(private readonly v: T) {}\n' +
        `  get(options?: { timeout?: ${DURATION} }): Promise<T> { void options; return Promise.resolve(this.v); }\n` +
        '  toCompletionStage(): Promise<T> { return Promise.resolve(this.v); }\n' +
        '  thenApply<R>(fn: KafkaFuture.BaseFunction<T, R>): KafkaFuture<R> { void fn; return null as unknown as KafkaFuture<R>; }\n' +
        '  whenComplete(action: KafkaFuture.BiConsumer<T | null, unknown>): KafkaFuture<T> { void action; return this; }\n' +
        '  getNow(valueIfAbsent: Extract<T | null, BareValue>): T | null { return valueIfAbsent; }\n' +
        '  static completedFuture<U>(value: U, ...check: BareCheck<U>): KafkaFuture<U> { void [value, check]; return null as unknown as KafkaFuture<U>; }\n' +
        '  static allOf(futures: ReadonlyArray<KafkaFuture<unknown>>): KafkaFuture<void> { void futures; return null as unknown as KafkaFuture<void>; }\n}\n' +
        'export declare namespace KafkaFuture {\n  export type BaseFunction<A, B> = (a: A) => B;\n  export type BiConsumer<A, B> = (a: A, b: B) => void;\n}\n',
    );
  const files = (source) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex([...CLASSES, TYPED, futureClass]),
      ...FILES,
      ...LIB,
      'src/producer/typed.ts': TYPED_TS,
      'src/common/kafka_future.ts': source,
    });
  let r = lint(files(ts()), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(
    files(
      ts((x) =>
        x
          .replace('toCompletionStage(): Promise<T>', 'toCompletionStage(): Promise<string>')
          .replace('fn: KafkaFuture.BaseFunction<T, R>', 'fn: (a: T) => R')
          .replace('BiConsumer<T | null, unknown>', 'BiConsumer<T | null, string>')
          .replace(`timeout?: ${DURATION}`, 'timeout?: string')
          .replace('ReadonlyArray<KafkaFuture<unknown>>', 'ReadonlyArray<string>')
          .replace('Extract<T | null, BareValue>', 'Extract<string | null, BareValue>')
          .replace('completedFuture<U>(value: U,', 'completedFuture<U>(value: U & string,'),
      ),
    ),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    'KafkaFuture.get: option timeout: Java Duration is not TypeScript string | undefined',
    'KafkaFuture.toCompletionStage: return: Java CompletionStage<T> is not TypeScript Promise<string>',
    'KafkaFuture.thenApply: parameter fn: Java BaseFunction<T, R> is not TypeScript (a: T) => R',
    'KafkaFuture.whenComplete: parameter action: Java BiConsumer<T, Throwable> is not TypeScript BiConsumer<T | null, string>',
    'KafkaFuture.getNow: parameter valueIfAbsent: Java T is not TypeScript string | null',
    'KafkaFuture.completedFuture: parameter value: Java U is not TypeScript U & string',
    'KafkaFuture.allOf: parameter futures: Java KafkaFuture<?>[] is not TypeScript readonly string[]',
  ]);
});

test('a Java parameter named check is compared, as any other: only a BareCheck rest parameter is skipped', () => {
  // Java's tags(String[] check) and label(String prefix, String[] check),
  // translated as rest parameters: of the right type they pass, of the wrong
  // one they are a mismatch, whatever the name.
  const cls = (name) =>
    javaClass('clients.producer.Tagged', {
      overloads: [
        method('tags', ['String[]'], { return_type: 'void', param_names: [name] }),
        method('label', ['String', 'String[]'], { return_type: 'void', param_names: ['prefix', name] }),
      ],
    });
  const source = (name, elem) =>
    'export class Tagged {\n' +
    `  tags(...${name}: ${elem}[]): void { void ${name}; }\n` +
    `  label(prefix: string, ...${name}: ${elem}[]): void { void [prefix, ${name}]; }\n}\n`;
  const files = (name, elem) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex([...CLASSES, cls(name)]),
      ...FILES,
      ...LIB,
      'src/producer/tagged.ts': source(name, elem),
    });
  for (const name of ['tagNames', 'check']) {
    let r = lint(files(name, 'string'), [RULE]);
    assert.equal(r.status, 0, r.text);
    r = lint(files(name, 'number'), [RULE]);
    assert.deepEqual(findings(r), [
      `Tagged.tags: parameter ${name}: Java String[] is not TypeScript number[]`,
      `Tagged.label: parameter ${name}: Java String[] is not TypeScript number[]`,
    ]);
  }
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-type-mapping: cannot run, no Java index/);
});

test('nothing mapped fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'java-index.json': javaIndex(classes), ...FILES, 'src/producer/producer.ts': 'export {};\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-type-mapping: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

test('where Java takes an Iterable<Header>, a [key, value] pair is no element: Java takes only Header objects', () => {
  const taker = javaClass('clients.producer.Taker', {
    overloads: [method('take', ['Iterable'], { param_types: ['Iterable<Header>'], return_type: 'void', param_names: ['headers'] })],
  });
  const source = (element) =>
    "import type { Header } from '../common/header/header';\n" +
    `export class Taker {\n  take(headers: Iterable<${element}>): void { void headers; }\n}\n`;
  const files = (element) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex([...classes, taker]),
      ...FILES,
      ...LIB,
      'src/producer/typed.ts': TYPED_TS,
      'src/common/kafka_future.ts': FUTURE_TS,
      'src/producer/taker.ts': source(element),
    });
  let r = lint(files('Header'), [RULE]);
  assert.equal(r.status, 0, r.text);
  // A string value: TypeScript 5.9 prints Uint8Array as Uint8Array<ArrayBufferLike>.
  r = lint(files('Header | readonly [key: string, value: string | null]'), [RULE]);
  assert.deepEqual(findings(r), [
    'Taker.take: parameter headers: Java Iterable<Header> is not TypeScript Iterable<Header | readonly [key: string, value: string | null]>',
  ]);
});

test("a serializer's serialize may also return a Promise of the bytes it takes (AWAITED_RETURNS)", () => {
  const serializer = javaClass('common.serialization.Serializer', {
    kind: 'interface',
    overloads: [method('serialize', ['String', 'T'], { return_type: 'byte[]', param_names: ['topic', 'data'] })],
  });
  const source = (returned) =>
    `export interface Serializer<T> {\n  serialize(options: { topic: string; data: T | null }): ${returned};\n}\n`;
  const files = (returned) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex([...classes, serializer]),
      ...FILES,
      ...LIB,
      'src/producer/typed.ts': TYPED_TS,
      'src/common/kafka_future.ts': FUTURE_TS,
      'src/common/serialization/serializer.ts': source(returned),
    });
  // Taken from the user: a Uint8Array, as Java's byte[] taken is, or a Promise of one.
  let r = lint(files('Uint8Array | null | Promise<Uint8Array | null>'), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(files('Uint8Array | Promise<string>'), [RULE]);
  assert.equal(r.status, 1, r.text);
  assert.equal(findings(r).length, 1, r.text);
  assert.match(findings(r)[0], /^Serializer\.serialize: return: Java byte\[\] is not TypeScript /);
});

test('a Java array or collection taken is any iterable; one returned is an array, a Set, or the TopicPartition and MetricName collections', () => {
  const coll = javaClass('clients.producer.Coll', {
    overloads: [
      method('take', ['Node[]'], { param_types: ['Node[]'], return_type: 'void', param_names: ['nodes'] }),
      method('nodes', [], { return_type: 'Node[]' }),
      method('names', [], { return_type: 'List<String>' }),
      method('tags', [], { return_type: 'Set<String>' }),
      method('parts', [], { return_type: 'Set<TopicPartition>' }),
      method('lags', [], { return_type: 'Map<TopicPartition, Long>' }),
      method('metrics', [], { return_type: 'Map<MetricName, ? extends Metric>' }),
      method('labels', [], { return_type: 'Map<String, String>' }),
      method('label', ['Map'], { param_types: ['Map<String, String>'], return_type: 'void', param_names: ['labels'] }),
    ],
  });
  const extra = [
    coll,
    javaClass('common.Node', {}),
    javaClass('common.TopicPartition', {}),
    javaClass('common.MetricName', {}),
    javaClass('common.Metric', { kind: 'interface' }),
  ];
  const GOOD = {
    take: 'take(nodes: Iterable<Node>): void { void nodes; }',
    nodes: 'nodes(): Node[] { return []; }',
    names: 'names(): string[] { return []; }',
    tags: 'tags(): Set<string> { return new Set(); }',
    parts: 'parts(): TopicPartitionSet { return new TopicPartitionSet(); }',
    lags: 'lags(): TopicPartitionMap<bigint> { return new TopicPartitionMap(); }',
    metrics: 'metrics(): MetricNameMap<Metric> { return new MetricNameMap(); }',
    labels: 'labels(): Map<string, string> { return new Map(); }',
    label: 'label(labels: ReadonlyMap<string, string>): void { void labels; }',
  };
  const source = (members) =>
    "import { Node } from '../common/node';\nimport { TopicPartition } from '../common/topic_partition';\n" +
    "import type { MetricName } from '../common/metric_name';\nimport type { Metric } from '../common/metric';\nimport { MetricNameMap, TopicPartitionMap, TopicPartitionSet } from './collections';\n" +
    `void TopicPartition;\nexport class Coll {\n${Object.values(members).map((m) => `  ${m}\n`).join('')}}\n`;
  const files = (members) =>
    project({
      'tsconfig.json': TSCONFIG,
      'java-index.json': javaIndex([...classes, ...extra]),
      ...FILES,
      ...LIB,
      'src/producer/typed.ts': TYPED_TS,
      'src/common/kafka_future.ts': FUTURE_TS,
      'src/common/node.ts': 'export class Node { private readonly b = 1; }\n',
      'src/common/topic_partition.ts': 'export class TopicPartition { private readonly b = 1; }\n',
      'src/common/metric_name.ts': 'export class MetricName { private readonly b = 1; }\n',
      'src/common/metric.ts': 'export interface Metric { metricValue(): unknown }\n',
      'src/producer/collections.ts':
        'export class TopicPartitionSet { private readonly b = 1; }\nexport class TopicPartitionMap<V> { private readonly v: V | null = null; }\n' +
        'export class MetricNameMap<V> { private readonly v: V | null = null; }\n',
      'src/producer/coll.ts': source(members),
    });
  let r = lint(files(GOOD), [RULE]);
  assert.deepEqual(findings(r), [], r.text);
  for (const [name, member, finding] of [
    ['nodes', 'nodes(): Iterable<Node> { return []; }', 'Coll.nodes: return: Java Node[] is not TypeScript Iterable<Node>'],
    ['names', 'names(): Iterable<string> { return []; }', 'Coll.names: return: Java List<String> is not TypeScript Iterable<string>'],
    ['tags', 'tags(): string[] { return []; }', 'Coll.tags: return: Java Set<String> is not TypeScript string[]'],
    ['parts', 'parts(): Set<TopicPartition> { return new Set(); }', 'Coll.parts: return: Java Set<TopicPartition> is not TypeScript Set<TopicPartition>'],
    ['lags', 'lags(): TopicPartitionMap<number> { return new TopicPartitionMap(); }', 'Coll.lags: return: Java Map<TopicPartition, Long> is not TypeScript TopicPartitionMap<number>'],
    ['labels', 'labels(): ReadonlyMap<string, string> { return new Map(); }', 'Coll.labels: return: Java Map<String, String> is not TypeScript ReadonlyMap<string, string>'],
    ['metrics', 'metrics(): Map<MetricName, Metric> { return new Map(); }', 'Coll.metrics: return: Java Map<MetricName, Metric> is not TypeScript Map<MetricName, Metric>'],
  ]) {
    r = lint(files({ ...GOOD, [name]: member }), [RULE]);
    assert.deepEqual(findings(r), [finding], member);
  }
});

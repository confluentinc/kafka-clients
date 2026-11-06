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
  ],
  fields: [field('ON', { type: 'Mode' })],
});
const TYPED_TS = `import { Rec } from './rec';
import { Mode } from './mode';
import { KafkaFuture } from '../common/kafka_future';
export type Data = Uint8Array | string;
export class Typed {
  static readonly ON: Mode = Mode.ON;
  get count(): number { return 1; }
  get ready(): boolean | null { return null; }
  get label(): string { return ''; }
  get key(): Data | null { return null; }
  get bytes(): Uint8Array { return new Uint8Array(); }
  future(): KafkaFuture<Rec> { return KafkaFuture.of(); }
  names(): string[] { return []; }
  modes(): readonly Mode[] { return []; }
  wait(timeoutMs: number): void { void timeoutMs; }
  fetch(): unknown { return null; }
  flush(): void {}
  pause(timeoutMs: number): void { void timeoutMs; }
  pick(options: { x: string | number }): void { void options; }
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
    'build/java-index.json': javaIndex(classes),
    ...FILES,
    ...LIB,
    'lib/producer/typed.ts': TYPED_TS,
    'lib/common/kafka_future.ts': FUTURE_TS,
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

test('every row of the table maps: numbers, booleans, strings, Data, KafkaFuture, arrays, Duration, void and Promise', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('a type off the table is a type-mismatch; a Java type with no row is unmapped', () => {
  const r = lint(
    fixture({
      'lib/producer/typed.ts': TYPED_TS.replace('get count(): number', 'get count(): string')
        .replace('future(): KafkaFuture<Rec>', 'future(): KafkaFuture<string>')
        .replace("wait(timeoutMs: number): void", "wait(timeoutMs: string): void")
        .replace('flush(): void {}', 'widget(): unknown { return null; }\n  flush(): Promise<void> { return Promise.resolve(); }')
        .replace('pause(timeoutMs: number)', 'pause(timeout: number)')
        .replace('pick(options: { x: string | number })', 'pick(options: { x: string })'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'Typed.count: type: Java long is not TypeScript string',
    'Typed.future: return: Java Future<Rec> is not TypeScript KafkaFuture<string>',
    'Typed.wait: parameter timeoutMs: Java Duration is not TypeScript string',
    'Typed.widget: return: Java Widget has no row in the table (TypeScript unknown)',
    'Typed.flush: return: Java void is not TypeScript Promise<void>',
    'Typed.pause: parameter timeout: it translates Java\'s Duration timeout, so it is named timeoutMs',
    'Typed.pick: option x: Java String | int is not TypeScript string',
  ]);
});

test('a union holding the translated class and more is a mismatch', () => {
  const r = lint(fixture({ 'lib/producer/typed.ts': TYPED_TS.replace('future(): KafkaFuture<Rec>', 'future(): KafkaFuture<Rec | string>') }), [RULE]);
  assert.deepEqual(findings(r), ['Typed.future: return: Java Future<Rec> is not TypeScript KafkaFuture<string | Rec>']);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-type-mapping: cannot run, no Java index/);
});

test('nothing mapped fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), ...FILES, 'lib/producer/producer.ts': 'export {};\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-type-mapping: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

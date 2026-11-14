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

// J1 check-java-name (scripts/lint/rules/java_name.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { javaClass, javaIndex, mapping } = require('./_java_fixture');

const RULE = 'check-java-name';

const INDEX = javaIndex([
  javaClass('clients.producer.Rec'),
  javaClass('clients.producer.Record'),
  javaClass('clients.producer.Alpha'),
  javaClass('clients.producer.Beta'),
  javaClass('clients.producer.BarException'),
  javaClass('common.errors.FooException'),
  javaClass('common.errors.BazException'),
  javaClass('common.header.Header', { kind: 'interface' }),
  javaClass('clients.producer.ProducerRecord'),
]);

const REGISTRY = `export const ERRORS = [
  ['../common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],
  ['../producer/baz_error', 'BazError', 'org.apache.kafka.common.errors.BazException'],
  ['../illegal_state_error', 'IllegalStateError', 'java.lang.IllegalStateException'],
];
`;

/** The fixture's mapping lists: the invented RecArgs, and Header mapped to Java's common.header.Header. */
const LISTS = {
  invented: { 'src/producer/rec_types.ts:RecArgs': 'The options of Rec.' },
  javaTargets: { 'src/producer/rec_types.ts:Header': 'org.apache.kafka.common.header.Header' },
};

/** The fixture project, plus `files`. */
function fixture(files = {}) {
  return project({
    'tsconfig.json': TSCONFIG,
    'java-index.json': INDEX,
    'src/internal/error_registry.ts': REGISTRY,
    'src/internal/private.ts': 'export class Nothing {}\n',
    'src/producer/index.ts': "export { Rec } from './rec';\n",
    'src/producer/rec.ts': 'export class Rec {}\n',
    'src/common/errors/foo_error.ts': 'export class FooError extends Error {}\n',
    'src/illegal_state_error.ts': 'export class IllegalStateError extends Error {}\n',
    'src/producer/rec_types.ts':
      '/** A header. */\nexport type Header = { key: string };\n' + '/** The options of Rec. */\nexport type RecArgs = { a?: number };\n',
    'lint-mapping.json': mapping(LISTS),
    ...files,
  });
}

test('exports named as their Java class, in its file, pass; listed ones, errors and private modules too', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec, FooError, IllegalStateError, Header, RecArgs (index.ts and _private.ts are not public modules).
  assert.ok(r.out.includes('✅ check-java-name: 5 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/opts.ts': 'export type Opts = { a: number };\n',
      'src/producer/the_record.ts': 'export class Record {}\n',
      'src/producer/alpha.ts': 'export class Alpha {}\nexport interface Beta { x: number }\n',
      'src/producer/bar_exception.ts': 'export class BarException extends Error {}\n',
      'src/producer/baz_error.ts': 'export class BazError extends Error {}\n',
      'src/oops.ts': 'export class Oops {}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/oops.ts:Oops: exports Oops, which names no Java class of java.*: list it in lint-mapping.json\'s invented, or javaTargets',
    'src/producer/alpha.ts:Beta: exports Beta (clients.producer.Beta); the file must be beta.ts',
    'src/producer/bar_exception.ts:BarException: translates clients.producer.BarException: an exception\'s class is named BarError',
    'src/producer/baz_error.ts:BazError: translates common.errors.BazException, of common.errors: its file belongs in src/common/errors/',
    'src/producer/opts.ts:Opts: exports Opts, which names no Java class of clients.producer: list it in lint-mapping.json\'s invented, or javaTargets',
    'src/producer/the_record.ts:Record: exports Record (clients.producer.Record); the file must be record.ts',
    'src/producer/alpha.ts: holds 2 public Java types (Alpha, Beta); a file holds one',
  ]);
  assert.ok(r.err.includes('❌ check-java-name: 7 finding(s).'), r.text);
});

test('every export the type checker lists is seen: re-exports, ambient classes, enums, functions, constants', () => {
  const r = lint(
    fixture({
      'src/producer/widget.ts': 'class Widget {}\nexport { Widget };\n',
      'src/producer/acks.ts': "export enum Acks { ALL = 'all' }\n",
      'src/producer/space.ts': 'export namespace Space { export const x = 1; }\n',
      'src/producer/foo_bar.ts': 'export declare class FooBar { mutable: number; }\n',
      'src/producer/create.ts':
        'export function createProducer(): void {}\nexport const DEFAULT_ACKS = 1;\n' +
        '/** A JavaScript helper. */\nexport function helper(): void {}\n',
      'lint-mapping.json': mapping({ ...LISTS, invented: { ...LISTS.invented, 'src/producer/create.ts:helper': 'A JavaScript helper.' } }),
      // A re-export of a type another public module declares is listed once, there.
      'src/producer/again.ts': "export { Rec } from './rec';\n",
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/producer/foo_bar.ts:FooBar: exports FooBar, which names no Java class of clients.producer: list it in lint-mapping.json\'s invented, or javaTargets',
    'src/producer/widget.ts:Widget: exports Widget, which names no Java class of clients.producer: list it in lint-mapping.json\'s invented, or javaTargets',
    'src/producer/acks.ts:Acks: exports Acks as a TypeScript enum, a form no Java type translates to: make it a class',
    'src/producer/create.ts:createProducer: exports the function createProducer, which no Java class translates to: list it in lint-mapping.json\'s invented',
    'src/producer/create.ts:DEFAULT_ACKS: exports the variable DEFAULT_ACKS, which no Java class translates to: list it in lint-mapping.json\'s invented',
    'src/producer/space.ts:Space: exports Space as a TypeScript namespace, a form no Java type translates to: make it a class',
  ]);
});

test('a file holds one public Java type: Header beside ProducerRecord is a finding too', () => {
  const header = '/** A header. */\nexport type Header = { key: string };\n';
  const lists = (...modules) =>
    mapping({
      ...LISTS,
      javaTargets: {
        ...LISTS.javaTargets,
        ...Object.fromEntries(modules.map((m) => [`src/producer/${m}.ts:Header`, 'org.apache.kafka.common.header.Header'])),
      },
    });
  const r = lint(
    fixture({
      'src/producer/producer_record.ts': `export class ProducerRecord {}\n${header}`,
      'src/producer/rec.ts': `export class Rec {}\n${header}`,
      'lint-mapping.json': lists('producer_record', 'rec'),
    }),
    [RULE],
  );
  assert.deepEqual(findings(r).filter((f) => f.includes('holds')), [
    'src/producer/producer_record.ts: holds 2 public Java types (ProducerRecord, Header); a file holds one',
    'src/producer/rec.ts: holds 2 public Java types (Rec, Header); a file holds one',
  ]);
});

test('an overloaded function is one item, listed once', () => {
  const overloads =
    'export function parseSize(v: string): number;\nexport function parseSize(v: number): number;\n' +
    'export function parseSize(v: string | number): number { return Number(v); }\n';
  const listed = mapping({ ...LISTS, invented: { ...LISTS.invented, 'src/producer/helpers.ts:parseSize': 'A parsing helper.' } });
  let r = lint(fixture({ 'src/producer/helpers.ts': overloads, 'lint-mapping.json': listed }), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(fixture({ 'src/producer/helpers.ts': overloads }), [RULE]);
  assert.deepEqual(findings(r), [
    'src/producer/helpers.ts:parseSize: exports the function parseSize, which no Java class translates to: list it in lint-mapping.json\'s invented',
  ]);
});

test('a baseline line waives a finding', () => {
  const root = fixture({
    'src/producer/opts.ts': 'export type Opts = { a: number };\n',
    'eslint-baseline.txt': 'no-java-class src/producer/opts.ts:Opts\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without the Java index, or with one of another schema', () => {
  let r = lint(project({ 'tsconfig.json': TSCONFIG, 'src/a.ts': 'export class A {}\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(r.err.slice(0, 2), [
    '',
    '❌ check-java-name: cannot run, no Java index at java-index.json (run `cargo xtask java-api` from rust/)',
  ]);
  r = lint(project({ 'tsconfig.json': TSCONFIG, 'src/a.ts': '', 'java-index.json': '{"schema":1,"classes":[]}' }), [RULE]);
  assert.equal(
    r.err[1],
    '❌ check-java-name: cannot run, the Java index java-index.json is not schema 2 (rerun `cargo xtask java-api` from rust/)',
  );
});

test('a src/ exporting nothing fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'java-index.json': INDEX, 'src/a.ts': 'const a = 1;\nvoid a;\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-java-name: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

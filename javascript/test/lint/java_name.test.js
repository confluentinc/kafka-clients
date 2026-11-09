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
const { javaClass, javaIndex } = require('./_java_fixture');

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
  ['./common/errors/foo_error', 'FooError', 'org.apache.kafka.common.errors.FooException'],
  ['./producer/baz_error', 'BazError', 'org.apache.kafka.common.errors.BazException'],
  ['./illegal_state_error', 'IllegalStateError', 'java.lang.IllegalStateException'],
];
`;

/** The fixture project, plus `files`. */
function fixture(files = {}) {
  return project({
    'tsconfig.json': TSCONFIG,
    'build/java-index.json': INDEX,
    'lib/_error_registry.ts': REGISTRY,
    'lib/_private.ts': 'export class Nothing {}\n',
    'lib/producer/index.ts': "export { Rec } from './rec';\n",
    'lib/producer/rec.ts': 'export class Rec {}\n',
    'lib/common/errors/foo_error.ts': 'export class FooError extends Error {}\n',
    'lib/illegal_state_error.ts': 'export class IllegalStateError extends Error {}\n',
    'lib/producer/rec_types.ts':
      '/**\n * A header.\n *\n * @java org.apache.kafka.common.header.Header\n */\nexport type Header = { key: string };\n' +
      '/** @jsOnly The options of Rec. */\nexport type RecOptions = { a?: number };\n',
    ...files,
  });
}

test('exports named as their Java class, in its file, pass; tags, errors and private modules too', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec, FooError, IllegalStateError, Header, RecOptions (index.ts and _private.ts are not public modules).
  assert.ok(r.out.includes('✅ check-java-name: 5 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'lib/producer/opts.ts': 'export type Opts = { a: number };\n',
      'lib/producer/the_record.ts': 'export class Record {}\n',
      'lib/producer/alpha.ts': 'export class Alpha {}\nexport interface Beta { x: number }\n',
      'lib/producer/bar_exception.ts': 'export class BarException extends Error {}\n',
      'lib/producer/baz_error.ts': 'export class BazError extends Error {}\n',
      'lib/oops.ts': 'export class Oops {}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lib/oops.ts:Oops: exports Oops, which names no Java class of java.*: tag it @jsOnly <reason>, or @java <fqn>',
    'lib/producer/alpha.ts:Beta: exports Beta (clients.producer.Beta); the file must be beta.ts',
    'lib/producer/bar_exception.ts:BarException: translates clients.producer.BarException: an exception\'s class is named BarError',
    'lib/producer/baz_error.ts:BazError: translates common.errors.BazException, of common.errors: its file belongs in lib/common/errors/',
    'lib/producer/opts.ts:Opts: exports Opts, which names no Java class of clients.producer: tag it @jsOnly <reason>, or @java <fqn>',
    'lib/producer/the_record.ts:Record: exports Record (clients.producer.Record); the file must be record.ts',
    'lib/producer/alpha.ts: holds 2 public Java types (Alpha, Beta); a file holds one',
  ]);
  assert.ok(r.err.includes('❌ check-java-name: 7 finding(s).'), r.text);
});

test('every export the type checker lists is seen: re-exports, ambient classes, enums, functions, constants', () => {
  const r = lint(
    fixture({
      'lib/producer/widget.ts': 'class Widget {}\nexport { Widget };\n',
      'lib/producer/acks.ts': "export enum Acks { ALL = 'all' }\n",
      'lib/producer/space.ts': 'export namespace Space { export const x = 1; }\n',
      'lib/producer/foo_bar.ts': 'export declare class FooBar { mutable: number; }\n',
      'lib/producer/create.ts':
        'export function createProducer(): void {}\nexport const DEFAULT_ACKS = 1;\n' +
        '/** @jsOnly A JavaScript helper. */\nexport function helper(): void {}\n',
      // A re-export of a type another public module declares is listed once, there.
      'lib/producer/again.ts': "export { Rec } from './rec';\n",
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lib/producer/foo_bar.ts:FooBar: exports FooBar, which names no Java class of clients.producer: tag it @jsOnly <reason>, or @java <fqn>',
    'lib/producer/widget.ts:Widget: exports Widget, which names no Java class of clients.producer: tag it @jsOnly <reason>, or @java <fqn>',
    'lib/producer/acks.ts:Acks: exports Acks as a TypeScript enum, a form no Java type translates to: make it a class',
    'lib/producer/create.ts:createProducer: exports the function createProducer, which no Java class translates to: tag it @jsOnly <reason>',
    'lib/producer/create.ts:DEFAULT_ACKS: exports the variable DEFAULT_ACKS, which no Java class translates to: tag it @jsOnly <reason>',
    'lib/producer/space.ts:Space: exports Space as a TypeScript namespace, a form no Java type translates to: make it a class',
  ]);
});

test('Header may sit beside ProducerRecord in producer_record.ts, and nowhere else', () => {
  const header = '/** @java org.apache.kafka.common.header.Header */\nexport type Header = { key: string };\n';
  let r = lint(fixture({ 'lib/producer/producer_record.ts': `export class ProducerRecord {}\n${header}` }), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(
    fixture({
      'lib/producer/producer_record.ts': `export class ProducerRecord {}\n${header}export class Alpha {}\n`,
      'lib/producer/rec.ts': `export class Rec {}\n${header}`,
    }),
    [RULE],
  );
  assert.deepEqual(findings(r).filter((f) => f.includes('holds')), [
    'lib/producer/producer_record.ts: holds 2 public Java types (ProducerRecord, Alpha); a file holds one',
    'lib/producer/rec.ts: holds 2 public Java types (Rec, Header); a file holds one',
  ]);
});

test('an overloaded function is one item, tagged by whichever signature carries the tag', () => {
  const overloads =
    'export function parseSize(v: string): number;\nexport function parseSize(v: number): number;\n' +
    'export function parseSize(v: string | number): number { return Number(v); }\n';
  let r = lint(fixture({ 'lib/producer/helpers.ts': `/** @jsOnly A parsing helper. */\n${overloads}` }), [RULE]);
  assert.equal(r.status, 0, r.text);
  r = lint(fixture({ 'lib/producer/helpers.ts': overloads }), [RULE]);
  assert.deepEqual(findings(r), [
    'lib/producer/helpers.ts:parseSize: exports the function parseSize, which no Java class translates to: tag it @jsOnly <reason>',
  ]);
});

test('a baseline line waives a finding', () => {
  const root = fixture({
    'lib/producer/opts.ts': 'export type Opts = { a: number };\n',
    'eslint-baseline.txt': 'no-java-class lib/producer/opts.ts:Opts\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without the Java index, or with one of another schema', () => {
  let r = lint(project({ 'tsconfig.json': TSCONFIG, 'lib/a.ts': 'export class A {}\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.deepEqual(r.err.slice(0, 2), [
    '',
    '❌ check-java-name: cannot run, no Java index at build/java-index.json (run `cargo xtask java-index --json` from rust/)',
  ]);
  r = lint(project({ 'tsconfig.json': TSCONFIG, 'lib/a.ts': '', 'build/java-index.json': '{"schema":2,"classes":[]}' }), [RULE]);
  assert.equal(
    r.err[1],
    '❌ check-java-name: cannot run, the Java index build/java-index.json is not schema 1 (rerun `cargo xtask java-index --json`)',
  );
});

test('a lib/ exporting nothing fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': INDEX, 'lib/a.ts': 'const a = 1;\nvoid a;\n' }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-java-name: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

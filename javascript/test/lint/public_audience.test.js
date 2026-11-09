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

// J4 check-public-audience (scripts/lint/rules/public_audience.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, javaIndex } = require('./_java_fixture');

const RULE = 'check-public-audience';

const fixture = (files = {}, classes = CLASSES) =>
  project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), ...LIB, ...files });

test('Public classes and their public members pass; an error class is exempt', () => {
  // A non-Public error class is still exported: one class per FFI-enum exception.
  const classes = CLASSES.map((c) => (c.fqn.endsWith('FooException') ? { ...c, public_audience: false } : c));
  const r = lint(fixture({}, classes), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec, Mode, ProducerConfig, Callback, Header, Future; and the 22 members the J2 rule maps.
  assert.ok(r.out.includes('✅ check-public-audience: 28 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'lib/producer/sealed.ts': 'export class Sealed {}\n',
      'lib/producer/internals/acc.ts': 'export class Acc {}\n',
      'lib/common/x/unsupp.ts': 'export class Unsupp {}\n',
      'lib/producer/pkg.ts': 'export class Pkg {\n  constructor() {}\n}\n',
      'lib/producer/rec.ts': LIB['lib/producer/rec.ts'].replace('extra(): void {}', 'extra(): void {}\n  hidden(): void {}'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lib/common/x/unsupp.ts:Unsupp: translates common.x.Unsupp, whose package common.x is not a supported Kafka API',
    'lib/producer/internals/acc.ts:Acc: translates clients.producer.internals.Acc, whose package clients.producer.internals is internal',
    'lib/producer/sealed.ts:Sealed: translates clients.producer.Sealed, which is not @InterfaceAudience.Public in Kafka 4.4.0-rc3',
    'Pkg.constructor: translates the package-private constructor clients.producer.Pkg#Pkg',
    'Rec.hidden: translates the private method clients.producer.Rec#hidden',
  ]);
});

test('a @jsOnly export translates nothing, so it is not checked', () => {
  const r = lint(fixture({ 'lib/producer/sealed.ts': '/** @jsOnly A JavaScript helper. */\nexport class Sealed {}\n' }), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-public-audience: cannot run, no Java index/);
});

test('nothing translated fails: 0 items checked', () => {
  const r = lint(
    project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex([javaClass('clients.producer.Rec')]), 'lib/a.ts': 'export const a = 1;\n' }),
    [RULE],
  );
  assert.ok(r.err.includes('❌ check-public-audience: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

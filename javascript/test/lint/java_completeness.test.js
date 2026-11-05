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

// J3 check-java-completeness (scripts/lint/rules/java_completeness.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaIndex } = require('./_java_fixture');

const RULE = 'check-java-completeness';

const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(CLASSES), ...LIB, ...files });

test('a class translating every public, live Java member passes', () => {
  const r = lint(fixture(), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Rec: UNKNOWN, Rec, topic, offset, getSize, isOpen, hasKey, close, toString (not old, hidden,
  // mode, LEGACY, SECRET, nor equals / hashCode, which the client does not translate); Mode: ON, OFF, forName (not java.lang.Enum's members);
  // ProducerConfig: A_CONFIG (its constants only); the interface Header: key, value; the abstract
  // Future: get, not its constructor. The error and the function type ask nothing.
  assert.ok(r.out.includes('✅ check-java-completeness: 16 item(s) checked'), r.text);
});

test('a member, the constructor and a constant left out are each a finding', () => {
  const r = lint(
    fixture({
      'lib/producer/rec.ts': LIB['lib/producer/rec.ts']
        .replace('  static readonly UNKNOWN: number = -1;\n', '')
        .replace('  constructor(topic: string) { void topic; }\n', '  private constructor() {}\n')
        .replace('  get size(): number { return 0; }\n', ''),
      'lib/producer/producer_config.ts': 'export class ProducerConfig {}\n',
      'lib/common/header/header.ts': 'export type Header = { readonly key: string };\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'common.header.Header#value: Header does not translate it',
    'clients.producer.ProducerConfig#A_CONFIG: ProducerConfig does not translate it',
    'clients.producer.Rec#UNKNOWN: Rec does not translate it',
    'clients.producer.Rec#Rec: Rec has no public constructor',
    'clients.producer.Rec#getSize: Rec does not translate it',
  ]);
});

test('the baseline lists what is not translated yet', () => {
  const root = fixture({
    'lib/producer/rec.ts': LIB['lib/producer/rec.ts'].replace('  close(): void {}\n', ''),
    'lint-baseline.txt': '# not yet\nmissing clients.producer.Rec#close\n',
  });
  assert.equal(lint(root, [RULE]).status, 0);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.equal(r.status, 1);
  assert.match(r.err[1], /^❌ check-java-completeness: cannot run, no Java index/);
});

test('no class to complete fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(CLASSES), 'lib/producer/callback.ts': LIB['lib/producer/callback.ts'] }), [RULE]);
  assert.equal(r.status, 1);
  assert.ok(r.err.includes('❌ check-java-completeness: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

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

// J5 check-no-deprecated-translation (scripts/lint/rules/no_deprecated_translation.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, javaIndex, method } = require('./_java_fixture');

const RULE = 'check-no-deprecated-translation';

const fixture = (files = {}, classes = CLASSES) =>
  project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), ...LIB, ...files });

test('live API passes; a name with a live overload is live', () => {
  const classes = CLASSES.map((c) =>
    c.fqn.endsWith('.Rec') ? { ...c, overloads: [...c.overloads, method('close', ['Duration', 'boolean'], { deprecated: true })] } : c,
  );
  const r = lint(fixture({}, classes), [RULE]);
  assert.equal(r.status, 0, r.text);
  // 7 exported types, the error included, and 22 mapped members.
  assert.ok(r.out.includes('✅ check-no-deprecated-translation: 31 item(s) checked'), r.text);
});

test('a deprecated class, method and constant are findings', () => {
  const r = lint(
    fixture({
      'src/producer/old.ts': 'export class Old {}\n',
      'src/producer/stopper.ts': 'export class Stopper {\n  start(): void {}\n  stop(): void {}\n}\n',
      'src/producer/rec.ts': LIB['src/producer/rec.ts'].replace(
        'extra(): void {}',
        'extra(): void {}\n  old(): void {}\n  static readonly LEGACY: string = "l";',
      ),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/producer/old.ts:Old: translates clients.producer.Old, deprecated in Kafka 4.3.1 + 4.4.0-rc3',
    'Rec.old: translates clients.producer.Rec#old, deprecated in Kafka 4.3.1 + 4.4.0-rc3',
    'Rec.LEGACY: translates clients.producer.Rec#LEGACY, deprecated in Kafka 4.3.1 + 4.4.0-rc3',
    // Declared by a deprecated supertype of a live class.
    'Stopper.stop: translates clients.producer.Legacy#stop, deprecated in Kafka 4.3.1 + 4.4.0-rc3',
  ]);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-no-deprecated-translation: cannot run, no Java index/);
});

test('nothing translated fails: 0 items checked', () => {
  const r = lint(
    project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex([javaClass('clients.producer.Rec')]), 'src/a.ts': 'export const a = 1;\n' }),
    [RULE],
  );
  assert.ok(r.err.includes('❌ check-no-deprecated-translation: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

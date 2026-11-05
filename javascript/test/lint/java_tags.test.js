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

// The mapping tags, @jsOnly and @java (scripts/lint/rules/java_tags.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, javaIndex } = require('./_java_fixture');

const RULE = 'check-java-tags';

const classes = [...CLASSES, javaClass('common.header.Header', { kind: 'interface', overloads: [] })];
const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), ...LIB, ...files });

test('needed, well-formed tags pass', () => {
  const r = lint(
    fixture({
      'lib/producer/types.ts':
        '/** @jsOnly The options object of close. */\nexport type CloseOptions = { timeoutMs?: number };\n' +
        '/** @java org.apache.kafka.common.header.Header */\nexport type Header = { key: string };\n',
      'lib/producer/rec.ts': LIB['lib/producer/rec.ts'].replace(
        'extra(): void {}',
        'extra(): void {}\n  /** @java org.apache.kafka.clients.producer.Rec#hidden */\n  conceal(): void {}',
      ),
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
  // 9 exported items; the 24 public members of the exported classes and the 4 of the
  // interface and object types.
  assert.ok(r.out.includes('✅ check-java-tags: 37 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'lib/producer/types.ts':
        '/** @jsOnly */\nexport type NoReason = { a: number };\n' +
        '/** @java org.apache.kafka.clients.producer.Nowhere */\nexport type Lost = { a: number };\n' +
        '/**\n * @jsOnly A helper.\n * @java org.apache.kafka.common.header.Header\n */\nexport type Both = { a: number };\n',
      'lib/producer/mode.ts': '/** @jsOnly Not really. */\n' + LIB['lib/producer/mode.ts'],
      'lib/producer/callback.ts': '/** @java org.apache.kafka.clients.producer.Callback */\nexport type Callback = (m: unknown) => void;\n',
      'lib/common/future.ts': '/** @java org.apache.kafka.clients.producer.Rec */\nexport class Future {}\n',
      'lib/producer/rec.ts': LIB['lib/producer/rec.ts']
        .replace('get topic()', '/** @jsOnly Mine. */\n  get topic()')
        .replace('close(): void {}', '/** @java org.apache.kafka.clients.producer.Rec#close */\n  close(): void {}')
        .replace('extra(): void {}', '/** @java org.apache.kafka.clients.producer.Rec#hidden */\n  extra(): void {}')
        .replace('flush(): void {}', '/** @java org.apache.kafka.clients.producer.Rec#vanish */\n  flush(): void {}'),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'lib/common/future.ts:Future: @java org.apache.kafka.clients.producer.Rec re-maps what the convention maps to common.Future: rename the item, or drop the tag',
    'lib/producer/callback.ts:Callback: @java org.apache.kafka.clients.producer.Callback names what the convention finds: drop the tag',
    'lib/producer/mode.ts:Mode: is tagged @jsOnly, but it translates clients.producer.Mode: drop the tag',
    'lib/producer/types.ts:NoReason: @jsOnly needs a reason',
    'lib/producer/types.ts:Lost: @java org.apache.kafka.clients.producer.Nowhere names no class of the Java index',
    'lib/producer/types.ts:Both: carries both @jsOnly and @java',
    'Rec.topic: is tagged @jsOnly, but it translates clients.producer.Rec#topic: drop the tag',
    'Rec.close: @java org.apache.kafka.clients.producer.Rec#close names what the convention finds: drop the tag',
    'Rec.flush: @java org.apache.kafka.clients.producer.Rec#vanish: clients.producer.Rec declares no member vanish',
    'Rec.extra: @java org.apache.kafka.clients.producer.Rec#hidden re-maps what the convention maps to clients.producer.Base#extra: rename the item, or drop the tag',
  ]);
  assert.ok(r.err.includes('❌ check-java-tags: 10 finding(s).'), r.text);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-java-tags: cannot run, no Java index/);
});

test('nothing exported fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), 'lib/a.ts': 'const a = 1;\nvoid a;\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-java-tags: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

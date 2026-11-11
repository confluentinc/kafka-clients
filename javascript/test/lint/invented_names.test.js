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

// The mapping lists of lint-mapping.json, invented and javaTargets
// (scripts/lint/rules/invented_names.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, javaIndex, mapping, INVENTED } = require('./_java_fixture');

const RULE = 'check-invented-names';

const classes = [...CLASSES, javaClass('common.header.Header', { kind: 'interface', overloads: [] })];
const fixture = (files = {}) => project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), ...LIB, ...files });

test('needed entries that resolve pass, an invented options type named ...Args too', () => {
  const r = lint(
    fixture({
      'src/producer/types.ts':
        '/** The options object of close. */\nexport type CloseArgs = { timeoutMs?: number };\n' +
        '/** A header. */\nexport type RecordHeader = { key: string };\n' +
        '/** Not an options type: a map. */\nexport type RecConfigs = Record<string, string>;\n',
      'src/producer/rec.ts': LIB['src/producer/rec.ts'].replace('extra(): void {}', 'extra(): void {}\n  conceal(): void {}'),
      'lint-mapping.json': mapping({
        invented: {
          ...INVENTED,
          'src/producer/types.ts:CloseArgs': 'The options object of close.',
          'src/producer/types.ts:RecConfigs': 'The configuration map.',
        },
        javaTargets: {
          'src/producer/types.ts:RecordHeader': 'org.apache.kafka.common.header.Header',
          'Rec.conceal': 'org.apache.kafka.clients.producer.Rec#hidden',
        },
      }),
    }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
  // The exported items, the public members of the exported classes, interfaces
  // and object types, and the 6 entries.
  assert.ok(r.out.includes('✅ check-invented-names: 47 item(s) checked'), r.text);
});

test('each finding kind, with its exact text', () => {
  const r = lint(
    fixture({
      'src/producer/types.ts':
        '/** No reason. */\nexport type NoReason = { a: number };\n' +
        '/** Lost. */\nexport type Lost = { a: number };\n' +
        '/** Both. */\nexport type Both = { a: number };\n' +
        '/** An options type. */\nexport interface CloseOptions { timeoutMs?: number }\n',
      'src/common/future.ts': '/** A future. */\nexport class Future {}\n',
      'lint-mapping.json': mapping({
        invented: {
          ...INVENTED,
          'src/producer/types.ts:NoReason': '',
          'src/producer/types.ts:Both': 'A helper.',
          'src/producer/types.ts:CloseOptions': 'The options of close.',
          'src/producer/mode.ts:Mode': 'Not really.',
          'Rec.topic': 'Mine.',
          'Rec.gone': 'Removed.',
        },
        javaTargets: {
          'src/producer/types.ts:Lost': 'org.apache.kafka.clients.producer.Nowhere',
          'src/producer/types.ts:Both': 'org.apache.kafka.common.header.Header',
          'src/producer/callback.ts:Callback': 'org.apache.kafka.clients.producer.Callback',
          'src/common/future.ts:Future': 'org.apache.kafka.clients.producer.Rec',
          'Rec.close': 'org.apache.kafka.clients.producer.Rec#close',
          'Rec.extra': 'org.apache.kafka.clients.producer.Rec#hidden',
          'Rec.flush': 'org.apache.kafka.clients.producer.Rec#vanish',
          'Rec.nowhere': 'org.apache.kafka.clients.producer.Rec#hidden',
        },
      }),
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'src/common/future.ts:Future: javaTargets org.apache.kafka.clients.producer.Rec re-maps what the convention maps to common.Future: rename the item, or drop the entry',
    'src/producer/callback.ts:Callback: javaTargets org.apache.kafka.clients.producer.Callback names what the convention finds: drop the entry',
    'src/producer/mode.ts:Mode: is listed as invented, but it translates clients.producer.Mode: drop the entry',
    'src/producer/types.ts:NoReason: is invented, with no reason',
    'src/producer/types.ts:NoReason: is an invented options type: name it NoReasonArgs',
    'src/producer/types.ts:Lost: javaTargets org.apache.kafka.clients.producer.Nowhere names no class of the Java index',
    'src/producer/types.ts:Both: is both in invented and in javaTargets',
    'src/producer/types.ts:CloseOptions: is an invented options type: name it CloseArgs',
    'Rec.topic: is listed as invented, but it translates clients.producer.Rec#topic: drop the entry',
    'Rec.close: javaTargets org.apache.kafka.clients.producer.Rec#close names what the convention finds: drop the entry',
    'Rec.flush: javaTargets org.apache.kafka.clients.producer.Rec#vanish: clients.producer.Rec declares no member vanish',
    'Rec.extra: javaTargets org.apache.kafka.clients.producer.Rec#hidden re-maps what the convention maps to clients.producer.Base#extra: rename the item, or drop the entry',
    'Rec.gone: invented lists it, but no exported item or public member is keyed so: drop the entry',
    'Rec.nowhere: javaTargets lists it, but no exported item or public member is keyed so: drop the entry',
  ]);
  assert.ok(r.err.includes('❌ check-invented-names: 14 finding(s).'), r.text);
});

test('without lint-mapping.json both lists are empty: an invented export is then check-java-name\'s finding, not this rule\'s', () => {
  const r = lint(fixture({ 'lint-mapping.json': undefined, 'src/producer/rec.ts': LIB['src/producer/rec.ts'].replace('  extraJs(): void {}\n', '') }), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('cannot run without the Java index', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, ...LIB }), [RULE]);
  assert.match(r.err[1], /^❌ check-invented-names: cannot run, no Java index/);
});

test('nothing exported fails: 0 items checked', () => {
  const r = lint(project({ 'tsconfig.json': TSCONFIG, 'build/java-index.json': javaIndex(classes), 'src/a.ts': 'const a = 1;\nvoid a;\n' }), [RULE]);
  assert.ok(r.err.includes('❌ check-invented-names: 0 item(s) checked; a rule that checks nothing cannot pass'), r.text);
});

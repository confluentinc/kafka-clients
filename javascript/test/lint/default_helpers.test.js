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

// check-default-helpers (scripts/lint/rules/default_helpers.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { TSCONFIG, project, lint, findings } = require('./_project');
const { CLASSES, LIB, javaClass, method, javaIndex } = require('./_java_fixture');

const RULE = 'check-default-helpers';

// A Java interface the user implements, with one abstract and one default method.
const LISTENER = javaClass('clients.consumer.ConsumerRebalanceListener', {
  kind: 'interface',
  overloads: [method('onPartitionsRevoked', ['Collection']), method('onPartitionsLost', ['Collection'], { default: true })],
});
const LISTENER_TS =
  'export interface ConsumerRebalanceListener {\n  onPartitionsRevoked(options: { partitions: unknown }): void;\n' +
  '  onPartitionsLost?(options: { partitions: unknown }): void;\n}\n';
const HELPER =
  'export class ConsumerRebalanceListenerDefaults {\n  onPartitionsLost(): void {}\n}\n';

const fixture = (files = {}) =>
  project({
    'tsconfig.json': TSCONFIG,
    'java-index.json': javaIndex([...CLASSES, LISTENER]),
    ...LIB,
    'src/consumer/consumer_rebalance_listener.ts': LISTENER_TS,
    ...files,
  });

test("an interface the user implements with a default method has its helper class; one with none needs none", () => {
  const r = lint(fixture({ 'src/internal/consumer_rebalance_listener_defaults.ts': HELPER }), [RULE]);
  assert.equal(r.status, 0, r.text);
  // Callback, Header and ConsumerRebalanceListener, then the one helper.
  assert.ok(r.out.includes('✅ check-default-helpers: 4 item(s) checked'), r.text);
});

test('a missing helper, a missing default method and a helper nothing asks for are findings', () => {
  let r = lint(fixture(), [RULE]);
  assert.deepEqual(findings(r), [
    'ConsumerRebalanceListener: has the default methods onPartitionsLost in Java, but src/internal/consumer_rebalance_listener_defaults.ts declares no class ConsumerRebalanceListenerDefaults',
  ]);
  r = lint(
    fixture({
      'src/internal/consumer_rebalance_listener_defaults.ts': 'export class ConsumerRebalanceListenerDefaults {}\n',
      'src/internal/callback_defaults.ts': 'export class CallbackDefaults {}\n',
    }),
    [RULE],
  );
  assert.deepEqual(findings(r), [
    "ConsumerRebalanceListenerDefaults.onPartitionsLost: src/internal/consumer_rebalance_listener_defaults.ts does not translate Java's default org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsLost",
    'CallbackDefaults: src/internal/callback_defaults.ts declares it, but no interface the user implements has default methods for it',
  ]);
});

test('an interface an exported class implements is the client\'s, not the user\'s: it needs no helper', () => {
  const r = lint(
    fixture({ 'src/consumer/listener_impl.ts': "import type { ConsumerRebalanceListener } from './consumer_rebalance_listener';\nexport class ListenerImpl implements ConsumerRebalanceListener {\n  onPartitionsRevoked(): void {}\n}\n" }),
    [RULE],
  );
  assert.equal(r.status, 0, r.text);
});

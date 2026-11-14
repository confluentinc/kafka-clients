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

// check-tsfn-lifetime (scripts/lint/rules/tsfn_lifetime.js).

const test = require('node:test');
const assert = require('node:assert/strict');

const { project, lint, findings } = require('./_project');

const RULE = 'check-tsfn-lifetime';

const QUEUE =
  'int cbq_closed(napi_env env, Q* q) {\n  if (q->tsfn_alive) napi_unref_threadsafe_function(env, q->tsfn);\n  return 1;\n}\n';

test('the thread-safe function unreferenced only by cbq_closed passes', () => {
  const r = lint(project({ 'native/callback_queue.c': QUEUE }), [RULE]);
  assert.equal(r.status, 0, r.text);
});

test('a reference, an unreference off the close path, or none on it, is a finding', () => {
  let r = lint(
    project({
      'native/callback_queue.c': QUEUE,
      'native/producer.c':
        'void start(napi_env env, P* p) {\n  napi_ref_threadsafe_function(env, p->tsfn);\n}\n' +
        'void idle(napi_env env, P* p) {\n  napi_unref_threadsafe_function(env, p->tsfn);\n}\n',
    }),
    [RULE],
  );
  assert.equal(r.status, 1);
  assert.deepEqual(findings(r), [
    'native/producer.c:2: references the thread-safe function again: Node-API creates it referenced, and nothing references it later',
    'native/producer.c:5: unreferences the thread-safe function in idle, not on the close path (cbq_closed): a client keeps Node running until the core reports it closed',
  ]);
  r = lint(project({ 'native/callback_queue.c': 'int cbq_closed(napi_env env, Q* q) {\n  return 1;\n}\n' }), [RULE]);
  assert.deepEqual(findings(r), ['cbq_closed: calls no napi_unref_threadsafe_function: a closed client would keep Node running']);
});

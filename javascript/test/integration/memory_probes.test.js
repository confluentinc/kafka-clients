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

// The memory probes that need a broker (test/memory/_probe.js: a child with
// --expose-gc, the allocator's poisoning and the addon's test-only counts).
// A KafkaProducer's send() resolves at the record's hand-over, which needs
// the topic's metadata, so only a broker leaves a time between the hand-over
// and the delivery: linger.ms holds the batch in the producer for that time.
// Against an unreachable broker the hand-over fails with the delivery.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult, droppedFuturesProbe, droppedFutureCalls } = require('../memory/_probe');

const BOOTSTRAP = /** @type {string} */ (process.env.KAFKA_BOOTSTRAP_SERVERS);
const SKIP = BOOTSTRAP ? false : 'KAFKA_BOOTSTRAP_SERVERS is not set: no broker to produce to';
if (SKIP) console.log(`skipping the memory probes that need a broker: ${SKIP}`);

// The record's context owns its callback, not its future (javascript/CLAUDE.md,
// rule 5), and a future need not be awaited: a dropped one still runs its
// callback once, with the delivery. The record's key and value Buffers are the
// app's, borrowed only until the hand-over (rule 8): they are collected before
// the delivery, while the record waits in the producer.
(SKIP ? test.skip : test)(
  "a future dropped before its delivery still runs its callback once, with the delivery, and its record's Buffers go at the hand-over: a producer",
  () => {
    const topic = `js-it-dropped-futures-${process.pid}-${Date.now()}`;
    const configs = { 'bootstrap.servers': BOOTSTRAP, 'linger.ms': 10000 };
    const out = probeResult(droppedFuturesProbe(`new KafkaProducer({ configs: ${JSON.stringify(configs)} })`, 'await p.flush();', topic), {
      timeout: 120000,
    });
    assert.deepEqual(out.calls, droppedFutureCalls(topic));
    assert.deepEqual(out.beforeDelivery, ['value 0', 'value 1', 'value 2']);
    assert.deepEqual(out.collected, ['future 0', 'future 1', 'future 2', 'value 0', 'value 1', 'value 2']);
    assert.deepEqual(out.leaks, NO_LEAKS);
  },
  180000,
);

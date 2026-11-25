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

// A producer's live metrics(): each map holds an owned native view, and each
// KafkaMetric an owned native metric, each destroyed by its handle object's
// finalizer (native/metrics.c metricsStats()). Each probe runs in a child
// with --expose-gc and the allocator's poisoning, so a handle destroyed twice,
// or read after its destroy, crashes it.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe } = require('./_probe');

test('every metrics view and metric handle is destroyed once collected, none twice', () => {
  const out = probe(`
    const { KafkaProducer, MockProducer } = require(D + '/producer');
    const { MetricName } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const seen = {};
      await (async () => {
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'client.id': 'mem-metrics' } });
        const key = new MetricName({ name: 'record-send-total', group: 'producer-metrics', description: '', tags: { 'client-id': 'mem-metrics' } });
        for (let i = 0; i < 20; i++) {
          const map = p.metrics();
          // Each read of every kind: the size, a key, a value, the entries.
          seen.size = map.size();
          seen.has = map.has(key);
          seen.value = map.get(key).metricValue();
          seen.measurable = map.get(key).isMeasurable();
          seen.entries = [...map].length;
          for (const metric of map.values()) metric.metricValue();
          map.forEach((metric) => metric.metricValue());
          JSON.stringify(map);
          seen.absent = map.get(new MetricName({ name: 'absent', group: 'g', description: '', tags: {} }));
        }
        await p.close();
        const m = new MockProducer();
        seen.mock = m.metrics().size();
        await m.close();
      })();
      await settle();
      console.log(JSON.stringify({ seen, leaks: leaks(), stats: native.metricsStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.seen.has, true);
  assert.equal(out.seen.value, 0);
  assert.equal(out.seen.measurable, true);
  assert.equal(out.seen.entries, out.seen.size);
  assert.equal(out.seen.absent, null);
  assert.equal(out.seen.mock, 0);
  // One view per KafkaProducer.metrics() call (the mock's map is the mock's own).
  assert.equal(out.stats.viewsMade, 20);
  assert.ok(out.stats.metricsMade >= 20 * out.seen.size, JSON.stringify(out.stats));
});

test('a map kept after its producer is closed and collected still reads, and drops the metrics close() removed', () => {
  const out = probe(`
    const { KafkaProducer } = require(D + '/producer');
    const { MetricName } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const name = (n) => new MetricName({ name: n, group: 'producer-metrics', description: '', tags: { 'client-id': 'mem-kept' } });
      let map = null;
      let age = null;
      const result = {};
      await (async () => {
        const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'client.id': 'mem-kept' } });
        map = p.metrics();
        // Every metric cached, the ones close() removes included.
        result.before = [...map].length;
        age = map.get(name('metadata-age'));
        await p.close();
      })();
      await settle();
      const s = native.producerStats();
      result.producersLeft = s.producersMade - s.producersFreed;
      result.size = map.size();
      result.entries = [...map].length;
      result.age = typeof age.metricValue();
      result.same = map.get(name('metadata-age')) === age;
      result.flushTime = map.get(name('flush-time-ns-total'));
      await settle();
      // The metrics the map still holds: one handle each, the removed ones' freed.
      const m = native.metricsStats();
      result.metricsHeld = m.metricsMade - m.metricsFreed;
      map = null;
      age = null;
      await settle();
      console.log(JSON.stringify({ result, leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  const r = out.result;
  assert.equal(r.producersLeft, 0, 'the producer was collected');
  assert.ok(r.size > 0 && r.size < r.before, JSON.stringify(r));
  assert.equal(r.entries, r.size);
  assert.equal(r.age, 'number');
  assert.equal(r.same, true);
  assert.equal(r.flushTime, null);
  assert.equal(r.metricsHeld, r.size, JSON.stringify(r));
  assert.deepEqual(out.leaks, NO_LEAKS);
});

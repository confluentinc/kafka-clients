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

// A client whose construction fails once its handle object exists (its
// notify registration, made to fail by the test-only faultNext('notify'),
// native/addon.c) frees its native struct once the handle object is
// collected: the construction drops the callback queue's strong reference on
// it (native/producer.c, producer_handle_new; native/admin.c,
// admin_handle_new). The probe runs in a child with --expose-gc and the
// allocator's poisoning, so a struct freed twice crashes it.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe } = require('./_probe');

test('a producer whose construction fails frees its native struct once collected, and the next one works', () => {
  const out = probe(`
    const { KafkaProducer, MockProducer, ProducerRecord } = require(D + '/producer');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const thrown = [];
      await (async () => {
        const makes = [
          () => new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } }),
          () => new MockProducer(),
        ];
        for (const make of makes) {
          native.faultNext('notify');
          try {
            make();
            thrown.push('constructed');
          } catch (e) {
            thrown.push(e.name + ': ' + e.message);
          }
        }
        // The step fails once: the next producer is built and works.
        const m = new MockProducer({ autoComplete: true, partitioner: null, keySerializer: null, valueSerializer: null });
        await (await m.send(new ProducerRecord({ topic: 't', value: Buffer.from('v') }))).get();
        await m.close();
      })();
      const armed = native.faultNext(null);
      await settle();
      console.log(JSON.stringify({ thrown, armed, leaks: leaks(), stats: native.producerStats() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.thrown, ['TypeError: self_ must not be null', 'TypeError: self_ must not be null']);
  assert.deepEqual(out.armed, []);
  assert.deepEqual(out.leaks, NO_LEAKS);
  assert.equal(out.stats.producersMade, 3);
});

test('an admin client whose construction fails frees its native struct once collected, and the next one works', () => {
  const out = probe(`
    const { Admin, MockAdminClient } = require(D + '/admin');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const before = native.adminStats();
      const thrown = [];
      await (async () => {
        const makes = [() => Admin.create({ 'bootstrap.servers': '127.0.0.1:1' }), () => new MockAdminClient()];
        for (const make of makes) {
          native.faultNext('notify');
          try {
            make();
            thrown.push('constructed');
          } catch (e) {
            thrown.push(e.name + ': ' + e.message);
          }
        }
        // The step fails once: the next client is built and works.
        const m = new MockAdminClient();
        await m.describeCluster().clusterId().get();
        await m.close();
      })();
      const armed = native.faultNext(null);
      let after = native.adminStats();
      for (let i = 0; i < 50 && after.adminsFreed - before.adminsFreed < 3; i++) {
        await settle(1);
        after = native.adminStats();
      }
      const d = (k) => after[k] - before[k];
      console.log(JSON.stringify({ thrown, armed, made: d('adminsMade'), freed: d('adminsFreed'), leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.thrown, ['TypeError: self_ must not be null', 'TypeError: self_ must not be null']);
  assert.deepEqual(out.armed, []);
  assert.equal(out.made, 3);
  assert.equal(out.freed, 3);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

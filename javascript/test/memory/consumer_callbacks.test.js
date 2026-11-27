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

// The consumer's rebalance listeners and their reports, read through the
// addon's test-only counts (consumerStats(), native/consumer.c): a listener's
// context is kept until a later successful subscribe replaces it, kept by a
// failed one, by assign() and unsubscribe() and by a KafkaConsumer's empty
// topic list, let go once a KafkaConsumer's close() returns and kept by a mock
// until its destroy; a commit callback's function goes once it reported or
// its commit failed, its context once it reported or at the destroy, so a
// closed consumer it references is collected; a report that waited for a
// Promise is freed once made;
// and everything is freed once the consumers are closed and collected. Each
// probe runs in a child with --expose-gc and the allocator's poisoning.

const assert = require('node:assert/strict');

const { NO_LEAKS, probeResult: probe } = require('./_probe');

test("a listener's context lives as the FFI's subscribe docs give it, on a KafkaConsumer and on a mock", () => {
  const out = probe(`
    const { CloseOptions, KafkaConsumer, MockConsumer } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    const live = () => { const s = native.consumerStats(); return s.listenersMade - s.listenersFreed; };
    const listener = () => ({ onPartitionsRevoked() {}, onPartitionsAssigned() {} });
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      await (async () => {
        const kc = new KafkaConsumer({ 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' });
        await kc.subscribe({ topics: ['t'], callback: listener() });
        counts.subscribed = live();
        await kc.subscribe({ topics: ['u'], callback: listener() });
        counts.replaced = live();
        // A failed subscribe keeps the earlier listener, and its own, which the core may hold.
        await kc.subscribe({ topics: [''], callback: listener() }).catch(() => {});
        counts.failed = live();
        // An empty topic list is unsubscribe(): it registers none.
        await kc.subscribe({ topics: [], callback: listener() });
        counts.empty = live();
        await kc.unsubscribe();
        counts.unsubscribed = live();
        // A listener-less subscribe replaces it.
        await kc.subscribe({ topics: ['t'] });
        counts.withoutListener = live();
        await kc.subscribe({ topics: ['t'], callback: listener() });
        await kc.close({ option: CloseOptions.timeout({ timeout: 0 }) });
        counts.closed = live();
        const m = new MockConsumer('earliest');
        await m.subscribe({ topics: ['t'], callback: listener() });
        await m.assign([new TopicPartition({ topic: 'x', partition: 0 })]).catch(() => {});
        await m.unsubscribe();
        counts.mockKept = live();
        await m.close();
        counts.mockClosed = live();
      })();
      await settle();
      counts.collected = live();
      console.log(JSON.stringify({ counts, leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.counts, {
    subscribed: 1,
    replaced: 1,
    failed: 2,
    empty: 2,
    unsubscribed: 2,
    withoutListener: 0,
    closed: 0,
    mockKept: 1,
    mockClosed: 1,
    collected: 0,
  });
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test("a failed subscribe's listener, which the mock may hold, is kept until a later successful subscribe", () => {
  const out = probe(`
    const { MockConsumer, SubscriptionPattern } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    const live = () => { const s = native.consumerStats(); return s.listenersMade - s.listenersFreed; };
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      const tp = new TopicPartition({ topic: 't', partition: 0 });
      await (async () => {
        // After assign(): the mock registers the listener before it refuses the subscribe.
        const m = new MockConsumer('earliest');
        const calls = [];
        const listener = (name) => ({ onPartitionsRevoked: () => { calls.push(name + ' revoked'); }, onPartitionsAssigned: () => { calls.push(name + ' assigned'); } });
        await m.assign([tp]);
        await m.subscribe({ topics: ['t'], callback: listener('L1') }).catch((e) => calls.push(e.name));
        counts.afterAssign = live();
        await m.rebalance([]).catch((e) => calls.push(e.name));
        await m.losePartitions([tp]).catch((e) => calls.push(e.name));
        await m.unsubscribe();
        // Topics, then a refused pattern.
        await m.subscribe({ topics: ['t'], callback: listener('L2') });
        await m.rebalance([tp]);
        await m.subscribe({ pattern: new SubscriptionPattern('t.*'), callback: listener('L3') }).catch((e) => calls.push(e.name));
        counts.afterPattern = live();
        await m.rebalance([]).catch((e) => calls.push(e.name));
        await m.losePartitions([tp]).catch((e) => calls.push(e.name));
        // A later successful subscribe releases them all but its own.
        await m.unsubscribe();
        await m.subscribe({ topics: ['t'], callback: listener('L4') });
        counts.replaced = live();
        counts.calls = calls;
        await m.close();
      })();
      await settle();
      counts.collected = live();
      console.log(JSON.stringify({ counts, leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.equal(out.counts.afterAssign, 1);
  assert.equal(out.counts.afterPattern, 2);
  assert.equal(out.counts.replaced, 1);
  assert.equal(out.counts.collected, 0);
  assert.equal(out.counts.calls[0], 'IllegalStateError');
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test('a closed consumer whose listener references it is collected: the listener is held through its handle object', () => {
  const out = probe(`
    const { CloseOptions, KafkaConsumer, MockConsumer } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const tp = new TopicPartition({ topic: 't', partition: 0 });
      await (async () => {
        for (let i = 0; i < 3; i++) {
          const m = new MockConsumer('earliest');
          // consumer-threading.md's pattern: a commit in onPartitionsRevoked.
          await m.subscribe({ topics: ['t'], callback: { onPartitionsRevoked: async () => { await m.commitSync(); }, onPartitionsAssigned() {} } });
          await m.rebalance([tp]);
          await m.rebalance([]);
          await m.close();
        }
        const kc = new KafkaConsumer({ 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' });
        await kc.subscribe({ topics: ['t'], callback: { onPartitionsRevoked: () => kc.commitSync(), onPartitionsAssigned() {} } });
        await kc.close({ option: CloseOptions.timeout({ timeout: 0 }) }).catch(() => {});
      })();
      await settle();
      console.log(JSON.stringify({ stats: native.consumerStats(), leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.equal(out.stats.consumersMade, 4);
  assert.equal(out.stats.consumersFreed, 4);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test("a listener's report that waited for its Promise is freed once made, and a thrown value once the core let go of it", () => {
  const out = probe(`
    const { MockConsumer } = require(D + '/consumer');
    const { TopicPartition } = require(D + '/common');
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      await (async () => {
        const m = new MockConsumer('earliest');
        const tp = new TopicPartition({ topic: 't', partition: 0 });
        let n = 0;
        await m.subscribe({ topics: ['t'], callback: {
          onPartitionsRevoked: () => new Promise((resolve) => setImmediate(resolve)),
          async onPartitionsAssigned() { if (++n % 2 === 0) throw new Error('boom ' + n); },
        } });
        for (let i = 0; i < 20; i++) await m.rebalance(i % 2 === 0 ? [tp] : []).catch(() => {});
        await settle();
        const s = native.consumerStats();
        counts.deferrals = s.deferralsMade;
        counts.pending = s.deferralsMade - s.deferralsFreed;
        await m.close();
      })();
      await settle();
      console.log(JSON.stringify({ counts, leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.ok(out.counts.deferrals >= 20, JSON.stringify(out.counts));
  assert.equal(out.counts.pending, 0);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test("a commit callback's context is freed once it reported, or at the destroy", () => {
  const out = probe(`
    const { CloseOptions, KafkaConsumer, MockConsumer, OffsetAndMetadata } = require(D + '/consumer');
    const { TopicPartition, TopicPartitionMap } = require(D + '/common');
    const live = () => { const s = native.consumerStats(); return s.commitCallbacksMade - s.commitCallbacksFreed; };
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      const tp = new TopicPartition({ topic: 't', partition: 0 });
      const offsets = new TopicPartitionMap([[tp, new OffsetAndMetadata(1n)]]);
      await (async () => {
        const m = new MockConsumer('earliest');
        await m.assign([tp]);
        // The mock runs each callback inside commitAsync(): reported at once,
        // or once its Promise settled, or with what it threw.
        await m.commitAsync({ offsets, callback: () => {} });
        await m.commitAsync({ offsets, callback: () => new Promise((resolve) => setImmediate(resolve)) });
        await m.commitAsync({ offsets, callback: () => { throw new Error('boom'); } }).catch(() => {});
        counts.mock = live();
        await m.close();
        // A KafkaConsumer with no broker: the commit never completes, so the
        // callback is kept until the destroy.
        const kc = new KafkaConsumer({ 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' });
        await kc.assign([tp]);
        await kc.commitAsync({ offsets, callback: () => {} });
        counts.kafkaPending = live();
        await kc.close({ option: CloseOptions.timeout({ timeout: 0 }) }).catch(() => {});
      })();
      await settle();
      counts.collected = live();
      console.log(JSON.stringify({ counts, leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.counts.mock, 0);
  assert.equal(out.counts.kafkaPending, 1);
  assert.equal(out.counts.collected, 0);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test("a refused commit's callback lets go of its function at once, and a closed consumer whose callbacks reference it is collected", () => {
  const out = probe(`
    const { CloseOptions, KafkaConsumer, OffsetAndMetadata } = require(D + '/consumer');
    const { TopicPartition, TopicPartitionMap } = require(D + '/common');
    const live = () => { const s = native.consumerStats(); return s.commitCallbacksMade - s.commitCallbacksFreed; };
    const configs = { 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' };
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = {};
      const tp = new TopicPartition({ topic: 't', partition: 0 });
      const refs = [];
      await (async () => {
        // Refused while a poll is pending: the core never runs the callbacks.
        const kc = new KafkaConsumer(configs);
        await kc.assign([tp]);
        const polling = kc.poll(2000);
        let refused = 0;
        const refuse = () => {
          const callback = () => kc.commitSync();
          refs.push(new WeakRef(callback));
          return kc.commitAsync({ callback });
        };
        for (let i = 0; i < 200; i++) await refuse().catch((e) => { if (e.name === 'ConcurrentModificationError') refused++; });
        await polling;
        counts.refused = refused;
        await settle();
        // Their contexts stay until the destroy; their functions are gone.
        counts.contexts = live();
        counts.functionsAlive = refs.filter((w) => w.deref() !== undefined).length;
        await kc.close({ option: CloseOptions.timeout({ timeout: 0 }) }).catch(() => {});
        // Refused on a closed consumer.
        const closed = new KafkaConsumer(configs);
        await closed.close({ option: CloseOptions.timeout({ timeout: 0 }) });
        await closed.commitAsync({ callback: () => closed.commitSync() }).catch(() => {});
        // Still pending at close(0).
        const pending = new KafkaConsumer(configs);
        await pending.assign([tp]);
        await pending.commitAsync({ offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(1)]]), callback: () => pending.commitSync() });
        await pending.close({ option: CloseOptions.timeout({ timeout: 0 }) }).catch(() => {});
      })();
      await settle();
      console.log(JSON.stringify({ counts, stats: native.consumerStats(), leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.deepEqual(out.counts, { refused: 200, contexts: 200, functionsAlive: 0 });
  assert.equal(out.stats.consumersMade, 3);
  assert.equal(out.stats.consumersFreed, 3);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

test('a closed consumer is collected when its last callback threw a value that reaches it: the value is held through its handle object', () => {
  const out = probe(`
    const { CloseOptions, KafkaConsumer, MockConsumer, OffsetAndMetadata } = require(D + '/consumer');
    const { TopicPartition, TopicPartitionMap } = require(D + '/common');
    const configs = { 'bootstrap.servers': '127.0.0.1:1', 'group.id': 'g', 'group.protocol': 'consumer' };
    (async () => {
      const hold = setInterval(() => {}, 1000);
      const counts = { thrown: 0 };
      const tp = new TopicPartition({ topic: 't', partition: 0 });
      await (async () => {
        // A mock's listener and commit callback: an Error made inside one, whose stack holds it and so its
        // closure, and a value that references the mock.
        for (const throwing of ['listener-error', 'listener-object', 'callback-error', 'callback-object']) {
          const m = new MockConsumer('earliest');
          const value = () => (throwing.endsWith('error') ? new Error('failed on ' + m.closed()) : { m });
          await m.subscribe({ topics: ['t'], callback: { onPartitionsRevoked() {}, onPartitionsAssigned() { if (throwing.startsWith('listener')) throw value(); } } });
          await m.rebalance([tp]).catch(() => counts.thrown++);
          if (throwing.startsWith('callback')) await m.commitAsync({ offsets: new TopicPartitionMap([[tp, new OffsetAndMetadata(1)]]), callback: () => { throw value(); } }).catch(() => counts.thrown++);
          await m.close();
        }
        // A KafkaConsumer's commit callback, run by the next commitSync().
        const kc = new KafkaConsumer(configs);
        await kc.assign([tp]);
        await kc.commitAsync({ offsets: new TopicPartitionMap(), callback: ({ exception }) => { if (exception === null) throw new Error('could not record the commit of ' + kc.groupMetadata().groupId()); } });
        await kc.commitSync({ offsets: new TopicPartitionMap() }).catch(() => counts.thrown++);
        await kc.close({ option: CloseOptions.timeout({ timeout: 0 }) }).catch(() => {});
      })();
      await settle();
      console.log(JSON.stringify({ counts, stats: native.consumerStats(), leaks: leaks() }));
      clearInterval(hold);
    })();
  `);
  assert.equal(out.counts.thrown, 5);
  assert.equal(out.stats.consumersMade, 5);
  assert.equal(out.stats.consumersFreed, 5);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

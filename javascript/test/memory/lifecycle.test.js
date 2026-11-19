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

// A producer's lifetime as a process sees it: the main thread ending with
// records still in the core (the environment's teardown, javascript/CLAUDE.md
// rule 10), and producers made, used and closed by the hundred, which leave
// no thread, descriptor, memory or native handle behind.

const assert = require('node:assert/strict');

const { NO_LEAKS, CRASH_TEXT, runProbe, lastJson } = require('./_probe');

/**
 * A child that opens a producer, sends 1,000 records that wait in the core
 * for metadata that never comes, and then ends as `end` says.
 *
 * @param {string} end the statement that ends the process
 * @returns {string} the probe's body
 */
const inFlight = (end) => `
  const { KafkaProducer, ProducerRecord } = require(D + '/producer');
  const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 60000, 'linger.ms': 60000 } });
  for (let i = 0; i < 1000; i++) {
    p.send({ record: new ProducerRecord({ topic: 'exit', key: Buffer.from('k' + i), value: Buffer.from('v' + i) }), callback: () => {} }).catch(() => {});
  }
  setTimeout(() => {
    console.log('ending');
    ${end}
  }, 300);
`;

test('the main thread exiting with records in flight ends with its own status, not a crash', () => {
  for (let round = 0; round < 3; round++) {
    const r = runProbe(inFlight('process.exit(3);'), { timeout: 60000 });
    assert.equal(r.signal, null, `round ${round}: killed by ${r.signal}\n${r.stderr}`);
    assert.equal(r.status, 3, `round ${round}:\n${r.stderr}`);
    assert.equal(r.stdout, 'ending\n');
    assert.doesNotMatch(r.stderr, CRASH_TEXT);
  }
});

test('an uncaught error with records in flight ends with status 1, not a crash', () => {
  for (let round = 0; round < 3; round++) {
    const r = runProbe(inFlight("throw new Error('the app failed');"), { timeout: 60000 });
    assert.equal(r.signal, null, `round ${round}: killed by ${r.signal}\n${r.stderr}`);
    assert.equal(r.status, 1, `round ${round}:\n${r.stderr}`);
    assert.equal(r.stdout, 'ending\n');
    assert.match(r.stderr, /Error: the app failed/);
    assert.doesNotMatch(r.stderr, CRASH_TEXT);
  }
});

// The thread and descriptor counts are read from /proc on Linux, and from
// ps and /dev/fd on macOS; elsewhere they are not counted.
const COUNTED = ['linux', 'darwin'].includes(process.platform);

// Under AddressSanitizer (`make test-asan`, which preloads gcc's libasan), RSS
// holds the sanitizer's quarantine of freed memory, a few hundred MB, so the
// RSS bound applies only without it.
const SANITIZED = /libasan/.test(process.env.LD_PRELOAD ?? '');

test('producers made, used and closed by the hundred leave no thread, descriptor, memory or handle behind', () => {
  const r = runProbe(
    `
    const { execSync } = require('node:child_process');
    const fs = require('node:fs');
    const { KafkaProducer, ProducerRecord } = require(D + '/producer');
    const threads = () =>
      process.platform === 'linux'
        ? fs.readdirSync('/proc/self/task').length
        : process.platform === 'darwin'
          ? execSync('ps -M -p ' + process.pid).toString().trim().split('\\n').length - 1
          : 0;
    const fds = () => (${JSON.stringify(COUNTED)} ? fs.readdirSync(process.platform === 'linux' ? '/proc/self/fd' : '/dev/fd').length : 0);
    const collected = new Set();
    const registry = new FinalizationRegistry((n) => collected.add(n));
    const swallow = () => {};
    let made = 0;
    // One producer: made, a record sent and failed, closed.
    async function one() {
      const p = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1', 'max.block.ms': 30 } });
      registry.register(p, made++);
      const f = await p.send({ record: new ProducerRecord({ topic: 'churn', value: Buffer.from('v') }), callback: () => {} });
      await f.get().catch(swallow);
      await p.close();
    }
    const sample = () => ({ threads: threads(), fds: fds(), rss: process.memoryUsage().rss });
    (async () => {
      const hold = setInterval(() => {}, 1000);
      // The first producer starts what the process keeps afterwards.
      await one();
      await settle();
      const start = sample();
      for (let i = 0; i < 200; i++) await one();
      await settle();
      const sequential = sample();
      for (let round = 0; round < 5; round++) {
        await Promise.all(Array.from({ length: 20 }, () => one()));
        await settle();
      }
      const concurrent = sample();
      // 20,000 sends refused after close() leave the heap as it was. Their
      // native contexts stay on the producer's orphan list until the closed
      // producer is collected (javascript/CLAUDE.md, rule 9), so they are
      // counted once it is.
      let heapGrowth = 0;
      await (async () => {
        const closed = new KafkaProducer({ configs: { 'bootstrap.servers': '127.0.0.1:1' } });
        registry.register(closed, made++);
        await closed.close();
        const refuse = async (n) => {
          for (let i = 0; i < n; i++) await closed.send(new ProducerRecord({ topic: 'refused', value: Buffer.from('v') })).catch(swallow);
        };
        await refuse(2000);
        await settle();
        const heapBefore = process.memoryUsage().heapUsed;
        await refuse(20000);
        await settle();
        heapGrowth = process.memoryUsage().heapUsed - heapBefore;
      })();
      await settle();
      console.log(JSON.stringify({
        made,
        collected: collected.size,
        start,
        sequential,
        concurrent,
        heapGrowth,
        leaks: leaks(),
      }));
      clearInterval(hold);
    })();
  `,
    { timeout: 600000 },
  );
  assert.equal(r.signal, null, `killed by ${r.signal}\n${r.stderr}`);
  assert.equal(r.status, 0, r.stderr);
  assert.doesNotMatch(r.stderr, CRASH_TEXT);
  const out = /** @type {Record<string, any>} */ (lastJson(r));
  // The warm-up producer, 200 one after another, 100 twenty at once and
  // the one that refused the sends.
  assert.equal(out.made, 302);
  assert.equal(out.collected, 302);
  // Fewer threads and descriptors left over than one live producer holds
  // (about 18 threads), and memory within a bound.
  for (const name of ['sequential', 'concurrent']) {
    assert.ok(out[name].threads - out.start.threads < 10, `${name}: ${out[name].threads - out.start.threads} threads left over`);
    assert.ok(out[name].fds - out.start.fds < 10, `${name}: ${out[name].fds - out.start.fds} descriptors left over`);
    if (!SANITIZED)
      assert.ok(out[name].rss - out.start.rss < 200 * 1024 * 1024, `${name}: RSS grew by ${out[name].rss - out.start.rss} bytes`);
  }
  assert.ok(out.heapGrowth < 4 * 1024 * 1024, `the refused sends grew the heap by ${out.heapGrowth} bytes`);
  assert.deepEqual(out.leaks, NO_LEAKS);
});

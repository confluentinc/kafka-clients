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

// ThroughputThrottler. Java has no test of its own; these are the Python
// tool's (python/verifiable_clients/test/test_throughput_throttler.py, the
// Rust tool's in rust/tools/verifiable-clients/src/throughput_throttler.rs plus
// the timed sleep and the deficit carried below the minimum sleep). Where the
// Python tests run throttle() on a thread and join it, these await its
// Promise, racing it against a timer.

const test = require('node:test');
const assert = require('node:assert/strict');

const { ThroughputThrottler, LONG_MAX, NS_PER_SEC } = require('../throughput_throttler');

/** Whether `promise` settled within `ms`. */
async function settlesWithin(promise, ms) {
  let timer;
  const timeout = new Promise((resolve) => {
    timer = setTimeout(() => resolve(false), ms);
  });
  try {
    return await Promise.race([promise.then(() => true), timeout]);
  } finally {
    clearTimeout(timer);
  }
}

test('never throttles with a negative target', () => {
  const throttler = new ThroughputThrottler(-1, 0);
  assert.equal(throttler.shouldThrottle(0, 0), false);
  assert.equal(throttler.shouldThrottle(1_000_000, 10_000), false);
});

test('does not throttle when no time has elapsed', () => {
  const throttler = new ThroughputThrottler(100, 1000);
  assert.equal(throttler.shouldThrottle(10, 1000), false);
});

test('throttles when the rate exceeds the target', () => {
  // One second elapsed: the rate is the amount.
  const throttler = new ThroughputThrottler(100, 0);
  assert.equal(throttler.shouldThrottle(200, 1000), true);
  assert.equal(throttler.shouldThrottle(50, 1000), false);
  // Exactly at the target is not over it.
  assert.equal(throttler.shouldThrottle(100, 1000), false);
});

test('throttles with a zero target', () => {
  // 0 is not < 0, so the rate check applies: any positive rate throttles.
  const throttler = new ThroughputThrottler(0, 0);
  assert.equal(throttler.shouldThrottle(1, 1000), true);
  assert.equal(throttler.shouldThrottle(0, 1000), false);
  assert.equal(throttler.shouldThrottle(1, 0), false);
});

test('the sleep time is NS_PER_SEC over the target', () => {
  assert.equal(new ThroughputThrottler(1000, 0).sleepTimeNs, NS_PER_SEC / 1000);
  assert.equal(new ThroughputThrottler(1, 0).sleepTimeNs, NS_PER_SEC);
  // Java's (long) cast truncates.
  assert.equal(new ThroughputThrottler(3, 0).sleepTimeNs, 333_333_333);
});

test('the sleep time is Long.MAX_VALUE when the target is not positive', () => {
  assert.equal(new ThroughputThrottler(0, 0).sleepTimeNs, LONG_MAX);
  assert.equal(new ThroughputThrottler(-5, 0).sleepTimeNs, LONG_MAX);
});

test('a zero target waits until wakeup', async () => {
  const throttler = new ThroughputThrottler(0, 0);
  const throttled = throttler.throttle();
  assert.equal(await settlesWithin(throttled, 200), false);
  throttler.wakeup();
  assert.equal(await settlesWithin(throttled, 5000), true);
});

test('a zero target returns at once if already woken', async () => {
  // Java never resets the flag on this path, so it stays set.
  const throttler = new ThroughputThrottler(0, 0);
  throttler.wakeup();
  assert.equal(await settlesWithin(throttler.throttle(), 5000), true);
  assert.equal(await settlesWithin(throttler.throttle(), 5000), true);
});

test('the timed path sleeps for the deficit', async () => {
  // 10/sec: each throttle() adds 100 ms of deficit and sleeps it off.
  const throttler = new ThroughputThrottler(10, 0);
  const started = performance.now();
  await throttler.throttle();
  assert.ok(performance.now() - started >= 90, `${performance.now() - started}`);
  assert.equal(throttler.sleepDeficitNs, 0);
});

test('a deficit below the minimum sleep is carried', async () => {
  // 1000/sec: 1 ms a throttle(), under the 2 ms minimum, so the first call
  // does not sleep and the second sleeps both off.
  const throttler = new ThroughputThrottler(1000, 0);
  await throttler.throttle();
  assert.equal(throttler.sleepDeficitNs, 1_000_000);
  await throttler.throttle();
  assert.equal(throttler.sleepDeficitNs, 0);
});

test('the timed path sleeps, then wakeup interrupts it', async () => {
  // 1/sec: one throttle() sleeps about 1 s unless woken.
  const throttler = new ThroughputThrottler(1, 0);
  const started = performance.now();
  const throttled = throttler.throttle();
  await new Promise((resolve) => setTimeout(resolve, 50));
  throttler.wakeup();
  assert.equal(await settlesWithin(throttled, 5000), true);
  assert.ok(performance.now() - started < 500, `${performance.now() - started}`);
  // The timed path resets the flag, so the next throttle() sleeps again.
  assert.equal(throttler.wakeupFlag, false);
});

test('a wakeup before the timed throttle() is not lost', async () => {
  const throttler = new ThroughputThrottler(1, 0);
  throttler.wakeup();
  const started = performance.now();
  await throttler.throttle();
  assert.ok(performance.now() - started < 500, `${performance.now() - started}`);
});

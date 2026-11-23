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

// Translated from org.apache.kafka.server.util.ThroughputThrottler, the
// verifiable producer's one dependency outside the client: Java keeps it in
// server-common, which has no JavaScript counterpart. The twin of
// python/verifiable_clients/throughput_throttler.py.

const NS_PER_MS = 1_000_000;
const NS_PER_SEC = 1000 * NS_PER_MS;
const MIN_SLEEP_NS = 2 * NS_PER_MS;
// Java's Long.MAX_VALUE, as the sleep time of a throttler that never sleeps
// on its timed path.
const LONG_MAX = 2n ** 63n - 1n;

/** The monotonic clock, in nanoseconds (Java's System.nanoTime()). */
function nanoTime() {
  return Number(process.hrtime.bigint());
}

/**
 * This class helps producers throttle throughput.
 *
 * If `targetThroughput >= 0`, the resulting average throughput will be
 * approximately `min(targetThroughput, maximumPossibleThroughput)`. If
 * `targetThroughput < 0`, no throttling will occur.
 *
 * To use, do this between successive send attempts:
 *
 *     if (throttler.shouldThrottle(amountSoFar, sendStartMs)) {
 *       await throttler.throttle();
 *     }
 *
 * Note that this can be used to throttle message throughput or data
 * throughput.
 *
 * Java's `synchronized` / `wait` / `notifyAll` on the instance become a timer
 * and the Promise `throttle()` returns: JavaScript cannot block its one
 * thread, so `throttle()` is awaited, and `wakeup()` settles it at once. Java's
 * InterruptedException paths have no counterpart.
 */
class ThroughputThrottler {
  /**
   * @param {number} targetThroughput - can be messages/sec or bytes/sec
   * @param {number} startMs - when the very first message is sent
   */
  constructor(targetThroughput, startMs) {
    this.startMs = startMs;
    this.targetThroughput = targetThroughput;
    // Java's (long) cast truncates; Long.MAX_VALUE when not positive.
    this.sleepTimeNs = targetThroughput > 0 ? Math.trunc(NS_PER_SEC / targetThroughput) : LONG_MAX;
    this.sleepDeficitNs = 0;
    this.wakeupFlag = false;
    // The pending throttle()'s settle function, which wakeup() calls.
    this.waiter = null;
  }

  /**
   * @param {number} amountSoFar - bytes produced so far if you want to
   *   throttle data throughput, or messages produced so far if you want to
   *   throttle message throughput
   * @param {number} sendStartMs - timestamp of the most recently sent message
   * @returns {boolean} whether throttling should happen
   */
  shouldThrottle(amountSoFar, sendStartMs) {
    if (this.targetThroughput < 0) {
      // No throttling in this case
      return false;
    }
    // Java computes the elapsed seconds as a float: (sendStartMs - startMs) / 1000.f
    const elapsedSec = Math.fround(Math.fround(sendStartMs - this.startMs) / 1000);
    return elapsedSec > 0 && amountSoFar / elapsedSec > this.targetThroughput;
  }

  /**
   * Occasionally waits for small amounts of time to achieve
   * `targetThroughput`. Note that if `targetThroughput` is 0, this waits
   * until `wakeup()` is called.
   *
   * @returns {Promise<void>} settling when the wait ends
   */
  async throttle() {
    if (this.targetThroughput === 0) {
      // Java waits while !wakeup and never resets the flag on this path.
      while (!this.wakeupFlag) await this.#wait(null);
      return;
    }

    // throttle throughput by sleeping, on average,
    // (1 / this.throughput) seconds between "things sent"
    this.sleepDeficitNs += Number(this.sleepTimeNs);

    // If enough sleep deficit has accumulated, sleep a little
    if (this.sleepDeficitNs >= MIN_SLEEP_NS) {
      const sleepStartNs = nanoTime();
      let remaining = this.sleepDeficitNs;
      while (!this.wakeupFlag && remaining > 0) {
        await this.#wait(remaining / NS_PER_MS);
        remaining = this.sleepDeficitNs - (nanoTime() - sleepStartNs);
      }
      this.wakeupFlag = false;
      this.sleepDeficitNs = 0;
    }
  }

  /** Wakeup the throttler if it is sleeping. */
  wakeup() {
    this.wakeupFlag = true;
    const waiter = this.waiter;
    this.waiter = null;
    if (waiter !== null) waiter();
  }

  // Java's wait(ms) (or wait() for null): settles after `ms`, or at wakeup().
  #wait(ms) {
    return new Promise((resolve) => {
      let timer = null;
      const settle = () => {
        clearTimeout(timer);
        if (this.waiter === settle) this.waiter = null;
        resolve();
      };
      this.waiter = settle;
      // An untimed wait still holds a timer, so the event loop stays alive
      // until wakeup(): a signal's listener alone does not keep it running.
      timer = ms === null ? setInterval(() => {}, 1 << 30) : setTimeout(settle, Math.max(0, ms));
    });
  }
}

module.exports = { ThroughputThrottler, LONG_MAX, NS_PER_SEC };

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

// src/internal/duration.ts, the conversion of every Duration parameter, as
// python/confluent_kafka/_config.py's duration_to_ms is tested: whole
// milliseconds rounded down as Java's Duration.toMillis(), Java's
// negative-timeout message, Infinity as no limit, and the bound of the
// milliseconds a number holds exactly. A Temporal.Duration is tested
// with the @js-temporal/polyfill, and with the built-in Temporal where Node has
// one (Node 26).

const test = require('node:test');
const assert = require('node:assert/strict');
const { Temporal: Polyfill } = require('@js-temporal/polyfill');

const { durationToMillis, UNBOUNDED_MILLIS } = require('../../../dist/internal/duration');
const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');

const TEMPORALS = [['the polyfill', Polyfill]];
if (globalThis.Temporal !== undefined) TEMPORALS.push(['the built-in Temporal', globalThis.Temporal]);

const NOT_A_DURATION = 'timeout must be a number of milliseconds or a Temporal.Duration';

test('a number is milliseconds, rounded down as Java\'s toMillis()', () => {
  assert.equal(durationToMillis(0, 'timeout'), 0);
  assert.ok(Object.is(durationToMillis(-0, 'timeout'), 0));
  assert.equal(durationToMillis(1500, 'timeout'), 1500);
  // A fraction is floored, as the Temporal path and the Python client do.
  assert.equal(durationToMillis(1.5, 'timeout'), 1);
  assert.equal(durationToMillis(4999.99875, 'timeout'), 4999);
  assert.ok(Object.is(durationToMillis(0.25, 'timeout'), 0));
  assert.equal(durationToMillis(Number.MAX_SAFE_INTEGER, 'timeout'), Number.MAX_SAFE_INTEGER);
});

test("Infinity is no limit: Java's Long.MAX_VALUE milliseconds, exactly", () => {
  assert.equal(durationToMillis(Infinity, 'timeout'), 9223372036854775807n);
  assert.equal(UNBOUNDED_MILLIS, 9223372036854775807n);
});

test('a negative timeout is returned as it is, for the FFI to check: the binding repeats no precondition', () => {
  // Rounded down as a positive one is: -0.5 is -1 ms.
  assert.equal(durationToMillis(-1, 'timeout'), -1);
  assert.equal(durationToMillis(-0.5, 'timeout'), -1);
  assert.equal(durationToMillis(-5, 'timeout'), -5);
  assert.equal(durationToMillis(-Number.MAX_SAFE_INTEGER, 'timeout'), -Number.MAX_SAFE_INTEGER);
  assert.equal(durationToMillis(Polyfill.Duration.from({ milliseconds: -3 }), 'timeout'), -3);
  // A count a number does not hold exactly is a RangeError either way.
  assert.throws(() => durationToMillis(-(2 ** 60), 'timeout'), {
    name: 'RangeError',
    message: 'timeout of -1152921504606847000 ms is below -Number.MAX_SAFE_INTEGER ms',
  });
});

test('NaN, -Infinity and a number above Number.MAX_SAFE_INTEGER are RangeErrors', () => {
  for (const value of [NaN, -Infinity]) {
    assert.throws(() => durationToMillis(value, 'timeout'), {
      name: 'RangeError',
      message: `timeout must be a number of milliseconds, not ${value}`,
    });
  }
  for (const [value, shown] of [
    [Number.MAX_SAFE_INTEGER + 1, '9007199254740992'],
    [2 ** 63, '9223372036854776000'],
  ]) {
    assert.throws(() => durationToMillis(value, 'timeout'), {
      name: 'RangeError',
      message: `timeout of ${shown} ms is above Number.MAX_SAFE_INTEGER ms; pass Infinity for no limit`,
    });
  }
});

test('null, undefined and any other type are TypeErrors', () => {
  for (const value of ['5', 5n, true, {}, [], () => 5]) {
    assert.throws(() => durationToMillis(value, 'timeout'), { name: 'TypeError', message: NOT_A_DURATION });
  }
  assert.throws(() => durationToMillis(null, 'timeout'), { name: 'TypeError', message: 'timeout must not be null' });
  assert.throws(() => durationToMillis(undefined, 'timeout'), {
    name: 'TypeError',
    message: "timeout must not be undefined. Leave the key out, or pass null for Java's null",
  });
  // Another Temporal type is not a Duration.
  assert.throws(() => durationToMillis(Polyfill.Instant.fromEpochMilliseconds(0), 'timeout'), { name: 'TypeError', message: NOT_A_DURATION });
  // The parameter's name is the one given.
  assert.throws(() => durationToMillis('x', 'maxWait'), {
    name: 'TypeError',
    message: 'maxWait must be a number of milliseconds or a Temporal.Duration',
  });
});

for (const [label, Temporal] of TEMPORALS) {
  test(`a Temporal.Duration (${label}) is its total milliseconds, rounded down`, () => {
    assert.equal(durationToMillis(Temporal.Duration.from({ seconds: 1, milliseconds: 500 }), 'timeout'), 1500);
    assert.equal(durationToMillis(Temporal.Duration.from({ milliseconds: 1, microseconds: 999, nanoseconds: 999 }), 'timeout'), 1);
    assert.equal(durationToMillis(Temporal.Duration.from({ nanoseconds: 999999 }), 'timeout'), 0);
    assert.equal(durationToMillis(new Temporal.Duration(), 'timeout'), 0);
    assert.equal(durationToMillis(Temporal.Duration.from({ days: 1, hours: 2 }), 'timeout'), 26 * 3600 * 1000);
    assert.equal(durationToMillis(Temporal.Duration.from('PT0.25S'), 'timeout'), 250);
  });

  test(`a negative Temporal.Duration (${label}) is a negative count, as is one rounding below zero`, () => {
    assert.equal(durationToMillis(Temporal.Duration.from({ seconds: -1 }), 'timeout'), -1000);
    assert.equal(durationToMillis(Temporal.Duration.from({ nanoseconds: -1 }), 'timeout'), -1);
  });

  test(`a Temporal.Duration (${label}) with years, months or weeks is a RangeError`, () => {
    for (const fields of [{ years: 1 }, { months: 1 }, { weeks: 1 }, { weeks: -1 }]) {
      assert.throws(() => durationToMillis(Temporal.Duration.from(fields), 'timeout'), (e) => {
        assert.equal(e.constructor, RangeError);
        assert.ok(!(e instanceof IllegalArgumentError));
        assert.equal(e.message, 'timeout must not hold years, months or weeks, which have no fixed length');
        return true;
      });
    }
  });

  test(`a Temporal.Duration (${label}) above Number.MAX_SAFE_INTEGER ms is a RangeError`, () => {
    assert.equal(durationToMillis(Temporal.Duration.from({ milliseconds: Number.MAX_SAFE_INTEGER }), 'timeout'), Number.MAX_SAFE_INTEGER);
    assert.throws(() => durationToMillis(Temporal.Duration.from({ seconds: Number.MAX_SAFE_INTEGER }), 'timeout'), {
      name: 'RangeError',
      message: /^timeout of \d+ ms is above Number\.MAX_SAFE_INTEGER ms; pass Infinity for no limit$/,
    });
  });
}

test('a Temporal.Duration is recognised by its toStringTag alone', () => {
  // Another realm's, or another polyfill's, Duration: what the helper reads.
  const foreign = (millis) => ({
    [Symbol.toStringTag]: 'Temporal.Duration',
    years: 0,
    months: 0,
    weeks: 0,
    total: ({ unit }) => (unit === 'millisecond' ? millis : NaN),
  });
  assert.equal(durationToMillis(foreign(42.9), 'timeout'), 42);
  assert.throws(() => durationToMillis(foreign(2 ** 64), 'timeout'), {
    name: 'RangeError',
    message: 'timeout of 18446744073709552000 ms is above Number.MAX_SAFE_INTEGER ms; pass Infinity for no limit',
  });
});

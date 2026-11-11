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

// A Java `Duration` parameter -> whole milliseconds (private), as
// python/confluent_kafka/_config.py's `duration_to_ms`: the one conversion of
// every `Duration` the client takes (the public `Duration` type, duration.ts).
//
// A `Duration` is a number of milliseconds or a `Temporal.Duration`. A
// `Temporal.Duration` is recognised by its `Symbol.toStringTag`, so Node 26's
// built-in `Temporal` and the polyfills of Node 22-25 are both accepted,
// whether or not `globalThis.Temporal` exists. It is read with
// `total({ unit: 'millisecond' })`; its years, months and weeks have no fixed
// length, so a duration holding any is refused with a `RangeError`, as Java's
// `Duration` has no such units. A number, or the total, is rounded down, as Java's `toMillis()`.
// `Infinity` is no limit: Java's `Long.MAX_VALUE` milliseconds, exactly, as a
// bigint (a number cannot hold it). `NaN`, `-Infinity` and a count of
// milliseconds a number does not hold exactly (above
// `Number.MAX_SAFE_INTEGER`) are `RangeError`s.

import { illegalArgument } from './errors';

/** Java's message for a negative timeout (`KafkaProducer.close(Duration)`). */
export const NEGATIVE_TIMEOUT_MESSAGE = 'The timeout cannot be negative.';

/**
 * Java's `Long.MAX_VALUE` milliseconds, what `Infinity` (no limit) crosses as:
 * Java's untimed `close()` is `close(Duration.ofMillis(Long.MAX_VALUE))`.
 */
export const UNBOUNDED_MILLIS = 9223372036854775807n;

/** The parts of a `Temporal.Duration` the conversion reads. */
interface TemporalDurationLike {
  readonly years: number;
  readonly months: number;
  readonly weeks: number;
  total(options: { unit: 'millisecond' }): number;
}

export function isTemporalDuration(value: unknown): value is TemporalDurationLike {
  return (
    typeof value === 'object' &&
    value !== null &&
    (value as { [Symbol.toStringTag]?: unknown })[Symbol.toStringTag] === 'Temporal.Duration'
  );
}

/**
 * Whether `value` is a timeout as a bare value takes it: a number, a
 * `Temporal.Duration` or `null` (the form matcher's test, src/internal/args.ts).
 */
export function isDurationOrNull(value: unknown): boolean {
  return value === null || typeof value === 'number' || isTemporalDuration(value);
}

/**
 * A `Duration` parameter in whole milliseconds, rounded down as Java's
 * `Duration.toMillis()`.
 *
 * @param value - the parameter: a number of milliseconds (`Infinity`: no
 *   limit) or a `Temporal.Duration`
 * @param name - the parameter's name, for the messages
 * @returns the duration in milliseconds, a number from 0 to
 *   `Number.MAX_SAFE_INTEGER`, or `UNBOUNDED_MILLIS` for `Infinity`
 * @throws `TypeError` when `value` is `null` (Java's `timeout.toMillis()` on a
 *   null), `undefined`, or neither a number nor a `Temporal.Duration`
 * @throws `IllegalArgumentError` when the duration is negative, with Java's
 *   message
 * @throws `RangeError` when `value` is `NaN` or `-Infinity`, holds years,
 *   months or weeks, or the duration is above `Number.MAX_SAFE_INTEGER`
 *   milliseconds, which a number does not hold exactly
 */
export function durationToMillis(value: unknown, name: string): number | bigint {
  return toMillis(value, name, false);
}

/**
 * A wait's `Duration` parameter in whole milliseconds, as `durationToMillis`
 * reads it, except that a negative duration is no error: it is returned as it
 * is, and the caller waits for no time, as Java's
 * `CompletableFuture.get(timeout, unit)` does.
 *
 * @param value - the parameter: a number of milliseconds (`Infinity`: no
 *   limit) or a `Temporal.Duration`
 * @param name - the parameter's name, for the messages
 * @returns the duration in milliseconds, a number from
 *   `-Number.MAX_SAFE_INTEGER` to `Number.MAX_SAFE_INTEGER`, or
 *   `UNBOUNDED_MILLIS` for `Infinity`
 * @throws `TypeError` when `value` is `null`, `undefined`, or neither a number
 *   nor a `Temporal.Duration`
 * @throws `RangeError` when `value` is `NaN` or `-Infinity`, holds years,
 *   months or weeks, or the duration is beyond `Number.MAX_SAFE_INTEGER`
 *   milliseconds either way
 */
export function waitDurationToMillis(value: unknown, name: string): number | bigint {
  return toMillis(value, name, true);
}

// The conversion both read a Duration with: a negative count is Java's
// negative-timeout error first, unless `negative` lets it through.
function toMillis(value: unknown, name: string, negative: boolean): number | bigint {
  let millis: number;
  if (typeof value === 'number') {
    if (value === Infinity) return UNBOUNDED_MILLIS;
    if (Number.isNaN(value) || value === -Infinity) {
      throw new RangeError(`${name} must be a number of milliseconds, not ${value}`);
    }
    millis = Math.floor(value);
  } else if (isTemporalDuration(value)) {
    if (value.years !== 0 || value.months !== 0 || value.weeks !== 0) {
      throw new RangeError(`${name} must not hold years, months or weeks, which have no fixed length`);
    }
    millis = Math.floor(value.total({ unit: 'millisecond' }));
  } else if (value === null) {
    throw new TypeError(`${name} must not be null`);
  } else if (value === undefined) {
    throw new TypeError(`${name} must not be undefined. Leave the key out, or pass null for Java's null`);
  } else {
    throw new TypeError(`${name} must be a number of milliseconds or a Temporal.Duration`);
  }
  if (millis < 0 && !negative) throw illegalArgument(NEGATIVE_TIMEOUT_MESSAGE);
  if (millis > Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${name} of ${millis} ms is above Number.MAX_SAFE_INTEGER ms; pass Infinity for no limit`);
  }
  if (millis < -Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${name} of ${millis} ms is below -Number.MAX_SAFE_INTEGER ms`);
  }
  // A -0 is 0 milliseconds.
  return millis === 0 ? 0 : millis;
}

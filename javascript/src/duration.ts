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

// Java's java.time.Duration as the client takes it. This module holds only
// its type; src/internal/duration.ts converts it.

/**
 * A length of time, Java's `java.time.Duration`: a number of milliseconds, or
 * a `Temporal.Duration` (built into Node from 26.0.0; a polyfill's, such as
 * `@js-temporal/polyfill`, before), recognised by its `Symbol.toStringTag`.
 * Either is rounded down to whole milliseconds, as Java's `toMillis()`.
 * `Infinity` means no limit (Java's `Long.MAX_VALUE` milliseconds); `NaN`,
 * `-Infinity`, a number above `Number.MAX_SAFE_INTEGER` and a
 * `Temporal.Duration` holding years, months or weeks, which have no fixed
 * length, are refused with a `RangeError`, a negative duration with an
 * `IllegalArgumentError`, and a `null` timeout with
 * `TypeError('timeout must not be null')`.
 *
 * @remarks *(js-only)* The JavaScript form of Java's `java.time.Duration`: a
 * number is milliseconds, and a `Temporal.Duration` is typed by its
 * structure, so the built-in one and any polyfill's are accepted.
 */
export type Duration =
  | number
  | {
      readonly [Symbol.toStringTag]: 'Temporal.Duration';
      total(options: { unit: 'millisecond' }): number;
    };

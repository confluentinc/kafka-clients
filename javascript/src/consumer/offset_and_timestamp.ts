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

// Java's OffsetAndTimestamp, a class with methods (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): `offset` and `timestamp` are
// Java `long`s, so bigints, and `leaderEpoch` is Java's `Optional<Integer>`,
// `number | null` (Types). `equals`, `hashCode` and `toString` are Java's bodies.

import { JavaForms } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { checkInt, checkLong } from '../internal/java_number';
import { longHashCode, sameClass } from '../internal/java_object';

// Java's constructors, in Java declaration order (src/internal/args.ts).
const FORMS = new JavaForms('OffsetAndTimestamp', [
  ['offset', 'timestamp'],
  ['offset', 'timestamp', 'leaderEpoch'],
]);
const OFFSET_FORMS = new JavaForms('offset', [[]], { reader: true });
const TIMESTAMP_FORMS = new JavaForms('timestamp', [[]], { reader: true });
const LEADER_EPOCH_FORMS = new JavaForms('leaderEpoch', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * A container class for offset and timestamp.
 */
export class OffsetAndTimestamp {
  readonly #timestamp: bigint;

  readonly #offset: bigint;

  readonly #leaderEpoch: number | null;

  /**
   * Constructs a new OffsetAndTimestamp with the given offset and timestamp.
   *
   * @param options - the offset and the timestamp
   * @throws `IllegalArgumentError` If the offset or timestamp is negative,
   *   or the given parameters are no form's
   * @throws `TypeError` when the offset or the timestamp is neither a bigint
   *   nor an integer, or the epoch neither an integer nor null
   * @throws `RangeError` when a value is beyond its Java type, or a number
   *   beyond 2^53 - 1
   */
  constructor(options: {
    /** The offset: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    leaderEpoch?: never;
  });
  /**
   * Constructs a new OffsetAndTimestamp with the given offset, timestamp, and
   * leader epoch.
   *
   * @param options - the offset, the timestamp and the leader epoch
   * @throws `IllegalArgumentError` If the offset or timestamp is negative
   */
  constructor(options: {
    /** The offset: a bigint, or a whole number up to 2^53 - 1. */
    offset: bigint | number;
    /** The timestamp: a bigint, or a whole number up to 2^53 - 1. */
    timestamp: bigint | number;
    /** The leader epoch, or null for none. */
    leaderEpoch: number | null;
  });
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const offset = checkLong(FORMS.value(options, 'offset'), 'offset');
    const timestamp = checkLong(FORMS.value(options, 'timestamp'), 'timestamp');
    const epoch = form === 1 ? FORMS.value(options, 'leaderEpoch') : null;
    const leaderEpoch = epoch === null ? null : checkInt(epoch, 'leaderEpoch');
    if (offset < 0n) throw illegalArgument('Invalid negative offset');
    if (timestamp < 0n) throw illegalArgument('Invalid negative timestamp');
    this.#offset = offset;
    this.#timestamp = timestamp;
    this.#leaderEpoch = leaderEpoch;
  }

  /**
   * The timestamp.
   *
   * @returns the timestamp, a bigint
   * @throws `TypeError` when it is given an argument
   */
  timestamp(): bigint {
    TIMESTAMP_FORMS.match(undefined, arguments.length);
    return this.#timestamp;
  }

  /**
   * The offset.
   *
   * @returns the offset, a bigint
   * @throws `TypeError` when it is given an argument
   */
  offset(): bigint {
    OFFSET_FORMS.match(undefined, arguments.length);
    return this.#offset;
  }

  /**
   * Get the leader epoch corresponding to the offset that was found (if one
   * exists). This can be provided to seek() to ensure that the log hasn't been
   * truncated prior to fetching.
   *
   * @returns The leader epoch or null if it is not known
   * @throws `TypeError` when it is given an argument
   */
  leaderEpoch(): number | null {
    LEADER_EPOCH_FORMS.match(undefined, arguments.length);
    return this.#leaderEpoch;
  }

  /**
   * The offset and timestamp as Java writes them:
   * `(timestamp=100, leaderEpoch=5, offset=10)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(timestamp=${this.#timestamp}, leaderEpoch=${String(this.#leaderEpoch)}, offset=${this.#offset})`;
  }

  /**
   * Whether `o` is an `OffsetAndTimestamp` of the same timestamp, offset and
   * leader epoch, Java's `equals`.
   *
   * @param options - the object to compare with
   * @returns true when `o` is equal
   * @throws `IllegalArgumentError` when the given parameters are not `{o}`
   */
  equals(options: {
    /** The object to compare with. */
    o: unknown;
  }): boolean;
  /**
   * The same as `equals({o})`, with the object on its own; a plain object is
   * read as the options object.
   *
   * @param o - the object to compare with
   * @returns true when `o` is equal
   */
  equals(o: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const o = EQUALS_FORMS.value(options, 'o');
    if (this === o) return true;
    if (o === null || o === undefined || !sameClass(this, o)) return false;
    const that = o as OffsetAndTimestamp;
    return this.#timestamp === that.#timestamp && this.#offset === that.#offset && this.#leaderEpoch === that.#leaderEpoch;
  }

  /**
   * Java's `Objects.hash(timestamp, offset, leaderEpoch)`.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    h = (Math.imul(31, h) + longHashCode(this.#timestamp)) | 0;
    h = (Math.imul(31, h) + longHashCode(this.#offset)) | 0;
    // Optional.hashCode: the value's, 0 when empty.
    return (Math.imul(31, h) + (this.#leaderEpoch ?? 0)) | 0;
  }

  /**
   * The offset and timestamp as `JSON.stringify` writes them: `{ offset,
   * timestamp, leaderEpoch }`, each `long` a decimal string. It ignores the
   * key `JSON.stringify` passes.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol: `JSON.stringify` throws
   * on a `bigint`, and a `number` loses digits above 2^53.
   */
  toJSON(): { offset: string; timestamp: string; leaderEpoch: number | null } {
    return { offset: this.#offset.toString(), timestamp: this.#timestamp.toString(), leaderEpoch: this.#leaderEpoch };
  }
}

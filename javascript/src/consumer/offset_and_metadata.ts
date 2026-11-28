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

// Java's OffsetAndMetadata, a data class (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family): `offset`, `metadata` and `leaderEpoch` are
// readonly fields, and the object is frozen when it is created. `offset` is a
// `long`, so a bigint (Types); `leaderEpoch` is Java's
// `Optional<Integer>` getter, `number | null`, so a negative epoch reads as
// null, as Java's getter returns `Optional.empty()` for it. `equals`,
// `hashCode` and `toString` are Java's bodies.

import { JavaForms } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { checkInt, checkLong } from '../internal/java_number';
import { longHashCode, objectHashCode, sameClass } from '../internal/java_object';

const isLong = (value: unknown): boolean => typeof value === 'bigint' || typeof value === 'number';
const FORMS = new JavaForms('OffsetAndMetadata', [['offset', 'leaderEpoch', 'metadata'], ['offset', 'metadata'], ['offset']], {
  bareTypes: { offset: isLong },
});
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * The Kafka offset commit API allows users to provide additional metadata
 * (in the form of a string) when an offset is committed. This can be useful
 * (for example) to store information about which node made the commit, what
 * time the commit was made, etc.
 *
 * Java: `org.apache.kafka.clients.consumer.OffsetAndMetadata`.
 */
export class OffsetAndMetadata {
  /** The offset to be committed, Java's `offset()`. */
  readonly offset: bigint;

  /** The metadata of the previously consumed record, or `''`, Java's `metadata()`. */
  readonly metadata: string;

  /**
   * The leader epoch of the previously consumed record, or null if not known,
   * Java's `leaderEpoch()`. Log truncation is detected if there exists a
   * leader epoch which is larger than this epoch and begins at an offset
   * earlier than the committed offset.
   */
  readonly leaderEpoch: number | null;

  /**
   * Makes `OffsetAndMetadata` nominal: TypeScript checks it by name, not by
   * shape.
   */
  readonly #brand = true;

  /**
   * Construct a new OffsetAndMetadata object for committing through
   * `KafkaConsumer`.
   *
   * @param options - the offset to be committed, the optional leader epoch of
   *   the last consumed record, and the metadata
   * @throws `IllegalArgumentError` when the offset is negative ("Invalid
   *   negative offset"), or the given parameters are no form's
   * @throws `TypeError` when the offset is neither a bigint nor an integer,
   *   the epoch neither an integer nor null, or the metadata neither a string
   *   nor null
   * @throws `RangeError` when a value is beyond its Java type
   *
   * @remarks A null `metadata` is stored as `''`, as the server does.
   */
  constructor(options: {
    /** The offset to be committed. */
    offset: bigint | number;
    /** Optional leader epoch of the last consumed record: null for none. */
    leaderEpoch: number | null;
    /** Non-null metadata; null reads as `''`. */
    metadata: string | null;
  });
  /**
   * Construct a new OffsetAndMetadata object for committing through
   * `KafkaConsumer`, with no leader epoch.
   *
   * @param options - the offset to be committed and the metadata
   */
  constructor(options: {
    /** The offset to be committed. */
    offset: bigint | number;
    leaderEpoch?: never;
    /** Non-null metadata; null reads as `''`. */
    metadata: string | null;
  });
  /**
   * Construct a new OffsetAndMetadata object for committing through
   * `KafkaConsumer`. The metadata associated with the commit will be empty.
   *
   * @param offset - The offset to be committed
   */
  constructor(offset: bigint | number);
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const offset = checkLong(FORMS.value(options, 'offset'), 'offset');
    const epoch = form === 0 ? FORMS.value(options, 'leaderEpoch') : null;
    const metadata = form === 2 ? '' : FORMS.value(options, 'metadata');
    if (offset < 0n) throw illegalArgument('Invalid negative offset');
    const leaderEpoch = epoch === null ? null : checkInt(epoch, 'leaderEpoch');
    if (metadata !== null && typeof metadata !== 'string') throw new TypeError('metadata must be a string or null');
    this.offset = offset;
    this.leaderEpoch = leaderEpoch === null || leaderEpoch < 0 ? null : leaderEpoch;
    this.metadata = metadata ?? '';
    Object.freeze(this);
  }

  /**
   * Whether `o` is an `OffsetAndMetadata` of the same offset, metadata and
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
   * The same as `equals({o})`, with the object on its own; a plain object
   * is read as the options object.
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
    const that = o as OffsetAndMetadata;
    return this.offset === that.offset && this.metadata === that.metadata && this.leaderEpoch === that.leaderEpoch;
  }

  /**
   * Java's `Objects.hash(offset, metadata, leaderEpoch())`.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    h = (Math.imul(31, h) + longHashCode(this.offset)) | 0;
    h = (Math.imul(31, h) + objectHashCode(this.metadata)) | 0;
    // Optional.hashCode: the value's, 0 when empty.
    return (Math.imul(31, h) + (this.leaderEpoch ?? 0)) | 0;
  }

  /**
   * The offset and metadata as Java writes them:
   * `OffsetAndMetadata{offset=10, leaderEpoch=null, metadata='m'}`.
   *
   * @returns the text
   */
  toString(): string {
    return `OffsetAndMetadata{offset=${this.offset}, leaderEpoch=${String(this.leaderEpoch)}, metadata='${this.metadata}'}`;
  }

  /**
   * The offset and metadata as `JSON.stringify` writes them: `{ offset,
   * leaderEpoch, metadata }`, the offset as a decimal string. It ignores the
   * key `JSON.stringify` passes.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { offset: string; leaderEpoch: number | null; metadata: string } {
    return { offset: this.offset.toString(), leaderEpoch: this.leaderEpoch, metadata: this.metadata };
  }
}

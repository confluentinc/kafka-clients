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

// Java's org.apache.kafka.clients.admin.RecordsToDelete: its constructor is
// private, so the static `beforeOffset(offset)` builds it, beside the
// instance `beforeOffset()` of the same name (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Names, Q17).

import { JavaForms } from '../internal/args';
import { checkLong } from '../internal/java_number';
import { sameClass } from '../internal/java_object';

const BEFORE_OFFSET_FORMS = new JavaForms('beforeOffset', [['offset']], {
  bareTypes: { offset: (value) => typeof value === 'bigint' || typeof value === 'number' },
});
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const OFFSET_FORMS = READER('beforeOffset');
const HASH_CODE_FORMS = READER('hashCode');

/**
 * Describe records to delete in a call to `Admin.deleteRecords`.
 *
 * Instances come from `RecordsToDelete.beforeOffset`.
 */
export class RecordsToDelete {
  readonly #offset: bigint;

  private constructor(offset: bigint) {
    this.#offset = offset;
  }

  /**
   * Delete all the records before the given `offset`.
   *
   * @param offset - The offset before which all records will be deleted.
   *   Use `-1` to truncate to the high watermark.
   * @returns the records to delete
   */
  static beforeOffset(offset: bigint | number): RecordsToDelete;
  /**
   * The same as `beforeOffset(offset)`, the offset given by name.
   *
   * @param options - the offset before which all records will be deleted
   * @returns the records to delete
   */
  static beforeOffset(options: {
    /** The offset before which all records will be deleted; `-1` truncates to the high watermark. */
    offset: bigint | number;
  }): RecordsToDelete;
  /**
   * Builds the records to delete.
   *
   * @param options - the offset
   * @returns the records to delete
   * @throws `TypeError` or `RangeError` when it is not a Java `long`
   */
  static beforeOffset(options: unknown): RecordsToDelete {
    BEFORE_OFFSET_FORMS.match(options, arguments.length);
    return new RecordsToDelete(checkLong(BEFORE_OFFSET_FORMS.value(options, 'offset'), 'offset'));
  }

  /**
   * The offset before which all records will be deleted. Use `-1` to
   * truncate to the high watermark.
   *
   * @returns the offset
   * @throws `TypeError` when it is given an argument: it only reads
   */
  beforeOffset(): bigint {
    if (arguments.length !== 0) OFFSET_FORMS.match(undefined, arguments.length);
    return this.#offset;
  }

  /**
   * Whether `o` is a RecordsToDelete of the same offset.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    return sameClass(this, other) && this.#offset === (other as RecordsToDelete).#offset;
  }

  /**
   * Java's `(int) offset`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return Number(BigInt.asIntN(32, this.#offset));
  }

  /**
   * The records to delete as JSON, the offset a decimal string.
   *
   * @returns the offset
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return { beforeOffset: this.#offset.toString() };
  }

  /**
   * Java's `toString()`: `(beforeOffset = <offset>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(beforeOffset = ${this.#offset})`;
  }
}

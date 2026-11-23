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

// Java's org.apache.kafka.clients.admin.FinalizedVersionRange, a value class (root CLAUDE.md,
// JavaScript Binding Conventions, Class family): Java's public constructor,
// getters, and Java's equals, hashCode and toString.

import { JavaForms } from '../internal/args';
import { checkShort } from '../internal/java_number';
import { listHashCode } from '../internal/java_object';
import { IllegalArgumentError } from '../illegal_argument_error';

const FORMS = new JavaForms('FinalizedVersionRange', [['minVersionLevel', 'maxVersionLevel']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const LOWER_FORMS = READER('minVersionLevel');
const UPPER_FORMS = READER('maxVersionLevel');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['other']]);

/**
 * Represents a range of version levels supported by every broker in a cluster for some feature.
 */
export class FinalizedVersionRange {
  readonly #minVersionLevel: number;
  readonly #maxVersionLevel: number;

  /**
   * Raises an error unless the following condition is met: `minVersionLevel >= 1` and `maxVersionLevel >= 1` and `maxVersionLevel >= minVersionLevel`.
   *
   * @param options - the minimum version level value., and the maximum version level value. (Java `short`s)
   * @throws `IllegalArgumentError` Raised when the condition described above is not met.
   * @throws `TypeError` when a level is not an integer
   * @throws `RangeError` when a level is outside Java's `short`
   */
  constructor(options: {
    /** The minimum version level value. */
    minVersionLevel: number;
    /** The maximum version level value. */
    maxVersionLevel: number;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const min = checkShort(FORMS.value(options, 'minVersionLevel'), 'minVersionLevel');
    const max = checkShort(FORMS.value(options, 'maxVersionLevel'), 'maxVersionLevel');
    if (min < 0 || max < 0 || max < min) {
      throw new IllegalArgumentError(
        'Expected minVersionLevel >= 0, maxVersionLevel >= 0 and' +
          ' maxVersionLevel >= minVersionLevel, but received' +
          ` minVersionLevel: ${min}, maxVersionLevel: ${max}`,
      );
    }
    this.#minVersionLevel = min;
    this.#maxVersionLevel = max;
  }

  /**
   * The minimum version level value.
   *
   * @returns the level
   * @throws `TypeError` when it is given an argument: it only reads
   */
  minVersionLevel(): number {
    if (arguments.length !== 0) LOWER_FORMS.match(undefined, arguments.length);
    return this.#minVersionLevel;
  }

  /**
   * The maximum version level value.
   *
   * @returns the level
   * @throws `TypeError` when it is given an argument: it only reads
   */
  maxVersionLevel(): number {
    if (arguments.length !== 0) UPPER_FORMS.match(undefined, arguments.length);
    return this.#maxVersionLevel;
  }

  /**
   * Whether `other` is a FinalizedVersionRange with the same range.
   *
   * @param other - the value to compare with, on its own or as `{ other }`
   * @returns whether they are equal
   */
  equals(other: unknown): boolean {
    EQUALS_FORMS.match(other, arguments.length);
    const o = EQUALS_FORMS.value(other, 'other');
    if (o === this) return true;
    if (!(o instanceof FinalizedVersionRange)) return false;
    return this.#minVersionLevel === o.#minVersionLevel && this.#maxVersionLevel === o.#maxVersionLevel;
  }

  /**
   * Java's `Objects.hash(minVersionLevel, maxVersionLevel)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#minVersionLevel, this.#maxVersionLevel]);
  }

  /**
   * Java's `toString()`: `FinalizedVersionRange[min_version_level:1, max_version_level:2]`.
   *
   * @returns the text
   */
  toString(): string {
    return `FinalizedVersionRange[min_version_level:${this.#minVersionLevel}, max_version_level:${this.#maxVersionLevel}]`;
  }

  /**
   * The range as `JSON.stringify` writes it: `{ minVersionLevel, maxVersionLevel }`.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { minVersionLevel: number; maxVersionLevel: number } {
    return { minVersionLevel: this.#minVersionLevel, maxVersionLevel: this.#maxVersionLevel };
  }
}

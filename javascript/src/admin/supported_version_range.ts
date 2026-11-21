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

// Java's org.apache.kafka.clients.admin.SupportedVersionRange, a value class (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family): Java's public constructor,
// getters, and Java's equals, hashCode and toString.

import { JavaForms } from '../internal/args';
import { checkShort } from '../internal/java_number';
import { listHashCode, sameClass } from '../internal/java_object';
import { IllegalArgumentError } from '../illegal_argument_error';

const FORMS = new JavaForms('SupportedVersionRange', [['minVersion', 'maxVersion']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const LOWER_FORMS = READER('minVersion');
const UPPER_FORMS = READER('maxVersion');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['other']]);

/**
 * Represents a range of versions that a particular broker supports for some feature.
 */
export class SupportedVersionRange {
  readonly #minVersion: number;
  readonly #maxVersion: number;

  /**
   * Raises an error unless the following conditions are met: 0 <= minVersion <= maxVersion.
   *
   * @param options - the minimum version value., and the maximum version value. (Java `short`s)
   * @throws `IllegalArgumentError` Raised when the condition described above is not met.
   * @throws `TypeError` when a level is not an integer
   * @throws `RangeError` when a level is outside Java's `short`
   */
  constructor(options: {
    /** The minimum version value. */
    minVersion: number;
    /** The maximum version value. */
    maxVersion: number;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const min = checkShort(FORMS.value(options, 'minVersion'), 'minVersion');
    const max = checkShort(FORMS.value(options, 'maxVersion'), 'maxVersion');
    if (min < 0 || max < 0 || max < min) {
      throw new IllegalArgumentError(`Expected 0 <= minVersion <= maxVersion but received minVersion:${min}, maxVersion:${max}.`);
    }
    this.#minVersion = min;
    this.#maxVersion = max;
  }

  /**
   * The minimum version value.
   *
   * @returns the level
   * @throws `TypeError` when it is given an argument: it only reads
   */
  minVersion(): number {
    if (arguments.length !== 0) LOWER_FORMS.match(undefined, arguments.length);
    return this.#minVersion;
  }

  /**
   * The maximum version value.
   *
   * @returns the level
   * @throws `TypeError` when it is given an argument: it only reads
   */
  maxVersion(): number {
    if (arguments.length !== 0) UPPER_FORMS.match(undefined, arguments.length);
    return this.#maxVersion;
  }

  /**
   * Whether `other` is a SupportedVersionRange with the same range.
   *
   * @param other - the value to compare with, on its own or as `{ other }`
   * @returns whether they are equal
   */
  equals(other: unknown): boolean {
    EQUALS_FORMS.match(other, arguments.length);
    const o = EQUALS_FORMS.value(other, 'other');
    if (o === this) return true;
    if (!sameClass(this, o)) return false;
    const that = o as SupportedVersionRange;
    return this.#minVersion === that.#minVersion && this.#maxVersion === that.#maxVersion;
  }

  /**
   * Java's `Objects.hash(minVersion, maxVersion)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#minVersion, this.#maxVersion]);
  }

  /**
   * Java's `toString()`: `SupportedVersionRange[min_version:1, max_version:2]`.
   *
   * @returns the text
   */
  toString(): string {
    return `SupportedVersionRange[min_version:${this.#minVersion}, max_version:${this.#maxVersion}]`;
  }

  /**
   * The range as `JSON.stringify` writes it: `{ minVersion, maxVersion }`.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { minVersion: number; maxVersion: number } {
    return { minVersion: this.#minVersion, maxVersion: this.#maxVersion };
  }
}

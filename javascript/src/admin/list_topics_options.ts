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

// Java's org.apache.kafka.clients.admin.ListTopicsOptions, an option class
// (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q19). Java's
// setter listInternal and reader shouldListInternal keep their two names
// (Q17): `listInternal()` with no argument throws TypeError.

import { JavaForms } from '../internal/args';
import { objectHashCode, sameClass } from '../internal/java_object';
import { applyOptionKeys, checkBoolean, Setter } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const LIST_INTERNAL = new Setter('listInternal', 'shouldListInternal', (value) => typeof value === 'boolean');
const SHOULD_LIST_INTERNAL_FORMS = new JavaForms('shouldListInternal', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * Options for `Admin.listTopics()`.
 */
export class ListTopicsOptions extends AbstractOptions {
  #listInternal = false;

  /**
   * The options, each key calling its setter.
   *
   * @param options - the setters' values, each key optional
   * @throws `IllegalArgumentError` for a key the class has not
   * @throws `TypeError` for a value of the wrong type
   *
   * @remarks *(deviation)* Java builds the options through their setters
   * only. Why: one call builds a configured object (Q19); the setters stay.
   */
  constructor(options?: {
    /**
     * The timeout in milliseconds, or null for the default api timeout.
     *
     * @defaultValue `null`
     */
    timeoutMs?: number | null;
    /**
     * Whether we should list internal topics.
     *
     * @defaultValue `false`
     */
    listInternal?: boolean;
  }) {
    super();
    applyOptionKeys('ListTopicsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['listInternal', (v) => this.listInternal(v as boolean)],
    ]);
  }

  /**
   * Set whether we should list internal topics.
   *
   * @param listInternal - Whether we should list internal topics, on its own
   * @returns This ListTopicsOptions object.
   */
  listInternal(listInternal: boolean): this;
  /**
   * The same as `listInternal(listInternal)`.
   *
   * @param options - whether we should list internal topics
   * @returns This ListTopicsOptions object.
   */
  listInternal(options: {
    /** Whether we should list internal topics. */
    listInternal: boolean;
  }): this;
  /**
   * Sets whether internal topics are listed.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing or not a boolean
   */
  listInternal(options: unknown): this {
    this.#listInternal = checkBoolean(LIST_INTERNAL.value(options, arguments.length), 'listInternal');
    this.internalRecord('listInternal', this.#listInternal);
    return this;
  }

  /**
   * Return true if we should list internal topics.
   *
   * @returns whether internal topics are listed
   * @throws `TypeError` when it is given an argument: it only reads
   */
  shouldListInternal(): boolean {
    if (arguments.length !== 0) SHOULD_LIST_INTERNAL_FORMS.match(undefined, arguments.length);
    return this.#listInternal;
  }

  /**
   * Java's `toString()`: `ListTopicsOptions(listInternal=<value>)`.
   *
   * @returns the text
   */
  override toString(): string {
    return `ListTopicsOptions(listInternal=${String(this.#listInternal)})`;
  }

  /**
   * Whether `o` is a ListTopicsOptions listing internal topics alike.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    return this.#listInternal === (other as ListTopicsOptions).#listInternal;
  }

  /**
   * Java's `Objects.hash(listInternal)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (Math.imul(31, 1) + objectHashCode(this.#listInternal)) | 0;
  }
}

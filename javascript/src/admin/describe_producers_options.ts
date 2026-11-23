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

// Java's org.apache.kafka.clients.admin.DescribeProducersOptions, an option
// class (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q19).
// Java's getter / setter pair `brokerId` is one method (Q17); its
// `OptionalInt` reads as `number | null` (Types). `equals`, `hashCode` and
// `toString` are Java's.

import { JavaForms } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { objectHashCode, sameClass } from '../internal/java_object';
import { GetterSetter, applyOptionKeys } from '../internal/options';
import { AbstractOptions } from './abstract_options';

const BROKER_ID = new GetterSetter('brokerId', (value) => typeof value === 'number');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * Options for `Admin.describeProducers(partitions)`.
 */
export class DescribeProducersOptions extends AbstractOptions {
  #brokerId: number | null = null;

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
     * The broker to query instead of each partition's leader.
     *
     * @defaultValue none (Java's `OptionalInt.empty()`)
     */
    brokerId?: number;
  }) {
    super();
    applyOptionKeys('DescribeProducersOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['brokerId', (v) => this.brokerId(v as number)],
    ]);
  }

  /**
   * The broker to query, if one was set.
   *
   * @returns the broker id, or null when none was set
   */
  brokerId(): number | null;
  /**
   * Sets the broker to query instead of each partition's leader.
   *
   * @param brokerId - the broker id, on its own
   * @returns This DescribeProducersOptions object.
   * @throws `TypeError` when the value is not an integer (null included:
   *   Java's `int` cannot hold null)
   * @throws `RangeError` when it is beyond Java's `int`
   */
  brokerId(brokerId: number): this;
  /**
   * The same as `brokerId(brokerId)`.
   *
   * @param options - the broker id
   * @returns This DescribeProducersOptions object.
   * @throws `TypeError` when the value is not an integer (null included:
   *   Java's `int` cannot hold null)
   * @throws `RangeError` when it is beyond Java's `int`
   */
  brokerId(options: {
    /** The broker id. */
    brokerId: number;
  }): this;
  /**
   * Reads or sets the broker id.
   *
   * @param options - the value to set, or nothing to read it
   * @returns the value, or this object
   * @throws `TypeError` when the value is not an integer (null included:
   *   Java's `int` cannot hold null)
   * @throws `RangeError` when it is beyond Java's `int`
   */
  brokerId(options?: unknown): number | null | this {
    if (!BROKER_ID.sets(options, arguments.length)) return this.#brokerId;
    this.#brokerId = checkInt(BROKER_ID.value(options), 'brokerId');
    this.internalRecord('brokerId', this.#brokerId);
    return this;
  }

  /**
   * Whether `o` is a DescribeProducersOptions with the same broker id and timeout.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as DescribeProducersOptions;
    return this.#brokerId === that.#brokerId && this.timeoutMs() === that.timeoutMs();
  }

  /**
   * Java's `Objects.hash(brokerId, timeoutMs)`, an empty broker id hashing as 0.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    let h = 1;
    for (const part of [this.#brokerId, this.timeoutMs()]) h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }

  /**
   * Java's `toString()`: `DescribeProducersOptions(brokerId=OptionalInt[1],
   * timeoutMs=null)`.
   *
   * @returns the text
   */
  override toString(): string {
    const broker = this.#brokerId === null ? 'OptionalInt.empty' : `OptionalInt[${this.#brokerId}]`;
    return `DescribeProducersOptions(brokerId=${broker}, timeoutMs=${String(this.timeoutMs())})`;
  }
}

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

// Java's org.apache.kafka.clients.admin.ListTransactionsOptions, an option
// class (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q19):
// a keyed constructor, one key per field, named after the field, not the
// setter (`filteredStates` calls `filterStates`, `filteredProducerIds`
// `filterProducerIds`, `filteredDuration` `filterOnDuration`,
// `filteredTransactionalIdPattern` `filterOnTransactionalIdPattern`), as
// Python's. The set setters store Java's copy (`new HashSet<>(...)`, which
// keeps a null element); the readers return a new `Set` each call. Each
// setter records the stored value for the addon: the states as their
// ordinals. `equals`, `hashCode` and `toString` are Java's.

import { JavaForms, kind } from '../internal/args';
import { checkLong } from '../internal/java_number';
import { objectHashCode, sameClass } from '../internal/java_object';
import { Setter, applyOptionKeys } from '../internal/options';
import { AbstractOptions } from './abstract_options';
import { TransactionState } from './transaction_state';

const isIterable = (value: unknown): boolean =>
  value === null ||
  typeof value === 'string' ||
  (typeof value === 'object' && Symbol.iterator in value && !(value instanceof AbstractOptions));
const FILTER_STATES = new Setter('filterStates', 'filteredStates', isIterable, 'states');
const FILTER_PRODUCER_IDS = new Setter('filterProducerIds', 'filteredProducerIds', isIterable, 'producerIdFilters');
const FILTER_ON_DURATION = new Setter(
  'filterOnDuration',
  'filteredDuration',
  (value) => typeof value === 'bigint' || typeof value === 'number',
  'durationMs',
);
const FILTER_ON_TRANSACTIONAL_ID_PATTERN = new Setter(
  'filterOnTransactionalIdPattern',
  'filteredTransactionalIdPattern',
  (value) => value === null || typeof value === 'string',
  'pattern',
);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const FILTERED_STATES_FORMS = READER('filteredStates');
const FILTERED_PRODUCER_IDS_FORMS = READER('filteredProducerIds');
const FILTERED_DURATION_FORMS = READER('filteredDuration');
const FILTERED_TRANSACTIONAL_ID_PATTERN_FORMS = READER('filteredTransactionalIdPattern');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * Java's `new HashSet<>(collection)`: the distinct members, a null kept.
 *
 * @param value - the collection given
 * @param name - the parameter's name
 * @param check - the members' test, and their type's name
 * @returns the members
 * @throws `TypeError` for a null collection (Java dereferences it), a string,
 *   or a member of the wrong type
 */
function copyOf<T>(value: unknown, name: string, check: [(v: unknown) => boolean, string]): Array<T | null> {
  if (value === null) throw new TypeError(`${name} must not be null`);
  if (typeof value === 'string') throw new TypeError(`${name} must be an iterable of ${check[1]}, not string`);
  if (typeof value !== 'object' || !(Symbol.iterator in value))
    throw new TypeError(`${name} must be an iterable of ${check[1]}, not ${kind(value)}`);
  const out: Array<T | null> = [];
  for (const v of value as Iterable<unknown>) {
    if (v !== null && !check[0](v)) throw new TypeError(`${name} must contain only ${check[1]}, not ${kind(v)}`);
    if (!out.includes(v as T | null)) out.push(v as T | null);
  }
  return out;
}

/**
 * Java's `AbstractCollection.toString()` of a set: `[a, b]`.
 *
 * @param members - the members
 * @returns the text
 */
function setString(members: readonly unknown[]): string {
  return `[${members.map(String).join(', ')}]`;
}

/**
 * Java's `Set.equals`: the same members.
 *
 * @param a - the first set's members
 * @param b - the second set's members
 * @returns whether they are equal
 */
function sameMembers(a: readonly unknown[], b: readonly unknown[]): boolean {
  return a.length === b.length && a.every((v) => b.includes(v));
}

/**
 * Options for `Admin.listTransactions()`.
 */
export class ListTransactionsOptions extends AbstractOptions {
  #filteredStates: Array<TransactionState | null> = [];
  #filteredProducerIds: Array<bigint | null> = [];
  #filteredDuration = -1n;
  #filteredTransactionalIdPattern: string | null = null;

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
     * The states to filter by, `filterStates`'s value.
     *
     * @defaultValue the empty set
     */
    filteredStates?: Iterable<TransactionState>;
    /**
     * The producer ids to filter by, `filterProducerIds`'s value.
     *
     * @defaultValue the empty set
     */
    filteredProducerIds?: Iterable<bigint | number>;
    /**
     * The duration in milliseconds to filter by, `filterOnDuration`'s value.
     *
     * @defaultValue `-1n`
     */
    filteredDuration?: bigint | number;
    /**
     * The transactional ID pattern to filter by, `filterOnTransactionalIdPattern`'s value.
     *
     * @defaultValue `null`
     */
    filteredTransactionalIdPattern?: string | null;
  }) {
    super();
    applyOptionKeys('ListTransactionsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['filteredStates', (v) => this.filterStates(v as Iterable<TransactionState>)],
      ['filteredProducerIds', (v) => this.filterProducerIds(v as Iterable<bigint>)],
      ['filteredDuration', (v) => this.filterOnDuration(v as bigint)],
      ['filteredTransactionalIdPattern', (v) => this.filterOnTransactionalIdPattern(v as string | null)],
    ]);
  }

  /**
   * Filter only the transactions that are in a specific set of states. If no
   * filter is specified or if the passed set of states is empty, then
   * transactions in all states will be returned.
   *
   * @param states - the set of states to filter by, on its own
   * @returns this object
   * @throws `TypeError` when the value is missing, null (Java dereferences
   *   it), or not an iterable of TransactionState
   */
  filterStates(states: Iterable<TransactionState>): this;
  /**
   * The same as `filterStates(states)`.
   *
   * @param options - the set of states to filter by
   * @returns this object
   * @throws `TypeError` when the value is missing, null (Java dereferences
   *   it), or not an iterable of TransactionState
   */
  filterStates(options: {
    /** The set of states to filter by. */
    states: Iterable<TransactionState>;
  }): this;
  /**
   * Sets the states.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing, null (Java dereferences
   *   it), or not an iterable of TransactionState
   */
  filterStates(options: unknown): this {
    const value = FILTER_STATES.value(options, arguments.length);
    this.#filteredStates = copyOf<TransactionState>(value, 'states', [(v) => v instanceof TransactionState, 'TransactionState']);
    this.internalRecord(
      'filterStates',
      this.#filteredStates.map((s) => (s === null ? null : s.ordinal())),
    );
    return this;
  }

  /**
   * Filter only the transactions from producers in a specific set of
   * producerIds. If no filter is specified or if the passed collection of
   * producerIds is empty, then the transactions of all producerIds will be
   * returned.
   *
   * @param producerIdFilters - the set of producerIds to filter by, on its own
   * @returns this object
   * @throws `TypeError` when the value is missing, null (Java dereferences
   *   it), or not an iterable of whole numbers
   * @throws `RangeError` for an id beyond Java's `long`
   */
  filterProducerIds(producerIdFilters: Iterable<bigint | number>): this;
  /**
   * The same as `filterProducerIds(producerIdFilters)`.
   *
   * @param options - the set of producerIds to filter by
   * @returns this object
   * @throws `TypeError` when the value is missing, null (Java dereferences
   *   it), or not an iterable of whole numbers
   * @throws `RangeError` for an id beyond Java's `long`
   */
  filterProducerIds(options: {
    /** The set of producerIds to filter by. */
    producerIdFilters: Iterable<bigint | number>;
  }): this;
  /**
   * Sets the producer ids.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing, null (Java dereferences
   *   it), or not an iterable of whole numbers
   * @throws `RangeError` for an id beyond Java's `long`
   */
  filterProducerIds(options: unknown): this {
    const value = FILTER_PRODUCER_IDS.value(options, arguments.length);
    const given = copyOf<unknown>(value, 'producerIdFilters', [(v) => typeof v === 'bigint' || typeof v === 'number', 'bigints']);
    const ids: Array<bigint | null> = [];
    for (const v of given) {
      const id = v === null ? null : checkLong(v, 'producerIdFilters');
      if (!ids.includes(id)) ids.push(id);
    }
    this.#filteredProducerIds = ids;
    this.internalRecord('filterProducerIds', [...this.#filteredProducerIds]);
    return this;
  }

  /**
   * Filter only the transactions that are running longer than the specified
   * duration. If no filter is specified or if the passed duration ms is less
   * than 0, then the all transactions will be returned.
   *
   * @param durationMs - the duration in milliseconds to filter by, on its own
   * @returns this object
   * @throws `TypeError` when the value is missing or not a whole number
   * @throws `RangeError` when it is beyond Java's `long`
   */
  filterOnDuration(durationMs: bigint | number): this;
  /**
   * The same as `filterOnDuration(durationMs)`.
   *
   * @param options - the duration in milliseconds to filter by
   * @returns this object
   * @throws `TypeError` when the value is missing or not a whole number
   * @throws `RangeError` when it is beyond Java's `long`
   */
  filterOnDuration(options: {
    /** The duration in milliseconds to filter by. */
    durationMs: bigint | number;
  }): this;
  /**
   * Sets the duration.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing or not a whole number
   * @throws `RangeError` when it is beyond Java's `long`
   */
  filterOnDuration(options: unknown): this {
    this.#filteredDuration = checkLong(FILTER_ON_DURATION.value(options, arguments.length), 'durationMs');
    this.internalRecord('filterOnDuration', this.#filteredDuration);
    return this;
  }

  /**
   * Filter only the transactions that match with the given transactional ID
   * pattern. If the filter is null or if the passed string is empty, then all
   * the transactions will be returned.
   *
   * @param pattern - the transactional ID regular expression pattern to filter by, on its own
   * @returns this object
   * @throws `TypeError` when the value is missing, or neither a string nor null
   */
  filterOnTransactionalIdPattern(pattern: string | null): this;
  /**
   * The same as `filterOnTransactionalIdPattern(pattern)`.
   *
   * @param options - the transactional ID regular expression pattern to filter by
   * @returns this object
   * @throws `TypeError` when the value is missing, or neither a string nor null
   */
  filterOnTransactionalIdPattern(options: {
    /** The transactional ID regular expression pattern to filter by, or null. */
    pattern: string | null;
  }): this;
  /**
   * Sets the pattern.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing, or neither a string nor null
   */
  filterOnTransactionalIdPattern(options: unknown): this {
    const value = FILTER_ON_TRANSACTIONAL_ID_PATTERN.value(options, arguments.length);
    if (value !== null && typeof value !== 'string') throw new TypeError(`pattern must be a string or null, not ${kind(value)}`);
    this.#filteredTransactionalIdPattern = value;
    this.internalRecord('filterOnTransactionalIdPattern', value);
    return this;
  }

  /**
   * Returns the set of states to be filtered or empty if no states have been
   * specified.
   *
   * @returns the current set of filtered states (empty means that no states
   *   are filtered and all transactions will be returned), a new set
   * @throws `TypeError` when it is given an argument: it only reads
   */
  filteredStates(): Set<TransactionState> {
    if (arguments.length !== 0) FILTERED_STATES_FORMS.match(undefined, arguments.length);
    return new Set(this.#filteredStates as TransactionState[]);
  }

  /**
   * Returns the set of producerIds that are being filtered or empty if none
   * have been specified.
   *
   * @returns the current set of filtered producerIds (empty means that no
   *   producerIds are filtered and all transactions will be returned), a new set
   * @throws `TypeError` when it is given an argument: it only reads
   */
  filteredProducerIds(): Set<bigint> {
    if (arguments.length !== 0) FILTERED_PRODUCER_IDS_FORMS.match(undefined, arguments.length);
    return new Set(this.#filteredProducerIds as bigint[]);
  }

  /**
   * Returns the duration ms value being filtered.
   *
   * @returns the current duration filter value in ms (negative value means
   *   transactions are not filtered by duration)
   * @throws `TypeError` when it is given an argument: it only reads
   */
  filteredDuration(): bigint {
    if (arguments.length !== 0) FILTERED_DURATION_FORMS.match(undefined, arguments.length);
    return this.#filteredDuration;
  }

  /**
   * Returns transactional ID being filtered.
   *
   * @returns the current transactional ID pattern filter (empty means no
   *   transactional IDs are filtered and all transactions will be returned)
   * @throws `TypeError` when it is given an argument: it only reads
   */
  filteredTransactionalIdPattern(): string | null {
    if (arguments.length !== 0) FILTERED_TRANSACTIONAL_ID_PATTERN_FORMS.match(undefined, arguments.length);
    return this.#filteredTransactionalIdPattern;
  }

  /**
   * Java's `toString()`: `ListTransactionsOptions(filteredStates=[Ongoing],
   * filteredProducerIds=[1], filteredDuration=-1,
   * filteredTransactionalIdPattern=null, timeoutMs=null)`.
   *
   * @returns the text
   *
   * @remarks The sets are written in the order their members were given;
   * Java's `HashSet` writes them in its own hash order.
   */
  override toString(): string {
    return (
      `ListTransactionsOptions(filteredStates=${setString(this.#filteredStates)}` +
      `, filteredProducerIds=${setString(this.#filteredProducerIds)}, filteredDuration=${this.#filteredDuration}` +
      `, filteredTransactionalIdPattern=${String(this.#filteredTransactionalIdPattern)}, timeoutMs=${String(this.timeoutMs())})`
    );
  }

  /**
   * Whether `o` is a ListTransactionsOptions with the same filters (Java's
   * `equals` leaves the timeout out).
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ListTransactionsOptions;
    return (
      sameMembers(this.#filteredStates, that.#filteredStates) &&
      sameMembers(this.#filteredProducerIds, that.#filteredProducerIds) &&
      this.#filteredDuration === that.#filteredDuration &&
      this.#filteredTransactionalIdPattern === that.#filteredTransactionalIdPattern
    );
  }

  /**
   * Java's `Objects.hash(filteredStates, filteredProducerIds,
   * filteredDuration, filteredTransactionalIdPattern)`, each set hashing as
   * Java's `Set.hashCode()`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    const setHash = (members: readonly unknown[]): number => members.reduce<number>((h, v) => (h + objectHashCode(v)) | 0, 0);
    let h = 1;
    for (const part of [setHash(this.#filteredStates), setHash(this.#filteredProducerIds)]) h = (Math.imul(31, h) + part) | 0;
    for (const part of [this.#filteredDuration, this.#filteredTransactionalIdPattern]) h = (Math.imul(31, h) + objectHashCode(part)) | 0;
    return h;
  }
}

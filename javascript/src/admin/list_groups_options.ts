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
// Java's org.apache.kafka.clients.admin.ListGroupsOptions, an option class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family, Q19): a keyed
// constructor, one key per setter field, named after the field, not the
// setter (`groupStates` calls `inGroupStates`, `types` calls `withTypes`,
// `protocolTypes` calls `withProtocolTypes`), as Python's. The setters store
// Java's copy (null or empty: the empty set), refusing a null element as
// Java's `Set.copyOf` does; the readers return a new `Set` each call. Each
// setter records the stored set for the addon: the states and types as their
// ordinals, which the addon turns into the core's singletons.

import { JavaForms, kind } from '../internal/args';
import { Setter, applyOptionKeys } from '../internal/options';
import { GroupState } from '../common/group_state';
import { GroupType } from '../common/group_type';
import { AbstractOptions } from './abstract_options';

const isIterableOrNull = (value: unknown): boolean =>
  value === null ||
  typeof value === 'string' ||
  (typeof value === 'object' && Symbol.iterator in value && !(value instanceof AbstractOptions));
const IN_GROUP_STATES = new Setter('inGroupStates', 'groupStates', isIterableOrNull, 'groupStates');
const WITH_PROTOCOL_TYPES = new Setter('withProtocolTypes', 'protocolTypes', isIterableOrNull, 'protocolTypes');
const WITH_TYPES = new Setter('withTypes', 'types', isIterableOrNull, 'types');
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const GROUP_STATES_FORMS = READER('groupStates');
const PROTOCOL_TYPES_FORMS = READER('protocolTypes');
const TYPES_FORMS = READER('types');
const FOR_CONSUMER_GROUPS_FORMS = READER('forConsumerGroups');
const FOR_SHARE_GROUPS_FORMS = READER('forShareGroups');
const FOR_STREAMS_GROUPS_FORMS = READER('forStreamsGroups');

/** Java's `ConsumerProtocol.PROTOCOL_TYPE`. */
const CONSUMER_PROTOCOL_TYPE = 'consumer';

/**
 * Java's `(set == null || set.isEmpty()) ? Set.of() : Set.copyOf(set)`.
 *
 * @param value - the set given
 * @param name - the parameter's name
 * @param check - the elements' test, and their type's name
 * @returns the stored members
 */
function copyOf<T>(value: unknown, name: string, check: [(v: unknown) => boolean, string]): T[] {
  if (value === null) return [];
  if (typeof value === 'string') throw new TypeError(`${name} must be an iterable of ${check[1]}, not string`);
  if (typeof value !== 'object' || !(Symbol.iterator in value))
    throw new TypeError(`${name} must be an iterable of ${check[1]}, not ${kind(value)}`);
  const out: T[] = [];
  for (const v of value as Iterable<unknown>) {
    if (v === null) throw new TypeError(`${name} must not contain null`);
    if (!check[0](v)) throw new TypeError(`${name} must contain only ${check[1]}, not ${kind(v)}`);
    if (!out.includes(v as T)) out.push(v as T);
  }
  return out;
}

/**
 * Options for `Admin.listGroups()`.
 */
export class ListGroupsOptions extends AbstractOptions {
  #groupStates: GroupState[] = [];
  #types: GroupType[] = [];
  #protocolTypes: string[] = [];

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
     * The group states to list, `inGroupStates`'s value.
     *
     * @defaultValue the empty set
     */
    groupStates?: Iterable<GroupState> | null;
    /**
     * The group types to list, `withTypes`'s value.
     *
     * @defaultValue the empty set
     */
    types?: Iterable<GroupType> | null;
    /**
     * The protocol types to list, `withProtocolTypes`'s value.
     *
     * @defaultValue the empty set
     */
    protocolTypes?: Iterable<string> | null;
  }) {
    super();
    applyOptionKeys('ListGroupsOptions', options, arguments.length, [
      ['timeoutMs', (v) => this.timeoutMs(v as number | null)],
      ['groupStates', (v) => this.inGroupStates(v as Iterable<GroupState> | null)],
      ['types', (v) => this.withTypes(v as Iterable<GroupType> | null)],
      ['protocolTypes', (v) => this.withProtocolTypes(v as Iterable<string> | null)],
    ]);
  }

  /**
   * Only consumer groups will be returned by listGroups(). This operation sets
   * filters on group type and protocol type which select consumer groups.
   *
   * @returns the options
   * @throws `TypeError` when it is given an argument
   */
  static forConsumerGroups(): ListGroupsOptions {
    if (arguments.length !== 0) FOR_CONSUMER_GROUPS_FORMS.match(undefined, arguments.length);
    return new ListGroupsOptions().withTypes([GroupType.CLASSIC, GroupType.CONSUMER]).withProtocolTypes(['', CONSUMER_PROTOCOL_TYPE]);
  }

  /**
   * Only share groups will be returned by listGroups(). This operation sets a
   * filter on group type which select share groups.
   *
   * @returns the options
   * @throws `TypeError` when it is given an argument
   */
  static forShareGroups(): ListGroupsOptions {
    if (arguments.length !== 0) FOR_SHARE_GROUPS_FORMS.match(undefined, arguments.length);
    return new ListGroupsOptions().withTypes([GroupType.SHARE]);
  }

  /**
   * Only streams groups will be returned by listGroups(). This operation sets
   * a filter on group type which select streams groups.
   *
   * @returns the options
   * @throws `TypeError` when it is given an argument
   */
  static forStreamsGroups(): ListGroupsOptions {
    if (arguments.length !== 0) FOR_STREAMS_GROUPS_FORMS.match(undefined, arguments.length);
    return new ListGroupsOptions().withTypes([GroupType.STREAMS]);
  }

  /**
   * If groupStates is set, only groups in these states will be returned by
   * listGroups(). Otherwise, all groups are returned. This operation is
   * supported by brokers with version 2.6.0 or later.
   *
   * @param groupStates - the states, on its own (null or empty: all)
   * @returns This ListGroupsOptions object.
   */
  inGroupStates(groupStates: Iterable<GroupState> | null): this;
  /**
   * The same as `inGroupStates(groupStates)`.
   *
   * @param options - the states (null or empty: all)
   * @returns This ListGroupsOptions object.
   */
  inGroupStates(options: {
    /** The states (null or empty: all). */
    groupStates: Iterable<GroupState> | null;
  }): this;
  /**
   * Sets the states.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing, not an iterable of GroupState, or holds null
   */
  inGroupStates(options: unknown): this {
    const value = IN_GROUP_STATES.value(options, arguments.length);
    this.#groupStates = copyOf<GroupState>(value, 'groupStates', [(v) => v instanceof GroupState, 'GroupState']);
    this.internalRecord(
      'inGroupStates',
      this.#groupStates.map((s) => s.ordinal()),
    );
    return this;
  }

  /**
   * If protocol types is set, only groups of these protocol types will be
   * returned by listGroups(). Otherwise, all groups are returned.
   *
   * @param protocolTypes - the protocol types, on its own (null or empty: all)
   * @returns This ListGroupsOptions object.
   */
  withProtocolTypes(protocolTypes: Iterable<string> | null): this;
  /**
   * The same as `withProtocolTypes(protocolTypes)`.
   *
   * @param options - the protocol types (null or empty: all)
   * @returns This ListGroupsOptions object.
   */
  withProtocolTypes(options: {
    /** The protocol types (null or empty: all). */
    protocolTypes: Iterable<string> | null;
  }): this;
  /**
   * Sets the protocol types.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing, a string, not an iterable of strings, or holds null
   */
  withProtocolTypes(options: unknown): this {
    const value = WITH_PROTOCOL_TYPES.value(options, arguments.length);
    this.#protocolTypes = copyOf<string>(value, 'protocolTypes', [(v) => typeof v === 'string', 'strings']);
    this.internalRecord('withProtocolTypes', [...this.#protocolTypes]);
    return this;
  }

  /**
   * If types is set, only groups of these types will be returned by
   * listGroups(). Otherwise, all groups are returned.
   *
   * @param types - the group types, on its own (null or empty: all)
   * @returns This ListGroupsOptions object.
   */
  withTypes(types: Iterable<GroupType> | null): this;
  /**
   * The same as `withTypes(types)`.
   *
   * @param options - the group types (null or empty: all)
   * @returns This ListGroupsOptions object.
   */
  withTypes(options: {
    /** The group types (null or empty: all). */
    types: Iterable<GroupType> | null;
  }): this;
  /**
   * Sets the types.
   *
   * @param options - the value
   * @returns this object
   * @throws `TypeError` when the value is missing, not an iterable of GroupType, or holds null
   */
  withTypes(options: unknown): this {
    const value = WITH_TYPES.value(options, arguments.length);
    this.#types = copyOf<GroupType>(value, 'types', [(v) => v instanceof GroupType, 'GroupType']);
    this.internalRecord(
      'withTypes',
      this.#types.map((t) => t.ordinal()),
    );
    return this;
  }

  /**
   * Returns the list of group states that are requested or empty if no
   * states have been specified.
   *
   * @returns a new set of the states
   * @throws `TypeError` when it is given an argument: it only reads
   */
  groupStates(): Set<GroupState> {
    if (arguments.length !== 0) GROUP_STATES_FORMS.match(undefined, arguments.length);
    return new Set(this.#groupStates);
  }

  /**
   * Returns the list of protocol types that are requested or empty if no
   * protocol types have been specified.
   *
   * @returns a new set of the protocol types
   * @throws `TypeError` when it is given an argument: it only reads
   */
  protocolTypes(): Set<string> {
    if (arguments.length !== 0) PROTOCOL_TYPES_FORMS.match(undefined, arguments.length);
    return new Set(this.#protocolTypes);
  }

  /**
   * Returns the list of group types that are requested or empty if no types
   * have been specified.
   *
   * @returns a new set of the types
   * @throws `TypeError` when it is given an argument: it only reads
   */
  types(): Set<GroupType> {
    if (arguments.length !== 0) TYPES_FORMS.match(undefined, arguments.length);
    return new Set(this.#types);
  }
}

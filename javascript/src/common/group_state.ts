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

// Java's org.apache.kafka.common.GroupState, an enum (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family): frozen constants with java.lang.Enum's
// `name()`, `ordinal()`, `values()` and `valueOf()`. Java overrides
// `toString()` to return each constant's display name (`Stable`),
// while `toJSON()` returns the constant's name, as for every enum here.

import { JavaForms } from '../internal/args';
import { IllegalArgumentError } from '../illegal_argument_error';
import { GroupType } from './group_type';

const JAVA_CLASS = 'org.apache.kafka.common.GroupState';

const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VALUES_FORMS = READER('values');
const NAME_FORMS = READER('name');
const ORDINAL_FORMS = READER('ordinal');
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const PARSE_FORMS = new JavaForms('parse', [['name']], { bareTypes: { name: isName } });
const GROUP_STATES_FOR_TYPE_FORMS = new JavaForms('groupStatesForType', [['type']], {
  bareTypes: { type: (value) => value === null || value instanceof GroupType },
});

/**
 * The group state.
 *
 * The following table shows the correspondence between the group states and types.
 *
 * | State | Classic group | Consumer group | Share group | Streams group |
 * |---|---|---|---|---|
 * | UNKNOWN | Yes | Yes | Yes | Yes |
 * | PREPARING_REBALANCE | Yes | Yes | | |
 * | COMPLETING_REBALANCE | Yes | Yes | | |
 * | STABLE | Yes | Yes | Yes | Yes |
 * | DEAD | Yes | Yes | Yes | Yes |
 * | EMPTY | Yes | Yes | Yes | Yes |
 * | ASSIGNING | | Yes | | Yes |
 * | RECONCILING | | Yes | | Yes |
 * | NOT_READY | | | | Yes |
 */
export class GroupState {
  readonly #ordinal: number;
  readonly #name: string;
  readonly #displayName: string;

  /**
   * Made by this class only (the enum's constants).
   *
   * @param ordinal - the constant's position in Java's declaration
   * @param name - the constant's name
   * @param displayName - Java's `name` field, which `toString()` returns
   */
  private constructor(ordinal: number, name: string, displayName: string) {
    this.#ordinal = ordinal;
    this.#name = name;
    this.#displayName = displayName;
    Object.freeze(this);
  }

  /** The `Unknown` value. */
  static readonly UNKNOWN: GroupState = new GroupState(0, 'UNKNOWN', 'Unknown');

  /** The `PreparingRebalance` value. */
  static readonly PREPARING_REBALANCE: GroupState = new GroupState(1, 'PREPARING_REBALANCE', 'PreparingRebalance');

  /** The `CompletingRebalance` value. */
  static readonly COMPLETING_REBALANCE: GroupState = new GroupState(2, 'COMPLETING_REBALANCE', 'CompletingRebalance');

  /** The `Stable` value. */
  static readonly STABLE: GroupState = new GroupState(3, 'STABLE', 'Stable');

  /** The `Dead` value. */
  static readonly DEAD: GroupState = new GroupState(4, 'DEAD', 'Dead');

  /** The `Empty` value. */
  static readonly EMPTY: GroupState = new GroupState(5, 'EMPTY', 'Empty');

  /** The `Assigning` value. */
  static readonly ASSIGNING: GroupState = new GroupState(6, 'ASSIGNING', 'Assigning');

  /** The `Reconciling` value. */
  static readonly RECONCILING: GroupState = new GroupState(7, 'RECONCILING', 'Reconciling');

  /** The `NotReady` value. */
  static readonly NOT_READY: GroupState = new GroupState(8, 'NOT_READY', 'NotReady');

  /**
   * The constants in declaration order, Java's `values()`.
   *
   * @returns a new array of the constants
   * @throws `TypeError` when it is given an argument: it only reads
   */
  static values(): GroupState[] {
    VALUES_FORMS.match(undefined, arguments.length);
    return [
      GroupState.UNKNOWN,
      GroupState.PREPARING_REBALANCE,
      GroupState.COMPLETING_REBALANCE,
      GroupState.STABLE,
      GroupState.DEAD,
      GroupState.EMPTY,
      GroupState.ASSIGNING,
      GroupState.RECONCILING,
      GroupState.NOT_READY,
    ];
  }

  /**
   * The constant named exactly `name`, Java's `valueOf(String)`.
   *
   * @param name - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(name: string): GroupState;
  /**
   * The same as `valueOf(name)`, the name given by name.
   *
   * @param options - the name of the constant to return
   * @returns the constant with the specified name
   * @throws `IllegalArgumentError` when no constant has that name
   * @throws `TypeError` when the name is null or not a string
   */
  static valueOf(options: {
    /** The name of the constant to return. */
    name: string;
  }): GroupState;
  static valueOf(name: unknown): GroupState {
    VALUE_OF_FORMS.match(name, arguments.length);
    const value = VALUE_OF_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('Name is null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const found = GroupState.values().find((c) => c.#name === value);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${value}`);
    return found;
  }

  /**
   * Case-insensitive group state lookup by string name.
   *
   * @param name - the name, on its own or as `{ name }`
   * @returns the constant, or `UNKNOWN` when no constant has that name
   * @throws `TypeError` when the name is null (Java dereferences it) or not a string
   */
  static parse(name: string): GroupState;
  /**
   * The same as `parse(name)`, the name given by name.
   *
   * @param options - the name
   * @returns the constant, or `UNKNOWN` when no constant has that name
   */
  static parse(options: {
    /** The name to parse. */
    name: string;
  }): GroupState;
  static parse(name: unknown): GroupState {
    PARSE_FORMS.match(name, arguments.length);
    const value = PARSE_FORMS.value(name, 'name');
    if (value === null) throw new TypeError('name must not be null');
    if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
    const key = value.toUpperCase();
    return GroupState.values().find((c) => c.#displayName.toUpperCase() === key) ?? GroupState.UNKNOWN;
  }

  /**
   * The states a group of the given type can be in.
   *
   * @param type - the group type, on its own or as `{ type }`
   * @returns a new set of the states
   * @throws `IllegalArgumentError` "Group type not known" for `GroupType.UNKNOWN`
   * @throws `TypeError` when the type is not a GroupType
   */
  static groupStatesForType(type: GroupType): Set<GroupState>;
  /**
   * The same as `groupStatesForType(type)`, the type given by name.
   *
   * @param options - the group type
   * @returns a new set of the states
   */
  static groupStatesForType(options: {
    /** The group type. */
    type: GroupType;
  }): Set<GroupState>;
  static groupStatesForType(type: unknown): Set<GroupState> {
    GROUP_STATES_FOR_TYPE_FORMS.match(type, arguments.length);
    const value = GROUP_STATES_FOR_TYPE_FORMS.value(type, 'type');
    const G = GroupState;
    if (value === GroupType.CLASSIC) return new Set([G.PREPARING_REBALANCE, G.COMPLETING_REBALANCE, G.STABLE, G.DEAD, G.EMPTY]);
    if (value === GroupType.CONSUMER) {
      return new Set([G.PREPARING_REBALANCE, G.COMPLETING_REBALANCE, G.STABLE, G.DEAD, G.EMPTY, G.ASSIGNING, G.RECONCILING]);
    }
    if (value === GroupType.STREAMS) return new Set([G.STABLE, G.DEAD, G.EMPTY, G.ASSIGNING, G.RECONCILING, G.NOT_READY]);
    if (value === GroupType.SHARE) return new Set([G.STABLE, G.DEAD, G.EMPTY]);
    if (value !== null && !(value instanceof GroupType)) throw new TypeError('type must be a GroupType');
    throw new IllegalArgumentError('Group type not known');
  }

  /**
   * The name of this constant, exactly as declared, Java's `Enum.name()`.
   *
   * @returns the name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string {
    NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * The position of this constant in its enum declaration, Java's `Enum.ordinal()`.
   *
   * @returns the ordinal
   * @throws `TypeError` when it is given an argument: it only reads
   */
  ordinal(): number {
    ORDINAL_FORMS.match(undefined, arguments.length);
    return this.#ordinal;
  }

  /**
   * Java's `toString()` override: the display name (`Stable`).
   *
   * @returns the display name
   */
  toString(): string {
    return this.#displayName;
  }

  /**
   * The constant's name, which `JSON.stringify` writes for it.
   *
   * @param key - the key `JSON.stringify` passes, which is ignored
   * @returns the name
   *
   * @remarks *(js-only)* JavaScript's JSON protocol.
   */
  toJSON(key?: string): string {
    void key;
    return this.#name;
  }

  // Java's constants are static finals: the class is frozen once its static
  // fields are set, so none can be reassigned.
  static {
    Object.freeze(this);
  }
}

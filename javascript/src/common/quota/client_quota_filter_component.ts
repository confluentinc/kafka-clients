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

// Java's org.apache.kafka.common.quota.ClientQuotaFilterComponent, a value
// class (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family):
// Java's constructor is private, so instances come from its three static
// factories; its getters and Java's equals, hashCode and toString. Java's
// `match` is an `Optional<String>` that may itself be null: present
// (`ofEntity`), empty (`ofDefaultEntity`) or null (`ofEntityType`). The Types
// rule reads both empty and null as `null`, so `match()` returns null for
// both, as Python's does; the object keeps the three apart for equals,
// hashCode and toString, and the admin client sends the right one.

import { JavaForms, kind } from '../../internal/args';
import { sameClass, stringHashCode } from '../../internal/java_object';

const OF_ENTITY_FORMS = new JavaForms('ofEntity', [['entityType', 'entityName']]);
const isString = (value: unknown): boolean => value === null || typeof value === 'string';
const OF_DEFAULT_ENTITY_FORMS = new JavaForms('ofDefaultEntity', [['entityType']], { bareTypes: { entityType: isString } });
const OF_ENTITY_TYPE_FORMS = new JavaForms('ofEntityType', [['entityType']], { bareTypes: { entityType: isString } });
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const ENTITY_TYPE_FORMS = READER('entityType');
const MATCH_FORMS = READER('match');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/** How a component matches: an exact name, the default name, or any name. */
type Match = { kind: 'exact'; name: string } | { kind: 'default' } | { kind: 'any' };

/**
 * The entity type given, which must be a string.
 *
 * @param value - the value given
 * @returns the entity type
 */
function entityTypeOf(value: unknown): string {
  if (value === null) throw new TypeError('entityType must not be null');
  if (typeof value !== 'string') throw new TypeError(`entityType must be a string, not ${kind(value)}`);
  return value;
}

/**
 * Describes a component for applying a client quota filter.
 */
export class ClientQuotaFilterComponent {
  readonly #entityType: string;
  readonly #match: Match;

  /**
   * Java's private constructor. Instances come from
   * `ClientQuotaFilterComponent.ofEntity`, `ofDefaultEntity` and
   * `ofEntityType`.
   *
   * @param entityType - the entity type the filter component applies to
   * @param match - an exact name, the default name, or any specified name
   */
  private constructor(entityType: string, match: Match) {
    this.#entityType = entityType;
    this.#match = match;
  }

  /**
   * Constructs and returns a filter component that exactly matches the
   * provided entity name for the entity type.
   *
   * @param options - the entity type the filter component applies to, and
   *   the entity name that's matched exactly
   * @returns the component
   * @throws `TypeError` when the name or the type is null or not a string
   */
  static ofEntity(options: {
    /** The entity type the filter component applies to. */
    entityType: string;
    /** The entity name that's matched exactly. */
    entityName: string;
  }): ClientQuotaFilterComponent;
  static ofEntity(options: unknown): ClientQuotaFilterComponent {
    OF_ENTITY_FORMS.match(options, arguments.length);
    // Java checks the name (Optional.of(requireNonNull(entityName))) before
    // the constructor checks the type.
    const name = OF_ENTITY_FORMS.value(options, 'entityName');
    if (name === null) throw new TypeError('entityName must not be null');
    if (typeof name !== 'string') throw new TypeError(`entityName must be a string, not ${kind(name)}`);
    return new ClientQuotaFilterComponent(entityTypeOf(OF_ENTITY_FORMS.value(options, 'entityType')), { kind: 'exact', name });
  }

  /**
   * Constructs and returns a filter component that matches the built-in
   * default entity name for the entity type.
   *
   * @param entityType - the entity type the filter component applies to, on
   *   its own or as `{ entityType }`
   * @returns the component
   * @throws `TypeError` when the type is null or not a string
   */
  static ofDefaultEntity(entityType: string): ClientQuotaFilterComponent;
  /**
   * The same as `ofDefaultEntity(entityType)`, the parameter given by name.
   *
   * @param options - the entity type the filter component applies to
   * @returns the component
   */
  static ofDefaultEntity(options: {
    /** The entity type the filter component applies to. */
    entityType: string;
  }): ClientQuotaFilterComponent;
  static ofDefaultEntity(entityType: unknown): ClientQuotaFilterComponent {
    OF_DEFAULT_ENTITY_FORMS.match(entityType, arguments.length);
    return new ClientQuotaFilterComponent(entityTypeOf(OF_DEFAULT_ENTITY_FORMS.value(entityType, 'entityType')), { kind: 'default' });
  }

  /**
   * Constructs and returns a filter component that matches any specified
   * name for the entity type.
   *
   * @param entityType - the entity type the filter component applies to, on
   *   its own or as `{ entityType }`
   * @returns the component
   * @throws `TypeError` when the type is null or not a string
   */
  static ofEntityType(entityType: string): ClientQuotaFilterComponent;
  /**
   * The same as `ofEntityType(entityType)`, the parameter given by name.
   *
   * @param options - the entity type the filter component applies to
   * @returns the component
   */
  static ofEntityType(options: {
    /** The entity type the filter component applies to. */
    entityType: string;
  }): ClientQuotaFilterComponent;
  static ofEntityType(entityType: unknown): ClientQuotaFilterComponent {
    OF_ENTITY_TYPE_FORMS.match(entityType, arguments.length);
    return new ClientQuotaFilterComponent(entityTypeOf(OF_ENTITY_TYPE_FORMS.value(entityType, 'entityType')), { kind: 'any' });
  }

  /**
   * @returns the component's entity type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entityType(): string {
    if (arguments.length !== 0) ENTITY_TYPE_FORMS.match(undefined, arguments.length);
    return this.#entityType;
  }

  /**
   * @returns the optional match string: if present, the name that's matched
   *   exactly; `null` when it matches the default name or any specified name
   * @throws `TypeError` when it is given an argument: it only reads
   *
   * @remarks Java's empty and null `Optional` both read as `null`, by the
   * Types rule.
   */
  match(): string | null {
    if (arguments.length !== 0) MATCH_FORMS.match(undefined, arguments.length);
    return this.#match.kind === 'exact' ? this.#match.name : null;
  }

  /**
   * How the component matches, for the admin client: `[0, name]` for an
   * exact name, `[1, null]` for the default name, `[2, null]` for any.
   * Called by the admin client.
   *
   * @returns the match
   * @internal
   */
  internalMatch(): [number, string | null] {
    return this.#match.kind === 'exact' ? [0, this.#match.name] : [this.#match.kind === 'default' ? 1 : 2, null];
  }

  /**
   * Whether `o` is a ClientQuotaFilterComponent with the same entity type and
   * match.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ClientQuotaFilterComponent;
    if (this.#entityType !== that.#entityType || this.#match.kind !== that.#match.kind) return false;
    return this.#match.kind !== 'exact' || this.#match.name === (that.#match as { name: string }).name;
  }

  /**
   * Java's `Objects.hash(entityType, match)`: an empty `Optional` hashes as
   * 0, as a null one does.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    const matchHash = this.#match.kind === 'exact' ? stringHashCode(this.#match.name) : 0;
    return (Math.imul(31, (31 + stringHashCode(this.#entityType)) | 0) + matchHash) | 0;
  }

  /**
   * Java's `toString()`: `ClientQuotaFilterComponent(entityType=user,
   * match=Optional[alice])`, `match=Optional.empty` for the default name and
   * `match=null` for any name.
   *
   * @returns the text
   */
  toString(): string {
    const match =
      this.#match.kind === 'exact' ? `Optional[${this.#match.name}]` : this.#match.kind === 'default' ? 'Optional.empty' : 'null';
    return `ClientQuotaFilterComponent(entityType=${this.#entityType}, match=${match})`;
  }

  /**
   * The component as `JSON.stringify` writes it: `{ entityType, match,
   * matchKind }`, `match` the exact name or null, and `matchKind` one of
   * `'exact'`, `'default'` (the default name) and `'any'` (any name).
   *
   * @returns the component's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { entityType: string; match: string | null; matchKind: 'exact' | 'default' | 'any' } {
    return { entityType: this.#entityType, match: this.#match.kind === 'exact' ? this.#match.name : null, matchKind: this.#match.kind };
  }
}

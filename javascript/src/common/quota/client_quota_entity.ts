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

// Java's org.apache.kafka.common.quota.ClientQuotaEntity, a value class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's public
// constructor, the entity-type constants, `isValidEntityType`, `entries()`,
// and Java's equals, hashCode and toString. Java stores the map it is given;
// this class stores a copy (null stays null), so an entity used as a key of a
// ClientQuotaEntityMap cannot change, and `entries()` hands out a new `Map`.
// The map is taken as a `Map` or a plain object (Types).

import { finalStatics } from '../../internal/static_finals';
import { JavaForms, kind } from '../../internal/args';
import { mapEquals, mapHashCode, sameClass } from '../../internal/java_object';
import { javaString } from '../../internal/java_string';

const isMapLike = (value: unknown): boolean =>
  value === null || value instanceof Map || (typeof value === 'object' && Object.getPrototypeOf(value) === Object.prototype);
const FORMS = new JavaForms('ClientQuotaEntity', [['entries']], {
  bareTypes: { entries: (value) => value === null || value instanceof Map },
});
const IS_VALID_ENTITY_TYPE_FORMS = new JavaForms('isValidEntityType', [['entityType']], {
  bareTypes: { entityType: (value) => value === null || typeof value === 'string' },
});
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const ENTRIES_FORMS = READER('entries');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Describes a client quota entity, which is a mapping of entity types to
 * their names.
 */
export class ClientQuotaEntity {
  /** The type of an entity entry: a user. */
  static readonly USER: string = 'user';
  /** The type of an entity entry: a client id. */
  static readonly CLIENT_ID: string = 'client-id';
  /** The type of an entity entry: an IP address. */
  static readonly IP: string = 'ip';

  // Java's static finals: they can't be reassigned.
  static {
    finalStatics(this, 'USER', 'CLIENT_ID', 'IP');
  }

  readonly #entries: Map<string, string | null> | null;

  /**
   * Constructs a quota entity for the given types and names. If a name is
   * null, then it is mapped to the built-in default entity name.
   *
   * @param entries - maps entity type to its name, a `Map` on its own
   * @throws `TypeError` when it is not a map of strings to strings or null
   */
  constructor(entries: ReadonlyMap<string, string | null>);
  /**
   * The same as `new ClientQuotaEntity(entries)`, the parameter given by
   * name.
   *
   * @param options - maps entity type to its name, a `Map` or a plain object
   */
  constructor(options: {
    /** Maps entity type to its name; a null name is the default entity. */
    entries: ReadonlyMap<string, string | null> | Readonly<Record<string, string | null>>;
  });
  constructor(entries: unknown) {
    FORMS.match(entries, arguments.length);
    const value = FORMS.value(entries, 'entries');
    if (!isMapLike(value)) throw new TypeError(`entries must be a Map or a plain object of strings, not ${kind(value)}`);
    if (value === null) {
      this.#entries = null;
      return;
    }
    const pairs = value instanceof Map ? [...(value as Map<unknown, unknown>)] : Object.entries(value as Record<string, unknown>);
    const out = new Map<string, string | null>();
    for (const [k, v] of pairs) {
      if (typeof k !== 'string') throw new TypeError(`entries must have string keys, not ${kind(k)}`);
      if (v !== null && typeof v !== 'string') throw new TypeError(`entries must have string or null values, not ${kind(v)}`);
      out.set(k, v);
    }
    this.#entries = out;
  }

  /**
   * Whether `entityType` is one of the entity types.
   *
   * @param entityType - the entity type, on its own or as `{ entityType }`
   * @returns whether it is `USER`, `CLIENT_ID` or `IP`
   */
  static isValidEntityType(entityType: string | null): boolean;
  /**
   * The same as `isValidEntityType(entityType)`, the parameter given by name.
   *
   * @param options - the entity type
   * @returns whether it is `USER`, `CLIENT_ID` or `IP`
   */
  static isValidEntityType(options: {
    /** The entity type. */
    entityType: string | null;
  }): boolean;
  static isValidEntityType(entityType: unknown): boolean {
    IS_VALID_ENTITY_TYPE_FORMS.match(entityType, arguments.length);
    const t = IS_VALID_ENTITY_TYPE_FORMS.value(entityType, 'entityType');
    return t === ClientQuotaEntity.USER || t === ClientQuotaEntity.CLIENT_ID || t === ClientQuotaEntity.IP;
  }

  /**
   * @returns map of entity type to its name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entries(): Map<string, string | null> {
    if (arguments.length !== 0) ENTRIES_FORMS.match(undefined, arguments.length);
    return (this.#entries === null ? null : new Map(this.#entries)) as Map<string, string | null>;
  }

  /**
   * The string key a ClientQuotaEntityMap stores this entity under: the JSON
   * text of the entries sorted by entity type, or `"null"` for a null map.
   * Called by ClientQuotaEntityMap.
   *
   * @returns the key
   * @internal
   */
  internalKey(): string {
    if (this.#entries === null) return '["null"]';
    return JSON.stringify([...this.#entries].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  }

  /**
   * Whether `o` is a ClientQuotaEntity with equal entries.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    return mapEquals(this.#entries, (other as ClientQuotaEntity).#entries);
  }

  /**
   * Java's `Objects.hash(entries)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (31 + mapHashCode(this.#entries)) | 0;
  }

  /**
   * Java's `toString()`: `ClientQuotaEntity(entries={user=alice})`, the
   * entries in the order they were given.
   *
   * @returns the text
   */
  toString(): string {
    return `ClientQuotaEntity(entries=${javaString(this.#entries)})`;
  }

  /**
   * The entity as `JSON.stringify` writes it: `{ entries }`, an object of
   * entity type to name.
   *
   * @returns the entity's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { entries: Record<string, string | null> | null } {
    return { entries: this.#entries === null ? null : Object.fromEntries(this.#entries) };
  }
}

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

// Java's org.apache.kafka.common.quota.ClientQuotaAlteration, with its nested
// Op, value classes (root CLAUDE.md, JavaScript Binding Conventions, Class
// family; a nested class is a class of the namespace merged with it, Q11): Java's public constructors,
// getters, and Java's equals, hashCode (Op's) and toString. Java stores what
// it is given, a null included; the ops are held as a copy and handed out as
// a new array. ClientQuotaAlteration has no equals or hashCode in Java, so
// it compares by identity. A Java `Double` is a `number` or null.

import { JavaForms, kind } from '../../internal/args';
import { javaDoubleString, javaString } from '../../internal/java_string';
import { sameClass, stringHashCode } from '../../internal/java_object';
import { ClientQuotaEntity } from './client_quota_entity';

const FORMS = new JavaForms('ClientQuotaAlteration', [['entity', 'ops']]);
const OP_FORMS = new JavaForms('Op', [['key', 'value']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const ENTITY_FORMS = READER('entity');
const OPS_FORMS = READER('ops');
const KEY_FORMS = READER('key');
const VALUE_FORMS = READER('value');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Java's `Double.hashCode(value)`: `(int) (bits ^ (bits >>> 32))` over
 * `doubleToLongBits`, which writes every NaN as one.
 *
 * @param value - the double
 * @returns its hash code
 */
function doubleHashCode(value: number): number {
  const view = new DataView(new ArrayBuffer(8));
  view.setFloat64(0, Number.isNaN(value) ? NaN : value);
  const bits = Number.isNaN(value) ? 0x7ff8000000000000n : view.getBigUint64(0);
  return Number(BigInt.asIntN(32, bits ^ (bits >> 32n)));
}

/**
 * Java's `Double.equals`: the same `doubleToLongBits`, so NaN equals NaN and
 * 0.0 does not equal -0.0.
 *
 * @param a - the first double, or null
 * @param b - the second double, or null
 * @returns whether they are equal
 */
function doubleEquals(a: number | null, b: number | null): boolean {
  if (a === null || b === null) return a === b;
  return Object.is(a, b) || (Number.isNaN(a) && Number.isNaN(b));
}

/**
 * Describes a configuration alteration to be made to a client quota entity.
 */
export class ClientQuotaAlteration {
  readonly #entity: ClientQuotaEntity | null;
  readonly #ops: Array<ClientQuotaAlteration.Op | null> | null;

  /**
   * An alteration of an entity's quotas.
   *
   * @param options - the entity whose config will be modified, and the
   *   alteration to perform
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** The entity whose config will be modified. */
    entity: ClientQuotaEntity;
    /** The alteration to perform. */
    ops: Iterable<ClientQuotaAlteration.Op>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const entity = FORMS.value(options, 'entity');
    const ops = FORMS.value(options, 'ops');
    if (entity !== null && !(entity instanceof ClientQuotaEntity)) {
      throw new TypeError(`entity must be a ClientQuotaEntity or null, not ${kind(entity)}`);
    }
    this.#entity = entity;
    if (ops === null) {
      this.#ops = null;
      return;
    }
    if (typeof ops === 'string' || typeof ops !== 'object' || !(Symbol.iterator in ops)) {
      throw new TypeError(`ops must be an iterable of ClientQuotaAlteration.Op, not ${kind(ops)}`);
    }
    const out: Array<ClientQuotaAlteration.Op | null> = [];
    for (const op of ops as Iterable<unknown>) {
      if (op !== null && !(op instanceof ClientQuotaAlteration.Op)) {
        throw new TypeError(`ops must contain only ClientQuotaAlteration.Op, not ${kind(op)}`);
      }
      out.push(op);
    }
    this.#ops = out;
  }

  /**
   * @returns the entity whose config will be modified
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entity(): ClientQuotaEntity {
    if (arguments.length !== 0) ENTITY_FORMS.match(undefined, arguments.length);
    return this.#entity as ClientQuotaEntity;
  }

  /**
   * @returns the alteration to perform
   * @throws `TypeError` when it is given an argument: it only reads
   */
  ops(): ClientQuotaAlteration.Op[] {
    if (arguments.length !== 0) OPS_FORMS.match(undefined, arguments.length);
    return (this.#ops === null ? null : [...this.#ops]) as ClientQuotaAlteration.Op[];
  }

  /**
   * Java's `toString()`: `ClientQuotaAlteration(entity=<entity>,
   * ops=[<op>, ...])`.
   *
   * @returns the text
   */
  toString(): string {
    return `ClientQuotaAlteration(entity=${javaString(this.#entity)}, ops=${javaString(this.#ops)})`;
  }

  /**
   * The alteration as `JSON.stringify` writes it: `{ entity, ops }`.
   *
   * @returns the alteration's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { entity: ClientQuotaEntity | null; ops: Array<ClientQuotaAlteration.Op | null> | null } {
    return { entity: this.#entity, ops: this.#ops === null ? null : [...this.#ops] };
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested class ClientQuotaAlteration.Op, reached through the class name as in Java
export namespace ClientQuotaAlteration {
  /**
   * One alteration of one quota.
   */
  export class Op {
    readonly #key: string | null;
    readonly #value: number | null;

    /**
     * @param options - the quota type to alter, and its value: if set then
     *   the existing value is updated, otherwise if null, the existing value
     *   is cleared
     * @throws `TypeError` for a value of the wrong type
     */
    constructor(options: {
      /** The quota type to alter. */
      key: string;
      /** If set then the existing value is updated, otherwise if null, the existing value is cleared. */
      value: number | null;
    });
    constructor(options: unknown) {
      OP_FORMS.match(options, arguments.length);
      const key = OP_FORMS.value(options, 'key');
      const value = OP_FORMS.value(options, 'value');
      if (key !== null && typeof key !== 'string') throw new TypeError(`key must be a string or null, not ${kind(key)}`);
      if (value !== null && typeof value !== 'number') throw new TypeError(`value must be a number or null, not ${kind(value)}`);
      this.#key = key;
      this.#value = value;
    }

    /**
     * @returns the quota type to alter
     * @throws `TypeError` when it is given an argument: it only reads
     */
    key(): string {
      if (arguments.length !== 0) KEY_FORMS.match(undefined, arguments.length);
      return this.#key as string;
    }

    /**
     * If set then the existing value is updated, otherwise if null, the
     * existing value is cleared.
     *
     * @returns the value, or null
     * @throws `TypeError` when it is given an argument: it only reads
     */
    value(): number | null {
      if (arguments.length !== 0) VALUE_FORMS.match(undefined, arguments.length);
      return this.#value;
    }

    /**
     * Whether `o` is an Op with the same key and value (Java's
     * `Double.equals`: NaN equals NaN, 0.0 not -0.0).
     *
     * @param o - the value to compare with, on its own or as `{ o }`
     * @returns whether they are equal
     */
    equals(o: unknown): boolean {
      EQUALS_FORMS.match(o, arguments.length);
      const other = EQUALS_FORMS.value(o, 'o');
      if (other === this) return true;
      if (!sameClass(this, other)) return false;
      const that = other as Op;
      return this.#key === that.#key && doubleEquals(this.#value, that.#value);
    }

    /**
     * Java's `Objects.hash(key, value)`.
     *
     * @returns the hash code
     * @throws `TypeError` when it is given an argument: it only reads
     */
    hashCode(): number {
      if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
      const keyHash = this.#key === null ? 0 : stringHashCode(this.#key);
      const valueHash = this.#value === null ? 0 : doubleHashCode(this.#value);
      return (Math.imul(31, (31 + keyHash) | 0) + valueHash) | 0;
    }

    /**
     * Java's `toString()`: `ClientQuotaAlteration.Op(key=<key>,
     * value=<value>)`, the value as Java's `Double.toString` writes it.
     *
     * @returns the text
     */
    toString(): string {
      const value = this.#value === null ? 'null' : javaDoubleString(this.#value);
      return `ClientQuotaAlteration.Op(key=${javaString(this.#key)}, value=${value})`;
    }

    /**
     * The op as `JSON.stringify` writes it: `{ key, value }`.
     *
     * @returns the op's fields
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): { key: string | null; value: number | null } {
      return { key: this.#key, value: this.#value };
    }
  }
}

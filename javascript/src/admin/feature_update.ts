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

// Java's org.apache.kafka.clients.admin.FeatureUpdate, a value class that
// keeps Java's methods, with its nested UpgradeType enum, a class of the
// namespace merged with it (javascript/CLAUDE.md, JavaScript Binding Conventions,
// Names). equals, hashCode and toString are Java's. Java's constructor
// stores a null upgrade type when the level is not zero, and dereferences it
// only later: the same here.

import { JavaForms, kind } from '../internal/args';
import { checkShort } from '../internal/java_number';
import { listHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { IllegalArgumentError } from '../illegal_argument_error';

const FORMS = new JavaForms('FeatureUpdate', [['maxVersionLevel', 'upgradeType']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const VERSION_LEVEL_FORMS = READER('maxVersionLevel');
const UPGRADE_TYPE_FORMS = READER('upgradeType');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['other']]);
const TYPE_CODE_FORMS = READER('code');
const TYPE_FROM_CODE_FORMS = new JavaForms('fromCode', [['code']], { bareTypes: { code: (value) => typeof value === 'number' } });
const TYPE_VALUES_FORMS = READER('values');
const TYPE_VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], {
  bareTypes: { name: (value) => value === null || typeof value === 'string' },
});
const TYPE_NAME_FORMS = READER('name');
const TYPE_ORDINAL_FORMS = READER('ordinal');

/**
 * Encapsulates details about an update to a finalized feature.
 */
export class FeatureUpdate {
  readonly #maxVersionLevel: number;
  readonly #upgradeType: FeatureUpdate.UpgradeType | null;

  /**
   * An update of a finalized feature.
   *
   * @param options - `maxVersionLevel`: the new maximum version level for the
   *   finalized feature, a Java `short`; a value of zero is special and
   *   indicates that the update is intended to delete the finalized feature,
   *   and should be accompanied by setting the upgradeType to safe or unsafe.
   *   `upgradeType`: indicate what kind of upgrade should be performed in
   *   this operation (UPGRADE: upgrading the feature level; SAFE_DOWNGRADE:
   *   only downgrades which do not result in metadata loss are permitted;
   *   UNSAFE_DOWNGRADE: any downgrade, including those which may result in
   *   metadata loss, are permitted).
   * @throws `IllegalArgumentError` when the level is zero with `UPGRADE`, or
   *   negative
   * @throws `TypeError` when the level is zero and the type null (Java
   *   dereferences it), or a value has the wrong type
   * @throws `RangeError` when the level is outside Java's `short`
   */
  constructor(options: {
    /** The new maximum version level for the finalized feature. */
    maxVersionLevel: number;
    /** Indicate what kind of upgrade should be performed in this operation. */
    upgradeType: FeatureUpdate.UpgradeType;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const maxVersionLevel = checkShort(FORMS.value(options, 'maxVersionLevel'), 'maxVersionLevel');
    const upgradeType = FORMS.value(options, 'upgradeType');
    if (upgradeType !== null && !(upgradeType instanceof FeatureUpdate.UpgradeType)) {
      throw new TypeError(`upgradeType must be a FeatureUpdate.UpgradeType, not ${kind(upgradeType)}`);
    }
    if (maxVersionLevel === 0) {
      if (upgradeType === null) throw new TypeError('upgradeType must not be null');
      if (upgradeType === FeatureUpdate.UpgradeType.UPGRADE) {
        throw new IllegalArgumentError(
          `The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:${maxVersionLevel} is < 1.`,
        );
      }
    }
    if (maxVersionLevel < 0) throw new IllegalArgumentError('Cannot specify a negative version level.');
    this.#maxVersionLevel = maxVersionLevel;
    this.#upgradeType = upgradeType;
  }

  /**
   * The new maximum version level for the finalized feature.
   *
   * @returns the level
   * @throws `TypeError` when it is given an argument: it only reads
   */
  maxVersionLevel(): number {
    if (arguments.length !== 0) VERSION_LEVEL_FORMS.match(undefined, arguments.length);
    return this.#maxVersionLevel;
  }

  /**
   * What kind of upgrade should be performed.
   *
   * @returns the upgrade type (null only when plain JavaScript gave the
   *   constructor null, which Java stores)
   * @throws `TypeError` when it is given an argument: it only reads
   */
  upgradeType(): FeatureUpdate.UpgradeType {
    if (arguments.length !== 0) UPGRADE_TYPE_FORMS.match(undefined, arguments.length);
    return this.#upgradeType as FeatureUpdate.UpgradeType;
  }

  /**
   * Whether `other` is a FeatureUpdate with the same level and upgrade type.
   *
   * @param other - the value to compare with, on its own or as `{ other }`
   * @returns whether they are equal
   * @throws `TypeError` when the levels are equal and this update's type is
   *   null (Java dereferences it)
   */
  equals(other: unknown): boolean {
    EQUALS_FORMS.match(other, arguments.length);
    const o = EQUALS_FORMS.value(other, 'other');
    if (o === this) return true;
    if (!(o instanceof FeatureUpdate)) return false;
    if (this.#maxVersionLevel !== o.#maxVersionLevel) return false;
    if (this.#upgradeType === null) throw new TypeError('upgradeType must not be null');
    return this.#upgradeType === o.#upgradeType;
  }

  /**
   * Java's `Objects.hash(maxVersionLevel, upgradeType)`, the type hashed by
   * its ordinal (Java's enum hash is the object's identity).
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#maxVersionLevel, this.#upgradeType === null ? null : this.#upgradeType.ordinal()]);
  }

  /**
   * Java's `toString()`: `FeatureUpdate{maxVersionLevel:2, upgradeType:UPGRADE}`.
   *
   * @returns the text
   */
  toString(): string {
    return `FeatureUpdate{maxVersionLevel:${this.#maxVersionLevel}, upgradeType:${javaString(this.#upgradeType)}}`;
  }

  /**
   * The update as `JSON.stringify` writes it: `{ maxVersionLevel,
   * upgradeType }`, the type by name.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { maxVersionLevel: number; upgradeType: string | null } {
    return { maxVersionLevel: this.#maxVersionLevel, upgradeType: this.#upgradeType === null ? null : this.#upgradeType.name() };
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested enum FeatureUpdate.UpgradeType, reached through the class name as in Java
export namespace FeatureUpdate {
  /**
   * The kinds of update of a finalized feature.
   */
  export class UpgradeType {
    readonly #ordinal: number;
    readonly #name: string;
    readonly #code: number;

    /**
     * Made by this class only (the enum's constants).
     *
     * @param ordinal - the constant's position in Java's declaration
     * @param name - the constant's name
     * @param code - Java's byte code
     */
    private constructor(ordinal: number, name: string, code: number) {
      this.#ordinal = ordinal;
      this.#name = name;
      this.#code = code;
      Object.freeze(this);
    }

    /** An upgrade type Java does not know. */
    static readonly UNKNOWN: UpgradeType = new UpgradeType(0, 'UNKNOWN', 0);

    /** Upgrading the feature level. */
    static readonly UPGRADE: UpgradeType = new UpgradeType(1, 'UPGRADE', 1);

    /** Only downgrades which do not result in metadata loss are permitted. */
    static readonly SAFE_DOWNGRADE: UpgradeType = new UpgradeType(2, 'SAFE_DOWNGRADE', 2);

    /** Any downgrade, including those which may result in metadata loss, are permitted. */
    static readonly UNSAFE_DOWNGRADE: UpgradeType = new UpgradeType(3, 'UNSAFE_DOWNGRADE', 3);

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): UpgradeType[] {
      TYPE_VALUES_FORMS.match(undefined, arguments.length);
      return [UpgradeType.UNKNOWN, UpgradeType.UPGRADE, UpgradeType.SAFE_DOWNGRADE, UpgradeType.UNSAFE_DOWNGRADE];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: string): UpgradeType;
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
    }): UpgradeType;
    static valueOf(name: unknown): UpgradeType {
      TYPE_VALUE_OF_FORMS.match(name, arguments.length);
      const value = TYPE_VALUE_OF_FORMS.value(name, 'name');
      if (value === null) throw new TypeError('Name is null');
      if (typeof value !== 'string') throw new TypeError(`name must be a string, not ${typeof value}`);
      const found = UpgradeType.values().find((t) => t.#name === value);
      if (found === undefined)
        throw new IllegalArgumentError(`No enum constant org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType.${value}`);
      return found;
    }

    /**
     * The upgrade type with the given code: `UNKNOWN` for a code Java does
     * not know.
     *
     * @param code - the code, on its own or as `{ code }`
     * @returns the upgrade type
     * @throws `TypeError` when the code is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    static fromCode(code: number): UpgradeType;
    /**
     * The same as `fromCode(code)`, the code given by name.
     *
     * @param options - the code
     * @returns the upgrade type
     * @throws `TypeError` when the code is not an integer
     * @throws `RangeError` when it is outside Java's `int`
     */
    static fromCode(options: {
      /** The code, a Java `int`. */
      code: number;
    }): UpgradeType;
    static fromCode(code: unknown): UpgradeType {
      TYPE_FROM_CODE_FORMS.match(code, arguments.length);
      const value = TYPE_FROM_CODE_FORMS.value(code, 'code');
      if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError('code must be an integer');
      if (value < -(2 ** 31) || value > 2 ** 31 - 1) throw new RangeError(`code must be from ${-(2 ** 31)} to ${2 ** 31 - 1}`);
      if (value === 1) return UpgradeType.UPGRADE;
      if (value === 2) return UpgradeType.SAFE_DOWNGRADE;
      if (value === 3) return UpgradeType.UNSAFE_DOWNGRADE;
      return UpgradeType.UNKNOWN;
    }

    /**
     * The upgrade type's code, a Java `byte`.
     *
     * @returns the code
     * @throws `TypeError` when it is given an argument: it only reads
     */
    code(): number {
      TYPE_CODE_FORMS.match(undefined, arguments.length);
      return this.#code;
    }

    /**
     * The name of this constant, exactly as declared, Java's `Enum.name()`.
     *
     * @returns the name
     * @throws `TypeError` when it is given an argument: it only reads
     */
    name(): string {
      TYPE_NAME_FORMS.match(undefined, arguments.length);
      return this.#name;
    }

    /**
     * The position of this constant in its enum declaration, Java's `Enum.ordinal()`.
     *
     * @returns the ordinal
     * @throws `TypeError` when it is given an argument: it only reads
     */
    ordinal(): number {
      TYPE_ORDINAL_FORMS.match(undefined, arguments.length);
      return this.#ordinal;
    }

    /**
     * The constant's name, as Java's `Enum.toString()`.
     *
     * @returns the name
     */
    toString(): string {
      return this.#name;
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
  }
}

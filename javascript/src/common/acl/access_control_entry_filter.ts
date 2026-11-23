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

// Java's org.apache.kafka.common.acl.AccessControlEntryFilter, a value class
// (root CLAUDE.md, JavaScript Binding Conventions, Class family): Java's
// public constructor, ANY, getters, matches, equals, hashCode and toString.
// Java's package-private constructor from AccessControlEntryData has no
// counterpart: AccessControlEntry.toFilter uses the public one, which takes
// the same fields.

import { JavaForms } from '../../internal/args';
import { listHashCode } from '../../internal/java_object';
import { AccessControlEntry } from './access_control_entry';
import { AclOperation } from './acl_operation';
import { AclPermissionType } from './acl_permission_type';

const FORMS = new JavaForms('AccessControlEntryFilter', [['principal', 'host', 'operation', 'permissionType']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const PRINCIPAL_FORMS = READER('principal');
const HOST_FORMS = READER('host');
const OPERATION_FORMS = READER('operation');
const PERMISSION_TYPE_FORMS = READER('permissionType');
const IS_UNKNOWN_FORMS = READER('isUnknown');
const MATCHES_AT_MOST_ONE_FORMS = READER('matchesAtMostOne');
const FIND_INDEFINITE_FIELD_FORMS = READER('findIndefiniteField');
const HASH_CODE_FORMS = READER('hashCode');
const MATCHES_FORMS = new JavaForms('matches', [['other']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Represents a filter which matches access control entries.
 */
export class AccessControlEntryFilter {
  readonly #principal: string | null;
  readonly #host: string | null;
  readonly #operation: AclOperation;
  readonly #permissionType: AclPermissionType;

  /**
   * Create an instance of an access control entry filter with the provided
   * parameters.
   *
   * @param options - the principal or null, the host or null, the non-null
   *   operation and the non-null permission type
   * @throws `TypeError` when the operation or the permission type is null,
   *   or a parameter is of the wrong type
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{principal, host, operation, permissionType}`
   */
  constructor(options: {
    /** The principal, or null. */
    principal: string | null;
    /** The host, or null. The value `*` means any host. */
    host: string | null;
    /** The operation. */
    operation: AclOperation;
    /** The permission type. */
    permissionType: AclPermissionType;
  }) {
    FORMS.match(options, arguments.length);
    const principal = FORMS.value(options, 'principal');
    const host = FORMS.value(options, 'host');
    const operation = FORMS.value(options, 'operation');
    const permissionType = FORMS.value(options, 'permissionType');
    if (principal !== null && typeof principal !== 'string') throw new TypeError('principal must be a string or null');
    if (host !== null && typeof host !== 'string') throw new TypeError('host must be a string or null');
    if (operation === null) throw new TypeError('operation must not be null');
    if (!(operation instanceof AclOperation)) throw new TypeError('operation must be an AclOperation');
    if (permissionType === null) throw new TypeError('permissionType must not be null');
    if (!(permissionType instanceof AclPermissionType)) throw new TypeError('permissionType must be an AclPermissionType');
    this.#principal = principal;
    this.#host = host;
    this.#operation = operation;
    this.#permissionType = permissionType;
  }

  /** Matches any access control entry. */
  static readonly ANY: AccessControlEntryFilter = new AccessControlEntryFilter({
    principal: null,
    host: null,
    operation: AclOperation.ANY,
    permissionType: AclPermissionType.ANY,
  });

  /**
   * Return the principal or null.
   *
   * @returns the principal, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  principal(): string | null {
    if (arguments.length !== 0) PRINCIPAL_FORMS.match(undefined, arguments.length);
    return this.#principal;
  }

  /**
   * Return the host or null. The value `*` means any host.
   *
   * @returns the host, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  host(): string | null {
    if (arguments.length !== 0) HOST_FORMS.match(undefined, arguments.length);
    return this.#host;
  }

  /**
   * Return the AclOperation.
   *
   * @returns the operation
   * @throws `TypeError` when it is given an argument: it only reads
   */
  operation(): AclOperation {
    if (arguments.length !== 0) OPERATION_FORMS.match(undefined, arguments.length);
    return this.#operation;
  }

  /**
   * Return the AclPermissionType.
   *
   * @returns the permission type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  permissionType(): AclPermissionType {
    if (arguments.length !== 0) PERMISSION_TYPE_FORMS.match(undefined, arguments.length);
    return this.#permissionType;
  }

  /**
   * Return true if there are any UNKNOWN components.
   *
   * @returns whether there are any UNKNOWN components
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    if (arguments.length !== 0) IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this.#operation.isUnknown() || this.#permissionType.isUnknown();
  }

  /**
   * Returns true if this filter matches the given AccessControlEntry.
   *
   * @param other - the entry, on its own or as `{ other }`
   * @returns whether this filter matches it
   * @throws `TypeError` when the entry is null where Java reads it, or not
   *   an AccessControlEntry
   */
  matches(other: AccessControlEntry): boolean;
  /**
   * The same as `matches(other)`, the parameter given by name.
   *
   * @param options - the entry
   * @returns whether this filter matches it
   */
  matches(options: {
    /** The entry. */
    other: AccessControlEntry;
  }): boolean;
  /**
   * Returns true if this filter matches the given AccessControlEntry.
   *
   * @param other - the entry, on its own or as `{ other }`
   * @returns whether this filter matches it
   */
  matches(other: unknown): boolean {
    MATCHES_FORMS.match(other, arguments.length);
    const given = MATCHES_FORMS.value(other, 'other');
    // Java dereferences the entry only where it reads it.
    const o = (): AccessControlEntry => {
      if (given === null) throw new TypeError('other must not be null');
      if (!(given instanceof AccessControlEntry)) throw new TypeError('other must be an AccessControlEntry');
      return given;
    };
    if (this.#principal !== null && this.#principal !== o().principal()) return false;
    if (this.#host !== null && this.#host !== o().host()) return false;
    if (this.#operation !== AclOperation.ANY && this.#operation !== o().operation()) return false;
    return this.#permissionType === AclPermissionType.ANY || this.#permissionType === o().permissionType();
  }

  /**
   * Returns true if this filter could only match one ACE -- in other words,
   * if there are no ANY or UNKNOWN fields.
   *
   * @returns whether it could only match one ACE
   * @throws `TypeError` when it is given an argument: it only reads
   */
  matchesAtMostOne(): boolean {
    if (arguments.length !== 0) MATCHES_AT_MOST_ONE_FORMS.match(undefined, arguments.length);
    return this.findIndefiniteField() === null;
  }

  /**
   * Returns a string describing an ANY or UNKNOWN field, or null if there is
   * no such field.
   *
   * @returns the description, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  findIndefiniteField(): string | null {
    if (arguments.length !== 0) FIND_INDEFINITE_FIELD_FORMS.match(undefined, arguments.length);
    if (this.#principal === null) return 'Principal is NULL';
    if (this.#host === null) return 'Host is NULL';
    if (this.#operation === AclOperation.ANY) return 'Operation is ANY';
    if (this.#operation === AclOperation.UNKNOWN) return 'Operation is UNKNOWN';
    if (this.#permissionType === AclPermissionType.ANY) return 'Permission type is ANY';
    if (this.#permissionType === AclPermissionType.UNKNOWN) return 'Permission type is UNKNOWN';
    return null;
  }

  /**
   * Whether `o` is an AccessControlEntryFilter with the same four fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (!(other instanceof AccessControlEntryFilter)) return false;
    return (
      this.#principal === other.#principal &&
      this.#host === other.#host &&
      this.#operation === other.#operation &&
      this.#permissionType === other.#permissionType
    );
  }

  /**
   * Java's `hashCode()`: `Objects.hash(principal, host, operation,
   * permissionType)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#principal, this.#host, this.#operation, this.#permissionType]);
  }

  /**
   * Java's `toString()`: `(principal=<p>, host=<h>, operation=<op>,
   * permissionType=<type>)`, a null principal or host as `<any>`.
   *
   * @returns the text
   */
  toString(): string {
    const principal = this.#principal ?? '<any>';
    const host = this.#host ?? '<any>';
    return `(principal=${principal}, host=${host}, operation=${this.#operation.name()}, permissionType=${this.#permissionType.name()})`;
  }

  /**
   * The filter as `JSON.stringify` writes it: `{ principal, host, operation,
   * permissionType }`, the enums by name, a null principal or host null.
   *
   * @returns the filter's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { principal: string | null; host: string | null; operation: string; permissionType: string } {
    return { principal: this.#principal, host: this.#host, operation: this.#operation.name(), permissionType: this.#permissionType.name() };
  }
}

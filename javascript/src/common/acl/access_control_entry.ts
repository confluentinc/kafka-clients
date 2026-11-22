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

// Java's org.apache.kafka.common.acl.AccessControlEntry, a value class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family): Java's public
// constructor, getters, toFilter, equals, hashCode and toString. Java keeps
// the four fields in the package-private AccessControlEntryData, shared with
// AccessControlEntryFilter; here each class holds them itself.

import { JavaForms } from '../../internal/args';
import { listHashCode } from '../../internal/java_object';
import { IllegalArgumentError } from '../../illegal_argument_error';
import { AccessControlEntryFilter } from './access_control_entry_filter';
import { AclOperation } from './acl_operation';
import { AclPermissionType } from './acl_permission_type';

const FORMS = new JavaForms('AccessControlEntry', [['principal', 'host', 'operation', 'permissionType']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const PRINCIPAL_FORMS = READER('principal');
const HOST_FORMS = READER('host');
const OPERATION_FORMS = READER('operation');
const PERMISSION_TYPE_FORMS = READER('permissionType');
const TO_FILTER_FORMS = READER('toFilter');
const IS_UNKNOWN_FORMS = READER('isUnknown');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Represents an access control entry. ACEs are a tuple of principal, host,
 * operation, and permissionType.
 */
export class AccessControlEntry {
  readonly #principal: string;
  readonly #host: string;
  readonly #operation: AclOperation;
  readonly #permissionType: AclPermissionType;

  /**
   * Create an instance of an access control entry with the provided
   * parameters.
   *
   * @param options - the non-null principal, the non-null host, the non-null
   *   operation (`ANY` is not an allowed operation) and the non-null
   *   permission type (`ANY` is not an allowed type)
   * @throws `TypeError` when a parameter is null or of the wrong type
   * @throws `IllegalArgumentError` when the operation or the permission type
   *   is `ANY`, or the given parameters are not `{principal, host, operation,
   *   permissionType}`
   */
  constructor(options: {
    /** The principal. */
    principal: string;
    /** The host, or `*` for all hosts. */
    host: string;
    /** The operation; `ANY` is not an allowed operation. */
    operation: AclOperation;
    /** The permission type; `ANY` is not an allowed type. */
    permissionType: AclPermissionType;
  }) {
    FORMS.match(options, arguments.length);
    const principal = FORMS.value(options, 'principal');
    const host = FORMS.value(options, 'host');
    const operation = FORMS.value(options, 'operation');
    const permissionType = FORMS.value(options, 'permissionType');
    if (principal === null) throw new TypeError('principal must not be null');
    if (typeof principal !== 'string') throw new TypeError('principal must be a string');
    if (host === null) throw new TypeError('host must not be null');
    if (typeof host !== 'string') throw new TypeError('host must be a string');
    if (operation === null) throw new TypeError('operation must not be null');
    if (!(operation instanceof AclOperation)) throw new TypeError('operation must be an AclOperation');
    if (operation === AclOperation.ANY) throw new IllegalArgumentError('operation must not be ANY');
    if (permissionType === null) throw new TypeError('permissionType must not be null');
    if (!(permissionType instanceof AclPermissionType)) throw new TypeError('permissionType must be an AclPermissionType');
    if (permissionType === AclPermissionType.ANY) throw new IllegalArgumentError('permissionType must not be ANY');
    this.#principal = principal;
    this.#host = host;
    this.#operation = operation;
    this.#permissionType = permissionType;
  }

  /**
   * Return the principal for this entry.
   *
   * @returns the principal
   * @throws `TypeError` when it is given an argument: it only reads
   */
  principal(): string {
    if (arguments.length !== 0) PRINCIPAL_FORMS.match(undefined, arguments.length);
    return this.#principal;
  }

  /**
   * Return the host or `*` for all hosts.
   *
   * @returns the host
   * @throws `TypeError` when it is given an argument: it only reads
   */
  host(): string {
    if (arguments.length !== 0) HOST_FORMS.match(undefined, arguments.length);
    return this.#host;
  }

  /**
   * Return the AclOperation. This method will never return AclOperation.ANY.
   *
   * @returns the operation
   * @throws `TypeError` when it is given an argument: it only reads
   */
  operation(): AclOperation {
    if (arguments.length !== 0) OPERATION_FORMS.match(undefined, arguments.length);
    return this.#operation;
  }

  /**
   * Return the AclPermissionType. This method will never return
   * AclPermissionType.ANY.
   *
   * @returns the permission type
   * @throws `TypeError` when it is given an argument: it only reads
   */
  permissionType(): AclPermissionType {
    if (arguments.length !== 0) PERMISSION_TYPE_FORMS.match(undefined, arguments.length);
    return this.#permissionType;
  }

  /**
   * Create a filter which matches only this AccessControlEntry.
   *
   * @returns the filter
   * @throws `TypeError` when it is given an argument: it only reads
   */
  toFilter(): AccessControlEntryFilter {
    if (arguments.length !== 0) TO_FILTER_FORMS.match(undefined, arguments.length);
    return new AccessControlEntryFilter({
      principal: this.#principal,
      host: this.#host,
      operation: this.#operation,
      permissionType: this.#permissionType,
    });
  }

  /**
   * Return true if this AclResource has any UNKNOWN components.
   *
   * @returns whether it has any UNKNOWN components
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    if (arguments.length !== 0) IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this.#operation.isUnknown() || this.#permissionType.isUnknown();
  }

  /**
   * Whether `o` is an AccessControlEntry with the same four fields.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (!(other instanceof AccessControlEntry)) return false;
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
   * permissionType=<type>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(principal=${this.#principal}, host=${this.#host}, operation=${this.#operation.name()}, permissionType=${this.#permissionType.name()})`;
  }

  /**
   * The entry as `JSON.stringify` writes it: `{ principal, host, operation,
   * permissionType }`, the enums by name.
   *
   * @returns the entry's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { principal: string; host: string; operation: string; permissionType: string } {
    return { principal: this.#principal, host: this.#host, operation: this.#operation.name(), permissionType: this.#permissionType.name() };
  }
}

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

// Java's CloseOptions, an option class (javascript/CLAUDE.md, JavaScript
// Binding Conventions, Class family): its constructor takes one object of
// keys, one per setter field, each given key calling its setter in Java's
// field order (`operation`, then `timeout`), although Java's constructor is
// private. Java's nested enum GroupMembershipOperation is a class of the
// namespace merged with CloseOptions, so a static member of it (Names). A
// timeout is kept in whole milliseconds; a null one means "not set", and a
// negative one is kept as it is, as Java's is (Signatures).

import type { Duration } from '../duration';
import { JavaForms } from '../internal/args';
import { durationToMillis, isDurationOrNull, UNBOUNDED_MILLIS } from '../internal/duration';
import { applyOptionKeys } from '../internal/options';
import { IllegalArgumentError } from '../illegal_argument_error';

const isOperation = (value: unknown): boolean => value === null || value instanceof CloseOptions.GroupMembershipOperation;
const STATIC_TIMEOUT_FORMS = new JavaForms('timeout', [['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const STATIC_OPERATION_FORMS = new JavaForms('groupMembershipOperation', [['operation']], { bareTypes: { operation: isOperation } });
const WITH_TIMEOUT_FORMS = new JavaForms('withTimeout', [['timeout']], { bareTypes: { timeout: isDurationOrNull } });
const WITH_OPERATION_FORMS = new JavaForms('withGroupMembershipOperation', [['operation']], { bareTypes: { operation: isOperation } });
const TIMEOUT_FORMS = new JavaForms('timeout', [[]], { reader: true });
const OPERATION_FORMS = new JavaForms('groupMembershipOperation', [[]], { reader: true });

/**
 * Options for controlling the consumer close behavior.
 *
 * This class allows customization of the close timeout and group membership
 * operation when a consumer is being shut down.
 */
export class CloseOptions {
  /**
   * Specifies the group membership operation upon shutdown.
   * By default, `GroupMembershipOperation.DEFAULT` will be applied, which follows the consumer's default behavior.
   */
  #operation: CloseOptions.GroupMembershipOperation = CloseOptions.GroupMembershipOperation.DEFAULT;

  /**
   * Specifies the maximum amount of time to wait for the close process to complete, in milliseconds, or null:
   * if no value is set, the default timeout will be applied.
   */
  #timeout: number | bigint | null = null;

  /**
   * Builds the options from one object of keys, each given key calling its
   * setter, `withGroupMembershipOperation` then `withTimeout`; a key left out,
   * or `undefined`, leaves Java's starting value.
   *
   * @param options - the keys
   * @throws `IllegalArgumentError` when it is given an unknown key or more than
   *   one argument
   * @throws `TypeError` when the argument is not a plain object, or a setter
   *   refuses its value
   * @throws `RangeError` when the timeout is `NaN`, `-Infinity`, beyond
   *   `Number.MAX_SAFE_INTEGER` milliseconds, or a `Temporal.Duration` with
   *   years, months or weeks
   *
   * @remarks *(deviation)* Java's constructor is private, and Java builds the
   * options only through its static methods and setters. Why: one call builds
   * a configured object, and the setters stay, so Java's chain works too.
   */
  constructor(options?: {
    /**
     * The group membership operation to apply.
     *
     * @defaultValue `CloseOptions.GroupMembershipOperation.DEFAULT`
     */
    operation?: CloseOptions.GroupMembershipOperation;
    /**
     * The maximum time to wait for the consumer to close. If null, the
     * default timeout will be used.
     *
     * @defaultValue `null`
     */
    timeout?: Duration | null;
  }) {
    applyOptionKeys('CloseOptions', options, arguments.length, [
      ['operation', (value) => void this.withGroupMembershipOperation({ operation: value as CloseOptions.GroupMembershipOperation })],
      ['timeout', (value) => void this.withTimeout({ timeout: value as Duration | null })],
    ]);
  }

  /**
   * Static method to create a `CloseOptions` with a custom timeout.
   *
   * @param options - The maximum time to wait for the consumer to close.
   * @returns A new `CloseOptions` instance with the specified timeout.
   * @throws `RangeError` when the timeout is not a whole count of milliseconds
   *   a number holds
   */
  static timeout(options: {
    /** The maximum time to wait for the consumer to close, or null for the default. */
    timeout: Duration | null;
  }): CloseOptions;
  /**
   * Static method to create a `CloseOptions` with a custom timeout, the same
   * as `CloseOptions.timeout({ timeout })`.
   *
   * @param timeout - The maximum time to wait for the consumer to close.
   * @returns A new `CloseOptions` instance with the specified timeout.
   */
  static timeout(timeout: Duration | null): CloseOptions;
  static timeout(options: unknown): CloseOptions {
    STATIC_TIMEOUT_FORMS.match(options, arguments.length);
    return new CloseOptions().withTimeout({ timeout: STATIC_TIMEOUT_FORMS.value(options, 'timeout') as Duration | null });
  }

  /**
   * Static method to create a `CloseOptions` with a specified group
   * membership operation.
   *
   * @param options - The group membership operation to apply. Must be one of
   *   `LEAVE_GROUP`, `REMAIN_IN_GROUP`, or `DEFAULT`.
   * @returns A new `CloseOptions` instance with the specified group membership operation.
   * @throws `TypeError` when the operation is null ("operation should not be null")
   */
  static groupMembershipOperation(options: {
    /** The group membership operation to apply. */
    operation: CloseOptions.GroupMembershipOperation;
  }): CloseOptions;
  /**
   * Static method to create a `CloseOptions` with a specified group
   * membership operation, the same as
   * `CloseOptions.groupMembershipOperation({ operation })`.
   *
   * @param operation - The group membership operation to apply.
   * @returns A new `CloseOptions` instance with the specified group membership operation.
   */
  static groupMembershipOperation(operation: CloseOptions.GroupMembershipOperation): CloseOptions;
  static groupMembershipOperation(options: unknown): CloseOptions {
    STATIC_OPERATION_FORMS.match(options, arguments.length);
    const operation = STATIC_OPERATION_FORMS.value(options, 'operation') as CloseOptions.GroupMembershipOperation;
    return new CloseOptions().withGroupMembershipOperation({ operation });
  }

  /**
   * Fluent method to set the timeout for the close process.
   *
   * @param options - The maximum time to wait for the consumer to close. If
   *   null, the default timeout will be used.
   * @returns This `CloseOptions` instance.
   * @throws `RangeError` when the timeout is not a whole count of milliseconds
   *   a number holds
   */
  withTimeout(options: {
    /** The maximum time to wait for the consumer to close, or null for the default. */
    timeout: Duration | null;
  }): this;
  /**
   * Fluent method to set the timeout for the close process, the same as
   * `withTimeout({ timeout })`.
   *
   * @param timeout - The maximum time to wait for the consumer to close. If
   *   null, the default timeout will be used.
   * @returns This `CloseOptions` instance.
   */
  withTimeout(timeout: Duration | null): this;
  withTimeout(options: unknown): this {
    WITH_TIMEOUT_FORMS.match(options, arguments.length);
    const timeout = WITH_TIMEOUT_FORMS.value(options, 'timeout');
    // Java's Optional.ofNullable: null is "not set".
    this.#timeout = timeout === null ? null : durationToMillis(timeout, 'timeout');
    return this;
  }

  /**
   * Fluent method to set the group membership operation upon shutdown.
   *
   * @param options - The group membership operation to apply. Must be one of
   *   `LEAVE_GROUP`, `REMAIN_IN_GROUP`, or `DEFAULT`.
   * @returns This `CloseOptions` instance.
   * @throws `TypeError` when the operation is null ("operation should not be
   *   null") or not a `GroupMembershipOperation`
   */
  withGroupMembershipOperation(options: {
    /** The group membership operation to apply. */
    operation: CloseOptions.GroupMembershipOperation;
  }): this;
  /**
   * Fluent method to set the group membership operation upon shutdown, the
   * same as `withGroupMembershipOperation({ operation })`.
   *
   * @param operation - The group membership operation to apply.
   * @returns This `CloseOptions` instance.
   */
  withGroupMembershipOperation(operation: CloseOptions.GroupMembershipOperation): this;
  withGroupMembershipOperation(options: unknown): this {
    WITH_OPERATION_FORMS.match(options, arguments.length);
    const operation = WITH_OPERATION_FORMS.value(options, 'operation');
    // Java's Objects.requireNonNull(operation, "operation should not be null").
    if (operation === null) throw new TypeError('operation should not be null');
    if (!(operation instanceof CloseOptions.GroupMembershipOperation)) {
      throw new TypeError('operation must be a CloseOptions.GroupMembershipOperation');
    }
    this.#operation = operation;
    return this;
  }

  /**
   * Returns the group membership operation configured for this close.
   *
   * @returns The group membership operation
   * @throws `TypeError` when it is given an argument
   */
  groupMembershipOperation(): CloseOptions.GroupMembershipOperation {
    OPERATION_FORMS.match(undefined, arguments.length);
    return this.#operation;
  }

  /**
   * Returns the timeout configured for this close.
   *
   * @returns The timeout in milliseconds (`Infinity` for no limit), or null
   *   if using the default timeout
   * @throws `TypeError` when it is given an argument
   */
  timeout(): number | null {
    TIMEOUT_FORMS.match(undefined, arguments.length);
    const timeout = this.#timeout;
    return timeout === UNBOUNDED_MILLIS ? Infinity : (timeout as number | null);
  }
}

const JAVA_CLASS = 'org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation';
const isName = (value: unknown): boolean => value === null || typeof value === 'string';
const VALUE_OF_FORMS = new JavaForms('valueOf', [['name']], { bareTypes: { name: isName } });
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const ORDINAL_FORMS = new JavaForms('ordinal', [[]], { reader: true });

// Java's nested enum, a static member of the class (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Names).
// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested enum CloseOptions.GroupMembershipOperation, reached through the class name as in Java
export namespace CloseOptions {
  /**
   * Enum to specify the group membership operation upon leaving a group.
   *
   * - **`LEAVE_GROUP`**: The consumer will leave the group. This is the
   *   default for dynamic members, and can be used by static members when they
   *   want to permanently leave the group and trigger a rebalance.
   * - **`REMAIN_IN_GROUP`**: The consumer will remain in the group. This is
   *   the default for static members, allowing them to rejoin quickly without
   *   triggering a rebalance. When used by dynamic members, no leave heartbeat
   *   will be sent and the member will be removed by the coordinator after the
   *   session timeout expires.
   * - **`DEFAULT`**: Applies the default behavior:
   *   - For **static members**: The consumer will remain in the group.
   *   - For **dynamic members**: The consumer will leave the group.
   */
  export class GroupMembershipOperation {
    readonly #name: string;

    readonly #ordinal: number;

    /**
     * Made by this class only (the enum's constants).
     *
     * @param ordinal - the constant's position in Java's declaration, its `ordinal()`
     * @param name - the constant's name, its `name()`
     */
    private constructor(ordinal: number, name: string) {
      this.#name = name;
      this.#ordinal = ordinal;
      Object.freeze(this);
    }

    /** The consumer will leave the group. */
    static readonly LEAVE_GROUP: GroupMembershipOperation = new GroupMembershipOperation(0, 'LEAVE_GROUP');

    /** The consumer will remain in the group. */
    static readonly REMAIN_IN_GROUP: GroupMembershipOperation = new GroupMembershipOperation(1, 'REMAIN_IN_GROUP');

    /** Applies the default behavior: static members remain in the group, and dynamic members leave it. */
    static readonly DEFAULT: GroupMembershipOperation = new GroupMembershipOperation(2, 'DEFAULT');

    /**
     * The constants in declaration order, Java's `values()`.
     *
     * @returns a new array of the constants
     * @throws `TypeError` when it is given an argument: it only reads
     */
    static values(): GroupMembershipOperation[] {
      VALUES_FORMS.match(undefined, arguments.length);
      return [GroupMembershipOperation.LEAVE_GROUP, GroupMembershipOperation.REMAIN_IN_GROUP, GroupMembershipOperation.DEFAULT];
    }

    /**
     * The constant named exactly `name`, Java's `valueOf(String)`.
     *
     * @param name - the name of the constant to return
     * @returns the constant with the specified name
     * @throws `IllegalArgumentError` when no constant has that name, or the
     *   given parameters are not `{name}`
     * @throws `TypeError` when the name is null or not a string
     */
    static valueOf(name: string): GroupMembershipOperation;
    /**
     * The constant named exactly `name`, the same as `valueOf(name)`.
     *
     * @param options - the constant's name
     * @returns the constant
     * @throws `IllegalArgumentError` when no constant has that name
     */
    static valueOf(options: {
      /** The name of the constant to return. */
      name: string;
    }): GroupMembershipOperation;
    static valueOf(options: unknown): GroupMembershipOperation {
      VALUE_OF_FORMS.match(options, arguments.length);
      const name = VALUE_OF_FORMS.value(options, 'name');
      // java.lang.Enum.valueOf: requireNonNull's "Name is null".
      if (name === null) throw new TypeError('Name is null');
      if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${typeof name}`);
      const found = GroupMembershipOperation.values().find((c) => c.#name === name);
      if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${name}`);
      return found;
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
     * The position of this constant in its enum declaration, the first one
     * being 0, Java's `Enum.ordinal()`.
     *
     * @returns the ordinal
     * @throws `TypeError` when it is given an argument: it only reads
     */
    ordinal(): number {
      ORDINAL_FORMS.match(undefined, arguments.length);
      return this.#ordinal;
    }

    /**
     * Java's `Enum.toString()`: the name.
     *
     * @returns the name
     */
    toString(): string {
      return this.#name;
    }

    /**
     * The name, which `JSON.stringify` writes for the constant.
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

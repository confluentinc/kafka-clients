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

// Java's org.apache.kafka.clients.admin.AbstractOptions: a TypeScript
// abstract class whose constructor throws TypeError when called for
// AbstractOptions itself (root CLAUDE.md, JavaScript Binding Conventions,
// Class family, Q16). Its type parameter `T extends AbstractOptions` stands
// for the class itself, so the setter returns `this` (Q26). The options
// record the setters that ran, in the order they last ran, which the client
// runs again on the native options (`<X>Options_new`, then `set_<field>`).

import { checkTimeoutMs, GetterSetter } from '../internal/options';

const TIMEOUT_MS = new GetterSetter('timeoutMs', (value) => value === null || typeof value === 'number');

/**
 * This class implements the common APIs that are shared by Options classes
 * for various AdminClient commands.
 */
export abstract class AbstractOptions {
  #timeoutMs: number | null = null;

  // The setters that ran, the last one last, with the value each was given.
  readonly #setters = new Map<string, unknown>();

  /**
   * Called by a subclass.
   *
   * @throws `TypeError` when called for `AbstractOptions` itself, which is
   *   abstract, as Java's is
   */
  constructor() {
    if (new.target === AbstractOptions) throw new TypeError('AbstractOptions cannot be constructed directly; use one of its subclasses');
  }

  /**
   * The timeout in milliseconds for this operation or `null` if the default
   * api timeout for the AdminClient should be used.
   *
   * @returns the timeout, or null
   */
  timeoutMs(): number | null;
  /**
   * Set the timeout in milliseconds for this operation or `null` if the
   * default api timeout for the AdminClient should be used.
   *
   * @param timeoutMs - the timeout, or null, on its own
   * @returns this object
   */
  timeoutMs(timeoutMs: number | null): this;
  /**
   * Set the timeout in milliseconds for this operation, the same as
   * `timeoutMs(timeoutMs)`.
   *
   * @param options - the timeout
   * @returns this object
   */
  timeoutMs(options: {
    /** The timeout in milliseconds, or null for the default api timeout. */
    timeoutMs: number | null;
  }): this;
  /**
   * Reads or sets the timeout.
   *
   * @param options - the timeout to set, or nothing to read it
   * @returns the timeout, or this object
   * @throws `RangeError` for `NaN`, an infinity, or a value outside 32 bits
   * @throws `TypeError` when it is neither null nor a whole number
   *
   * @remarks A negative timeout is sent as 0, which Java's deadline makes of
   * it.
   */
  timeoutMs(options?: unknown): number | null | this {
    if (!TIMEOUT_MS.sets(options, arguments.length)) return this.#timeoutMs;
    this.#timeoutMs = checkTimeoutMs(TIMEOUT_MS.value(options));
    this.internalRecord('timeoutMs', this.#timeoutMs);
    return this;
  }

  /**
   * Records that setter `name` ran with `value`, the latest run last. Called
   * by the subclasses' setters.
   *
   * @param name - the setter's Java name
   * @param value - the value it was given
   * @internal
   */
  internalRecord(name: string, value: unknown): void {
    this.#setters.delete(name);
    this.#setters.set(name, value);
  }

  /**
   * The setters that ran, in the order they last ran, as `[setter, value]`
   * pairs, for the native options. Called by the admin client.
   *
   * @returns the pairs
   * @internal
   */
  internalSetters(): Array<[string, unknown]> {
    return [...this.#setters];
  }
}

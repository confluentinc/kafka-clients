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

// The option classes' keyed constructors and their getter / setter methods
// (private): javascript/CLAUDE.md, JavaScript Binding Conventions, Class family
// (Q17, Q19). An option class takes one object of keys, one per setter field,
// named after the Java field; each key given, null included, calls its setter
// after construction, in the class's fixed order (the parent's fields first,
// then Java's field order, a setter reading a field after the one setting
// it), and a key left out, or given as `undefined`, leaves Java's starting
// value. A getter and setter with one Java name are one method: no argument
// reads, one argument sets, taken on its own or as `{ <name> }`.

import { JavaForms, kind } from './args';
import { illegalArgument } from './errors';

/** One key of an option class's constructor and the setter it calls. */
export type OptionKey = readonly [name: string, set: (value: unknown) => void];

/**
 * Runs the setters of the keys `args` gives, in `keys`' order.
 *
 * @param className - the class's name, for the errors
 * @param args - the constructor's argument
 * @param count - how many arguments the constructor was given
 * @param keys - the keys and their setters, in the fixed order
 */
export function applyOptionKeys(className: string, args: unknown, count: number, keys: readonly OptionKey[]): void {
  const names = keys.map(([name]) => name);
  const shape = `${className}() takes an object of the keys (${names.join(', ')})`;
  if (count > 1) throw illegalArgument(`${shape}; got ${count} arguments`);
  if (count === 0 || args === undefined) return;
  if (args === null || typeof args !== 'object' || Array.isArray(args)) throw new TypeError(`${shape}, not ${kind(args)}`);
  const proto: unknown = Object.getPrototypeOf(args);
  if (proto !== Object.prototype && proto !== null) throw new TypeError(`${shape}, not ${kind(args)}`);
  const record = args as Record<string, unknown>;
  const unknown = Object.keys(record).filter((key) => !names.includes(key));
  if (unknown.length > 0) throw illegalArgument(`${shape}; got (${unknown.join(', ')})`);
  for (const [name, set] of keys) {
    if (Object.prototype.propertyIsEnumerable.call(record, name) && record[name] !== undefined) set(record[name]);
  }
}

/** The forms of a getter and setter sharing Java name `name`: `()` and `(<name>)`. */
export class GetterSetter {
  readonly #name: string;
  readonly #param: string;
  readonly #forms: JavaForms;

  /**
   * @param name - the Java name
   * @param isValue - the test of the value given on its own
   * @param param - the setter's parameter's Java name, when it is not the
   *   method's (`allowReplicationFactorChange(boolean allow)`)
   */
  constructor(name: string, isValue: (value: unknown) => boolean, param: string = name) {
    this.#name = name;
    this.#param = param;
    this.#forms = new JavaForms(name, [[], [param]], { bareTypes: { [param]: isValue } });
  }

  /**
   * Whether a call sets (the value given) or reads (no argument).
   *
   * @param args - the call's first argument
   * @param count - how many arguments the call passed
   * @returns true for a call that sets
   * @throws `TypeError` for `undefined` or an empty options object, which
   *   neither read nor set
   */
  sets(args: unknown, count: number): boolean {
    if (count === 0) return false;
    if (this.#forms.match(args, count) === 0) {
      throw new TypeError(`${this.#name}() takes the value to set: leave the argument out to read it`);
    }
    return true;
  }

  /**
   * The value a call that sets gives.
   *
   * @param args - the call's first argument
   * @returns the value
   */
  value(args: unknown): unknown {
    return this.#forms.value(args, this.#param);
  }
}

/** The forms of a setter with no reader of its name: `(<name>)` only. */
export class Setter {
  readonly #name: string;
  readonly #param: string;
  readonly #reader: string | null;
  readonly #forms: JavaForms;

  /**
   * @param name - the setter's Java name
   * @param reader - the reader's Java name, for the error, or null for none
   * @param isValue - the test of the value given on its own
   * @param param - the parameter's Java name, when it is not the setter's
   */
  constructor(name: string, reader: string | null, isValue: (value: unknown) => boolean, param: string = name) {
    this.#name = name;
    this.#param = param;
    this.#reader = reader;
    this.#forms = new JavaForms(name, [[param]], { bareTypes: { [param]: isValue } });
  }

  /**
   * The value a call gives.
   *
   * @param args - the call's first argument
   * @param count - how many arguments the call passed
   * @returns the value
   * @throws `TypeError` for no argument: the setter does not read
   */
  value(args: unknown, count: number): unknown {
    if (count === 0) {
      const read = this.#reader === null ? '' : `: read it with ${this.#reader}()`;
      throw new TypeError(`${this.#name}() takes the value to set${read}`);
    }
    this.#forms.match(args, count);
    return this.#forms.value(args, this.#param);
  }
}

/**
 * A Java `boolean` setter's value.
 *
 * @param value - the value given
 * @param name - the parameter's name
 * @returns the boolean
 * @throws `TypeError` when it is not a boolean (null included: Java's
 *   primitive cannot hold null)
 */
export function checkBoolean(value: unknown, name: string): boolean {
  if (typeof value !== 'boolean') throw new TypeError(`${name} must be a boolean, not ${kind(value)}`);
  return value;
}

/**
 * Java's `Integer timeoutMs` (javascript/CLAUDE.md, JavaScript Binding Conventions,
 * Signatures, Q22): null, or a whole number in 32 bits; the addon sends a
 * negative one as 0.
 *
 * @param value - the value given
 * @returns the timeout, or null
 * @throws `TypeError` when it is neither null nor a whole number
 * @throws `RangeError` for `NaN`, an infinity, or a value outside 32 bits
 */
export function checkTimeoutMs(value: unknown): number | null {
  if (value === null) return null;
  if (typeof value !== 'number') throw new TypeError(`timeoutMs must be a number or null, not ${kind(value)}`);
  if (!Number.isFinite(value)) throw new RangeError(`timeoutMs must be finite, not ${String(value)}`);
  if (!Number.isInteger(value)) throw new TypeError('timeoutMs must be an integer');
  if (value < -(2 ** 31) || value > 2 ** 31 - 1) throw new RangeError(`timeoutMs must be from ${-(2 ** 31)} to ${2 ** 31 - 1}`);
  return value;
}

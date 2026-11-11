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

// Java overload matching for a method that takes its parameters as one object
// (private): python/confluent_kafka/_args.py's `java_forms`, in JavaScript.
//
// Java resolves an overload at compile time. A JavaScript method that collapses
// a Java overload set into one object of named parameters accepts the union of
// the overloads' parameters, so it must reject at run time any set of given
// names that matches no Java overload. `JavaForms` lists the method's Java
// overloads ("forms"), unfolded, each with its parameter names in Java
// declaration order. A set of given names matches a form only when it is
// exactly that form's parameter set; nothing is filled in to make a match.
//
// The argument is either the options object or, for a form with one
// parameter, that parameter's value on its own (the bare value: `send(record)`,
// `close(5000)`). A plain object is the options object; anything else is the
// bare value. One-parameter forms of different names take a bare value only
// when `bareTypes` tells them apart by its type (an error's `(message)`, a
// string, and `(cause)`, an `Error`: `new KafkaError('boom')`). When the bare
// value is itself a plain object (a configuration map,
// `new KafkaProducer({ 'bootstrap.servers': ... })`), a plain object is the
// options object only when one of its keys is a parameter name.
//
// "Given" means present: an own enumerable key. `null` is Java's `null`,
// which counts as given and selects the form that takes the parameter. A key
// present with `undefined` throws `TypeError` (to leave a parameter out, leave
// its key out). A name no form has is rejected as a set no form matches:
//
//     close() takes one of (), (timeout); got (timeoutMs)
//
// The method then reads each parameter through `value()`, which reads it as
// `match()` did: an own enumerable key of the options object, or the bare
// value, never an inherited or non-enumerable property.
//
// The table of forms is built once, when the method's module loads; a call
// costs one pass over the object's keys and one map lookup, with no
// allocation unless it fails.

import { illegalArgument } from './errors';

/** One Java form: its parameter names, in Java declaration order. */
export type Form = readonly string[];

/**
 * The JavaScript type of a value, as a form error names it, in Python's form:
 * `a string`, `a number`, `an array`, `null`, `a Map` (an object, by its
 * class).
 */
export function kind(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'an array';
  let name: string = typeof value;
  if (typeof value === 'object') {
    try {
      const proto: unknown = Object.getPrototypeOf(value);
      const ctor: unknown = proto === null ? undefined : (proto as { constructor?: unknown }).constructor;
      if (typeof ctor === 'function' && typeof ctor.name === 'string' && ctor.name !== '' && ctor !== Object) name = ctor.name;
    } catch {
      // A revoked Proxy: an object.
    }
  }
  return /^[aeiouAEIOU]/.test(name) ? `an ${name}` : `a ${name}`;
}

/** Options of a `JavaForms`. */
export interface JavaFormsOptions {
  /**
   * The order a message lists given names in; by default each name where it
   * first appears in the forms.
   */
  readonly order?: readonly string[];
  /**
   * The bare value of the one-parameter form is itself a plain object (a
   * configuration map): a plain object is then the options object only when
   * one of its own keys is a parameter name.
   */
  readonly bareIsPlainObject?: boolean;
  /** The `TypeError` message for a parameter given as `undefined`, by parameter name. */
  readonly undefinedMessages?: Readonly<Record<string, string>>;
  /**
   * The test of each one-parameter form's bare value, by parameter name,
   * tried in the forms' order: the bare value picks its form by its type
   * (`typeof`, `Array.isArray` or `instanceof`), and one no test accepts is
   * the form error. One-parameter forms of different names take a bare value
   * only when each has a test (an error's `message`, a string, and `cause`, an
   * `Error`); a lone one-parameter form without a test takes any value.
   */
  readonly bareTypes?: Readonly<Record<string, (value: unknown) => boolean>>;
  /**
   * The method only reads (`isDone()`, `name()`): it takes no argument, and
   * any argument is a `TypeError` (root CLAUDE.md, JavaScript Binding
   * Conventions, Class family).
   */
  readonly reader?: boolean;
  /** The form error's message for a call of `count` arguments, when not the default. */
  readonly manyArguments?: (count: number) => string;
}

/** Whether `value` is a plain object: an object whose prototype is `Object.prototype` or null. */
function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  const proto: unknown = Object.getPrototypeOf(value);
  return proto === Object.prototype || proto === null;
}

/** Whether `name` is an own enumerable property of `args`, what counts as given. */
function given(args: object, name: string): boolean {
  return Object.prototype.propertyIsEnumerable.call(args, name);
}

/** The Java forms of one method (or constructor), and the check of a call against them. */
export class JavaForms {
  readonly #method: string;
  // Each parameter name of the forms -> its bit, in the order a message lists
  // the names (a Map keeps its insertion order).
  readonly #bit: ReadonlyMap<string, number>;
  // The given-name mask of each form -> the form's index.
  readonly #forms: ReadonlyMap<number, number>;
  readonly #prefix: string;
  // The one-parameter forms that take a bare value: parameter, index and, when
  // the forms are told apart by the value's type, its test.
  readonly #bare: ReadonlyArray<readonly [name: string, index: number, test: ((value: unknown) => boolean) | null]>;
  readonly #bareIsPlainObject: boolean;
  readonly #reader: boolean;
  readonly #manyArguments: ((count: number) => string) | null;
  readonly #undefinedMessages: Readonly<Record<string, string>>;

  /**
   * @param method - the method's name in messages: the class name for a
   *   constructor
   * @param forms - the method's Java forms, in Java declaration order
   * @param options - the message order, whether the bare value is a plain
   *   object, and the messages for `undefined`
   */
  constructor(method: string, forms: readonly Form[], options: JavaFormsOptions = {}) {
    const union: string[] = [];
    for (const form of forms) for (const name of form) if (!union.includes(name)) union.push(name);
    const names = options.order ?? union;
    if (names.length !== union.length || union.some((name) => !names.includes(name))) {
      throw new Error(`JavaForms(${method}): the order ${names.join(', ')} is not the forms' names ${union.join(', ')}`);
    }
    if (names.length > 30) throw new Error(`JavaForms(${method}): more than 30 parameter names`);
    this.#method = method;
    this.#bit = new Map(names.map((name, i) => [name, 1 << i]));
    const table = new Map<number, number>();
    forms.forEach((form, index) => {
      const mask = this.#mask(form);
      if (!table.has(mask)) table.set(mask, index);
    });
    this.#forms = table;
    this.#prefix = `${method}() takes one of ${forms.map((form) => `(${this.#inOrder(this.#mask(form))})`).join(', ')}; got `;
    // Java's one-parameter forms sharing one name (Map and Properties as
    // `configs`) are one bare form. One-parameter forms of different names
    // (an error's `(message)` and `(cause)`) are told apart by the value's
    // type when `bareTypes` gives a test for each; without it they take no
    // bare value.
    const single = [...new Set(forms.filter((form) => form.length === 1).map((form) => form[0]))];
    const tests = options.bareTypes;
    if (tests !== undefined && single.length > 1 && single.some((name) => tests[name] === undefined)) {
      throw new Error(`JavaForms(${method}): bareTypes has no test for every one-parameter form`);
    }
    const bareNames = tests !== undefined || single.length === 1 ? single : [];
    this.#bare = bareNames.map((name) => [name, forms.findIndex((form) => form.length === 1 && form[0] === name), tests?.[name] ?? null] as const);
    this.#bareIsPlainObject = options.bareIsPlainObject === true;
    this.#reader = options.reader === true;
    this.#manyArguments = options.manyArguments ?? null;
    if (this.#reader && (forms.length !== 1 || forms[0].length !== 0)) throw new Error(`JavaForms(${method}): a reader takes no parameter`);
    if (this.#bareIsPlainObject && this.#bare.length !== 1) throw new Error(`JavaForms(${method}): no one-parameter form`);
    this.#undefinedMessages = options.undefinedMessages ?? {};
  }

  /**
   * The index of the form a call gives, in the order the forms were listed.
   * The call passes one argument, its options object or the bare value of its
   * one-parameter form, or none. A method passes its first argument and
   * `arguments.length`, so that extra positional arguments are refused rather
   * than dropped, and `f(undefined)` is told from `f()`: `f(undefined)` is a
   * `TypeError`, as `undefined` is never a value.
   *
   * @param args - the call's first argument
   * @param count - how many arguments the call passed
   * @returns the index of the matched form
   * @throws `IllegalArgumentError` when the given names are no form's, one of
   *   them is no form's parameter, or the call passed more than one argument
   * @throws `IllegalArgumentError` when `args` is a bare value that fits no
   *   form
   * @throws `TypeError` when a parameter, or the argument, is `undefined`, or
   *   a reader is given an argument
   */
  match(args: unknown, count: number): number {
    if (count > 0 && this.#reader) throw new TypeError(`${this.#method}() only reads: it takes no argument`);
    if (count > 1) {
      throw illegalArgument(this.#manyArguments !== null ? this.#manyArguments(count) : `${this.#prefix}${count} arguments, not one object of named parameters`);
    }
    if (count === 0) return this.#matched(0, null);
    if (args === undefined) {
      // `undefined` is "not given" only as a key left out, never as a value.
      if (this.#bare.length > 0) {
        const name = this.#bare[0][0];
        throw new TypeError(this.#undefinedMessages[name] ?? `${name} must not be undefined. Leave the argument out, or pass null for Java's null`);
      }
      if (this.#forms.size === 1 && this.#forms.has(0)) {
        throw new TypeError(`${this.#method}() takes no argument: leave it out rather than pass undefined`);
      }
      throw new TypeError(`${this.#method}() takes one object of named parameters, not undefined: leave the argument out, or pass null for Java's null`);
    }
    if (!this.#isOptions(args)) {
      const bare = this.#bareOf(args);
      if (bare !== null) return bare[1];
      // A value that fits no form: Python's form error.
      throw illegalArgument(`${this.#prefix}${kind(args)}`);
    }
    let mask = 0;
    let unknown = false;
    let undefinedName: string | null = null;
    for (const key in args) {
      if (!Object.hasOwn(args, key)) continue;
      const bit = this.#bit.get(key);
      if (bit === undefined) unknown = true;
      else {
        mask |= bit;
        if (undefinedName === null && args[key] === undefined) undefinedName = key;
      }
    }
    if (!unknown && undefinedName !== null) {
      throw new TypeError(
        this.#undefinedMessages[undefinedName] ??
          `${undefinedName} must not be undefined. Leave the key out, or pass null for Java's null`,
      );
    }
    return this.#matched(mask, unknown ? args : null);
  }

  /**
   * Parameter `name` of a call `match()` accepted: the options object's own
   * enumerable key, the bare value for the one-parameter form's name, else
   * `undefined` (not given).
   *
   * @param args - the argument `match()` accepted
   * @param name - the parameter's name
   * @returns the parameter's value, or `undefined` when it is not given
   */
  value(args: unknown, name: string): unknown {
    if (args === undefined) return undefined;
    if (this.#isOptions(args)) return given(args, name) ? args[name] : undefined;
    const bare = this.#bareOf(args);
    return bare !== null && name === bare[0] ? args : undefined;
  }

  // The one-parameter form a bare value is: the first whose test accepts it.
  #bareOf(value: unknown): readonly [string, number, unknown] | null {
    for (const bare of this.#bare) if (bare[2] === null || bare[2](value)) return bare;
    return null;
  }

  // Whether `args` is the options object rather than a bare value.
  #isOptions(args: unknown): args is Record<string, unknown> {
    if (!isPlainObject(args)) return false;
    if (!this.#bareIsPlainObject) return true;
    for (const key in args) if (Object.hasOwn(args, key) && this.#bit.has(key)) return true;
    return false;
  }

  #matched(mask: number, withUnknown: Record<string, unknown> | null): number {
    const index = withUnknown === null ? this.#forms.get(mask) : undefined;
    if (index !== undefined) return index;
    let names = this.#inOrder(mask);
    if (withUnknown !== null) {
      // The names no form has, after the others, in the object's own order.
      const extra = Object.keys(withUnknown).filter((key) => !this.#bit.has(key));
      names = names === '' ? extra.join(', ') : `${names}, ${extra.join(', ')}`;
    }
    throw illegalArgument(`${this.#prefix}(${names})`);
  }
  #mask(names: Form): number {
    let mask = 0;
    for (const name of names) mask |= this.#bit.get(name) as number;
    return mask;
  }

  #inOrder(mask: number): string {
    let out = '';
    for (const [name, bit] of this.#bit) if ((mask & bit) !== 0) out = out === '' ? name : `${out}, ${name}`;
    return out;
  }
}

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

// What every generated error class shares (private): Java's constructor forms
// matched by name, the message and cause each form gives `Error`, and the FFI
// ids (python/confluent_kafka/_throwable.py's counterpart).
//
// A generated module registers its class's Java forms with `setForms`, and
// declares its FFI id in the private static `_ffiId` (the Python classes'
// `_ffi_id`; `@internal`, so the declarations leave it out). The constructor of
// the class at the top of a chain resolves the arguments of every class below
// it against the forms of `new.target` (the nearest class with forms, for a
// user's own subclass), so a subclass constructor only passes its arguments
// on. An error the core reports is built by `create`, with the core's message
// and cause, whatever forms its class has, and keeps the core's id when it
// differs from its class's, in the error's own non-enumerable `_ffiId`.

import type { JavaForms } from './args';

/**
 * An error class: what `ffiIdOf` and `setForms` take (a class whose
 * constructor is `protected` too, which no constructor type matches).
 */
interface ErrorClassLike {
  readonly name: string;
  readonly prototype: unknown;
}

/** A class or an error, as its FFI id is read: its `_ffiId`, when it has one. */
interface WithFfiId {
  readonly _ffiId?: unknown;
}

/**
 * The message a Java form gives `Throwable`: its `message` parameter, the
 * cause's `toString()` (Java's `Throwable(Throwable)`), Java's `null`, or a
 * fixed text.
 */
export type FormMessage = 'message' | 'cause.toString()' | null | { readonly text: string };

/** One Java constructor form: its parameter names, in Java order, and its message. */
export type ErrorForm = readonly [params: readonly string[], message: FormMessage];

interface Registered {
  readonly name: string;
  readonly forms: readonly ErrorForm[];
  matcher: JavaForms | null;
}

const registered = new WeakMap<ErrorClassLike, Registered>();

/** The FFI id `cls` itself declares (its own static `_ffiId`), or `undefined`. */
export function ownFfiId(cls: ErrorClassLike): number | undefined {
  return Object.hasOwn(cls, '_ffiId') ? ((cls as WithFfiId)._ffiId as number) : undefined;
}

/**
 * The FFI id of `cls`: its own or its nearest ancestor's (the static `_ffiId`
 * it inherits), `undefined` when none has one.
 */
export function ffiIdOf(cls: ErrorClassLike): number | undefined {
  const id = (cls as WithFfiId)._ffiId;
  return typeof id === 'number' ? id : undefined;
}

/**
 * The FFI id the core reported for `error` (its own `_ffiId`), when `create`
 * was given one that differs from its class's; `undefined` otherwise.
 */
export function instanceFfiId(error: object): number | undefined {
  return Object.hasOwn(error, '_ffiId') ? ((error as WithFfiId)._ffiId as number) : undefined;
}

/** Registers the Java constructor forms of a generated class, in Java declaration order. */
export function setForms(cls: ErrorClassLike, forms: readonly ErrorForm[]): void {
  registered.set(cls, { name: cls.name, forms, matcher: null });
}

/** The nearest class at or above `cls` with registered forms. */
function formsOf(cls: unknown): Registered | undefined {
  for (let c: unknown = cls; typeof c === 'function' && c !== Error; c = Object.getPrototypeOf(c)) {
    const found = registered.get(c);
    if (found !== undefined) return found;
  }
  return undefined;
}

/** The JavaScript type of a value, as an argument error names it. */
function kind(value: unknown): string {
  if (value === null) return 'null';
  if (Array.isArray(value)) return 'an array';
  return typeof value;
}

function isMessage(value: unknown): boolean {
  return value === null || typeof value === 'string';
}

function isError(value: unknown): boolean {
  return value instanceof Error;
}

function isErrorOrNull(value: unknown): boolean {
  return value === null || value instanceof Error;
}

/**
 * The matcher of `r`'s forms, built on first use (`args` is required here,
 * not at load: it reports a form error with `IllegalArgumentError`, a
 * generated class, so a static import would be a cycle).
 */
function matcherOf(r: Registered): JavaForms {
  if (r.matcher !== null) return r.matcher;
  // eslint-disable-next-line @typescript-eslint/no-require-imports -- loaded on first use: args reports with IllegalArgumentError, a class this module serves, so a static import would be a cycle
  const { JavaForms: Matcher } = require('./args') as typeof import('./args');
  const singles = new Set(r.forms.filter(([params]) => params.length === 1).map(([params]) => params[0]));
  // A bare value is a message when it is a string or null, a cause when it
  // is an Error (or null, when the class has no message on its own).
  const bareTypes: Record<string, (value: unknown) => boolean> = {};
  if (singles.has('message')) bareTypes.message = isMessage;
  if (singles.has('cause')) bareTypes.cause = singles.has('message') ? isError : isErrorOrNull;
  r.matcher = new Matcher(
    r.name,
    r.forms.map(([params]) => params),
    {
      order: ['message', 'cause'].filter((p) => r.forms.some(([params]) => params.includes(p))),
      bareTypes,
      // The ES2022 `new Error(message, { cause })` is the form check's error
      // for more than one argument (root CLAUDE.md, JavaScript Binding
      // Conventions, Errors, Q39), listing the class's forms.
    },
  );
  return r.matcher;
}

/**
 * The arguments of `Error`'s constructor for a call of `new cls(...args)`:
 * Java's message (`null` reads as `''`) and, for a form that takes a cause,
 * `{cause}`.
 *
 * @throws `IllegalArgumentError` when the given parameters are no form's
 * @throws `TypeError` when a parameter is given as `undefined` or has the
 *   wrong type, or the class has no constructor form
 */
export function errorArgs(cls: ErrorClassLike, args: readonly unknown[]): [string, { cause: unknown } | undefined] {
  const r = formsOf(cls);
  const name = cls.name;
  if (r === undefined || r.forms.length === 0) {
    throw new TypeError(`${name} cannot be constructed: its Java constructors take a payload the client does not translate yet`);
  }
  const matcher = matcherOf(r);
  const first = args[0];
  const [params, message] = r.forms[matcher.match(first, args.length)];
  const cause = params.includes('cause') ? matcher.value(first, 'cause') : undefined;
  let text: string;
  if (message === null) text = '';
  else if (message === 'message') {
    const m = matcher.value(first, 'message');
    if (m !== null && typeof m !== 'string') throw new TypeError(`message must be a string, not ${kind(m)}`);
    text = m ?? '';
  } else if (message === 'cause.toString()') {
    // Java's (Throwable cause): the message is cause.toString(), null for a null cause.
    // eslint-disable-next-line @typescript-eslint/no-base-to-string -- Java's (Throwable cause) constructor takes cause.toString(), whatever the cause is
    text = cause === null || cause === undefined ? '' : String(cause);
  } else text = message.text;
  return [text, params.includes('cause') ? { cause } : undefined];
}

/**
 * Sets `name` on a newly constructed error of class `cls` (`new.target`):
 * the class name, JavaScript's convention.
 */
export function init(self: Error, cls: ErrorClassLike): void {
  Object.defineProperty(self, 'name', { value: cls.name, enumerable: false, configurable: true, writable: true });
}

/**
 * An error of class `cls` with `message` and, when it is not `undefined`,
 * `cause`, whatever constructor forms the class has: the error the core
 * reported, or one the binding throws itself. `id` is the FFI id the core
 * reported, kept for this error when it differs from its class's.
 */
export function create<T extends Error>(cls: abstract new (...args: never[]) => T, message: string, cause?: unknown, id?: number): T {
  const error = Reflect.construct(Error, cause === undefined ? [message] : [message, { cause }], cls) as T;
  // Reflect.construct never runs `cls`'s constructor, so V8 skips every frame
  // looking for it: capture the stack again, from the caller of `create`.
  Error.captureStackTrace(error, create);
  init(error, cls);
  if (id !== undefined && id !== ffiIdOf(cls)) {
    Object.defineProperty(error, '_ffiId', { value: id, enumerable: false, configurable: true, writable: false });
  }
  return error;
}

/** The Java forms `cls` itself registered (the tests read them), or `undefined`. */
export function ownForms(cls: ErrorClassLike): readonly ErrorForm[] | undefined {
  return registered.get(cls)?.forms;
}

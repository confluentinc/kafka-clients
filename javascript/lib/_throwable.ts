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

// What every generated error class shares (private): Java's Throwable
// constructors in JavaScript's `(message?, {cause}?)` form, and the fields a
// client error carries (python/confluent_kafka/_throwable.py's counterpart).
//
// A class's FFI id is registered with `setFfiId` by its generated module (the
// Python classes' `_ffi_id`); a class without one has its nearest ancestor's,
// or none.

import { FFI_NAMES } from './_error_registry';

/** The fields `init` sets on every client error. */
interface ClientErrorFields {
  code: number;
  errorName: string | null;
  retriable: boolean;
  apiError: boolean;
}

/** An error class: what `setFfiId`, `ffiIdOf` and `init` take. */
type ErrorClassLike = abstract new (...args: never[]) => unknown;

const ids = new WeakMap<ErrorClassLike, number>();

/** Registers the `kafka_common_Error_e` value of a generated class. */
export function setFfiId(cls: ErrorClassLike, id: number): void {
  ids.set(cls, id);
}

/** The FFI id `cls` itself registered, or `undefined`. */
export function ownFfiId(cls: ErrorClassLike): number | undefined {
  return ids.get(cls);
}

/**
 * The FFI id of `cls`: its own or its nearest ancestor's, `undefined` when
 * none has one.
 */
export function ffiIdOf(cls: ErrorClassLike): number | undefined {
  for (let c: unknown = cls; typeof c === 'function' && c !== Error; c = Object.getPrototypeOf(c)) {
    if (ids.has(c as ErrorClassLike)) return ids.get(c as ErrorClassLike);
  }
  return undefined;
}

/**
 * Java's message for `(message?, {cause}?)`: the message when one is given
 * (`null` reads as `''`), else the cause's `String()` (Java's
 * `(Throwable cause)` takes `cause.toString()`), else `''`.
 */
export function messageOf(message: unknown, options: unknown): string {
  if (message !== undefined) {
    if (message === null) return '';
    if (typeof message !== 'string') throw new TypeError(`message must be a string, not ${typeof message}`);
    return message;
  }
  const cause = causeOf(options);
  // eslint-disable-next-line @typescript-eslint/no-base-to-string -- Java's (Throwable cause) constructor takes cause.toString(), whatever the cause is
  return cause === undefined || cause === null ? '' : String(cause);
}

/** The `cause` of an options argument, `undefined` when it has none. */
export function causeOf(options: unknown): unknown {
  if (options === undefined || options === null) return undefined;
  if (typeof options !== 'object') throw new TypeError(`options must be an object, not ${typeof options}`);
  return (options as { cause?: unknown }).cause;
}

/** The `Error` options for `options`: `{cause}` when it gives one. */
export function optionsOf(options: unknown): { cause: unknown } | undefined {
  const cause = causeOf(options);
  return cause === undefined ? undefined : { cause };
}

/**
 * Sets the fields of a newly constructed error of class `cls` (`new.target`):
 * `name` (the class name, JavaScript's convention), `code` and `errorName`
 * (its class's FFI id), `retriable` and `apiError` (Java's `instanceof`).
 */
export function init(self: Error & ClientErrorFields, cls: ErrorClassLike): void {
  Object.defineProperty(self, 'name', { value: cls.name, enumerable: false, configurable: true, writable: true });
  const id = ffiIdOf(cls);
  self.code = id === undefined ? -1 : id;
  self.errorName = id === undefined ? null : (FFI_NAMES.get(id) ?? null);
  const [RetriableError, ApiError] = bases();
  self.retriable = self instanceof RetriableError;
  self.apiError = self instanceof ApiError;
}

let cachedBases: [ErrorClassLike, ErrorClassLike] | null = null;

/**
 * `[RetriableError, ApiError]`: required on first use, not at load (they
 * extend the classes this module serves), and kept once both are loaded, so a
 * conversion costs no `require()`.
 */
function bases(): [ErrorClassLike, ErrorClassLike] {
  if (cachedBases !== null) return cachedBases;
  // eslint-disable-next-line @typescript-eslint/no-require-imports -- loaded on first use: RetriableError extends the classes this module serves, so a static import would be a cycle
  const { RetriableError } = require('./common/errors/retriable_error') as typeof import('./common/errors/retriable_error');
  // eslint-disable-next-line @typescript-eslint/no-require-imports -- loaded on first use: ApiError extends the classes this module serves, so a static import would be a cycle
  const { ApiError } = require('./common/errors/api_error') as typeof import('./common/errors/api_error');
  const found: [ErrorClassLike, ErrorClassLike] = [RetriableError, ApiError];
  if (typeof RetriableError === 'function' && typeof ApiError === 'function') cachedBases = found;
  return found;
}

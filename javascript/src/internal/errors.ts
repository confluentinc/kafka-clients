// Copyright 2025 Confluent Inc.
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

// Errors across the FFI (private): core -> JavaScript, as
// python/confluent_kafka/_errors.py's from_ffi_error / class_for_ffi_id.
//
// The native layer reads a kafka_common_Error_t into a plain object {code,
// message, apiError, cause} (native/error.c) and hands it to `fromNative`,
// which builds the class of the error's FFI id (an unknown id builds the base
// `KafkaError`; a bare KafkaException, reported as UNKNOWN_SERVER_ERROR
// without being an ApiException, is a `KafkaError` too) with the core's
// message and its cause chain as `cause`, whatever constructor forms the class
// has (`throwable.create`). The FFI id is private, as Python's `_ffi_id`:
// `errorFfiId` gives it to the gRPC server and the tests, the core's own id
// when it differs from the class's (an unknown id). LOCAL_NULL_POINTER, the
// core's Java `NullPointerException` for a NULL mandatory argument, has no
// class: it is JavaScript's own `TypeError`, with the core's message, as it
// is Python's `TypeError` (the binding checks its arguments first, so it is
// not expected).
//
// The id -> class table is derived from the generated registry, every class
// with its own FFI id but the base `KafkaError`, which shares
// UNKNOWN_SERVER_ERROR (-1) with `UnknownServerError`.

import { ERRORS } from './error_registry';
import { create, ffiIdOf, instanceFfiId, ownFfiId } from './throwable';
import { KafkaError } from '../common/kafka_error';
import { IllegalArgumentError } from '../illegal_argument_error';
import { IllegalStateError } from '../illegal_state_error';

/** The FFI id the core reports for a bare `KafkaException`. */
export const UNKNOWN_SERVER_ERROR = -1;

/**
 * The FFI id the core reports for a NULL mandatory argument
 * (`kafka_common_Error_LOCAL_NULL_POINTER`, Java's `NullPointerException`).
 */
export const LOCAL_NULL_POINTER = -32;

/** A generated error class. */
export type ErrorClass = abstract new (...args: never[]) => Error;

/** The plain object the native layer reads a `kafka_common_Error_t` into (native/error.c). */
interface NativeError {
  code: number;
  message?: string | null;
  apiError?: unknown;
  cause?: unknown;
}

let classes: ErrorClass[] | null = null;
let roots: ErrorClass[] | null = null;
let byId: Map<number, ErrorClass> | null = null;

/** Every generated error class, in registry order. */
export function errorClasses(): ErrorClass[] {
  if (classes === null) {
    // eslint-disable-next-line @typescript-eslint/no-require-imports -- the generated classes are loaded on first use, by the registry's module names: they import this module, so a static import would be a cycle
    classes = ERRORS.map(([module, name]) => (require(module) as Record<string, ErrorClass>)[name]);
  }
  return classes;
}

/** The generated classes at the top of a chain: `KafkaError` and the Java built-ins. */
function rootClasses(): ErrorClass[] {
  if (roots === null) {
    const all = new Set<unknown>(errorClasses());
    roots = errorClasses().filter((cls) => !all.has(Object.getPrototypeOf(cls)));
  }
  return roots;
}

function table(): Map<number, ErrorClass> {
  if (byId === null) {
    const t = new Map<number, ErrorClass>();
    for (const cls of errorClasses()) {
      const id = ownFfiId(cls);
      if (cls === KafkaError || id === undefined) continue;
      const other = t.get(id);
      if (other !== undefined) throw new Error(`duplicate FFI id ${id}: ${other.name} and ${cls.name}`);
      t.set(id, cls);
    }
    byId = t;
  }
  return byId;
}

/** The class of an FFI id; the base `KafkaError` when the id is unknown. */
export function classForFfiId(id: number): ErrorClass {
  return table().get(id) ?? KafkaError;
}

/**
 * The FFI id of an error of the client's (an instance of a generated class,
 * or of a subclass of one): the core's id for an error the core reported, its
 * class's (or nearest ancestor's) otherwise, `UNKNOWN_SERVER_ERROR` for a
 * class without one; `undefined` for any other value.
 */
export function errorFfiId(error: unknown): number | undefined {
  if (!(error instanceof Error) || !rootClasses().some((cls) => error instanceof cls)) return undefined;
  return instanceFfiId(error) ?? ffiIdOf(error.constructor) ?? UNKNOWN_SERVER_ERROR;
}

/**
 * The error for the plain object the native layer builds; anything else
 * (an `Error` already, `null`) is returned as it is.
 */
export function fromNative(fields: unknown): unknown {
  if (fields instanceof Error || fields === null || typeof fields !== 'object') return fields;
  const reported = fields as NativeError;
  const cause = reported.cause === undefined || reported.cause === null ? undefined : fromNative(reported.cause);
  if (reported.code === LOCAL_NULL_POINTER) {
    // Java's NullPointerException is JavaScript's TypeError (as Python's TypeError).
    return new TypeError(reported.message ?? '', cause === undefined ? undefined : { cause });
  }
  let cls = classForFfiId(reported.code);
  if (reported.code === UNKNOWN_SERVER_ERROR && reported.apiError !== true) cls = KafkaError;
  return create(cls, reported.message ?? '', cause, reported.code);
}

/** A binding-side check where Java throws `IllegalArgumentException`. */
export function illegalArgument(message: string): IllegalArgumentError {
  return create(IllegalArgumentError, message);
}

/** A binding-side check where Java throws `IllegalStateException`. */
export function illegalState(message: string): IllegalStateError {
  return create(IllegalStateError, message);
}

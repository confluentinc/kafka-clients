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
// name, message, retriable, apiError, cause} (src/error.c) and hands it to
// `fromNative`, which constructs the class of the error's FFI id (an unknown id
// constructs the base `KafkaError`; a bare KafkaException, reported as
// UNKNOWN_SERVER_ERROR without being an ApiException, is a `KafkaError` too),
// with the core's message, its cause chain as `cause`, and the core's `code`,
// `errorName`, `retriable` and `apiError`. LOCAL_NULL_POINTER, the core's
// Java `NullPointerException` for a NULL mandatory argument, has no class: it
// is JavaScript's own `TypeError`, with the core's message, as it is Python's
// `TypeError` (the binding checks its arguments first, so it is not expected).
//
// The id -> class table is derived from the generated registry, every class
// with its own FFI id but the base `KafkaError`, which shares
// UNKNOWN_SERVER_ERROR (-1) with `UnknownServerError`.

import { ERRORS } from './_error_registry';
import { ownFfiId } from './_throwable';
import { KafkaError } from './common/kafka_error';
import { IllegalArgumentError } from './illegal_argument_error';
import { IllegalStateError } from './illegal_state_error';

/** The FFI id the core reports for a bare `KafkaException`. */
export const UNKNOWN_SERVER_ERROR = -1;

/**
 * The FFI id the core reports for a NULL mandatory argument
 * (`kafka_common_Error_LOCAL_NULL_POINTER`, Java's `NullPointerException`).
 */
export const LOCAL_NULL_POINTER = -32;

/** A generated error class: JavaScript's `(message?, {cause}?)` constructor. */
export type ErrorClass = new (message?: string | null, options?: { cause?: unknown }) => Error;

/** The fields of a client error that `fromNative` sets from the core's report. */
interface ReportedFields {
  code: number;
  errorName: string | null;
  retriable: boolean;
  apiError: boolean;
}

/** The plain object the native layer reads a `kafka_common_Error_t` into (src/error.c). */
interface NativeError {
  code: number;
  name?: string | null;
  message?: string | null;
  retriable?: unknown;
  apiError?: unknown;
  cause?: unknown;
}

let classes: ErrorClass[] | null = null;
let byId: Map<number, ErrorClass> | null = null;

/** Every generated error class, in registry order. */
export function errorClasses(): ErrorClass[] {
  if (classes === null) classes = ERRORS.map(([module, name]) => require(module)[name] as ErrorClass);
  return classes;
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
  if (reported.code === UNKNOWN_SERVER_ERROR && !reported.apiError) cls = KafkaError;
  const error = new cls(reported.message ?? '', cause === undefined ? undefined : { cause }) as Error & ReportedFields;
  // What the core reported wins over what the class alone says.
  error.code = reported.code;
  error.errorName = reported.name === undefined ? error.errorName : reported.name;
  error.retriable = Boolean(reported.retriable);
  error.apiError = Boolean(reported.apiError);
  return error;
}

/** A binding-side check where Java throws `IllegalArgumentException`. */
export function illegalArgument(message: string): IllegalArgumentError {
  return new IllegalArgumentError(message);
}

/** A binding-side check where Java throws `IllegalStateException`. */
export function illegalState(message: string): IllegalStateError {
  return new IllegalStateError(message);
}

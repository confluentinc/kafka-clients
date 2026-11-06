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

'use strict';

// Errors across the FFI (private): core -> JavaScript, as
// python/confluent_kafka/_errors.py's from_ffi_error / class_for_ffi_id.
//
// The native layer reads a kafka_common_Error_t into a plain object {code,
// name, message, retriable, apiError, cause} (src/error.c) and hands it to
// `fromNative`, which constructs the class of the error's FFI id (an unknown id
// constructs the base `KafkaError`; a bare KafkaException, reported as
// UNKNOWN_SERVER_ERROR without being an ApiException, is a `KafkaError` too),
// with the core's message, its cause chain as `cause`, and the core's `code`,
// `errorName`, `retriable` and `apiError`.
//
// The id -> class table is derived from the generated registry, every class
// with its own FFI id but the base `KafkaError`, which shares
// UNKNOWN_SERVER_ERROR (-1) with `UnknownServerError`.

const { ERRORS } = require('./_error_registry');
const { ownFfiId } = require('./_throwable');
const { KafkaError } = require('./common/kafka_error');
const { IllegalArgumentError } = require('./illegal_argument_error');
const { IllegalStateError } = require('./illegal_state_error');

/** The FFI id the core reports for a bare `KafkaException`. */
const UNKNOWN_SERVER_ERROR = -1;

/** @type {Function[] | null} */
let classes = null;
/** @type {Map<number, Function> | null} */
let byId = null;

/**
 * Every generated error class, in registry order.
 *
 * @returns {Function[]}
 */
function errorClasses() {
  if (classes === null) classes = ERRORS.map(([module, name]) => require(module)[name]);
  return classes;
}

/** @returns {Map<number, Function>} */
function table() {
  if (byId === null) {
    const t = new Map();
    for (const cls of errorClasses()) {
      const id = ownFfiId(cls);
      if (cls === KafkaError || id === undefined) continue;
      if (t.has(id)) throw new Error(`duplicate FFI id ${id}: ${t.get(id).name} and ${cls.name}`);
      t.set(id, cls);
    }
    byId = t;
  }
  return byId;
}

/**
 * The class of an FFI id; the base `KafkaError` when the id is unknown.
 *
 * @param {number} id
 * @returns {Function}
 */
function classForFfiId(id) {
  return table().get(id) ?? KafkaError;
}

/**
 * The error for the plain object the native layer builds; anything else
 * (an `Error` already, `null`) is returned as it is.
 *
 * @param {any} fields
 * @returns {any}
 */
function fromNative(fields) {
  if (fields instanceof Error || fields === null || typeof fields !== 'object') return fields;
  let cls = /** @type {any} */ (classForFfiId(fields.code));
  if (fields.code === UNKNOWN_SERVER_ERROR && !fields.apiError) cls = KafkaError;
  const cause = fields.cause === undefined || fields.cause === null ? undefined : fromNative(fields.cause);
  const error = new cls(fields.message ?? '', cause === undefined ? undefined : { cause });
  // What the core reported wins over what the class alone says.
  error.code = fields.code;
  error.errorName = fields.name === undefined ? error.errorName : fields.name;
  error.retriable = Boolean(fields.retriable);
  error.apiError = Boolean(fields.apiError);
  return error;
}

/**
 * A binding-side check where Java throws `IllegalArgumentException`.
 *
 * @param {string} message
 * @returns {IllegalArgumentError}
 */
function illegalArgument(message) {
  return new IllegalArgumentError(message);
}

/**
 * A binding-side check where Java throws `IllegalStateException`.
 *
 * @param {string} message
 * @returns {IllegalStateError}
 */
function illegalState(message) {
  return new IllegalStateError(message);
}

module.exports = { UNKNOWN_SERVER_ERROR, classForFfiId, errorClasses, fromNative, illegalArgument, illegalState };

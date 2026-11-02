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

// KafkaError: every kafka_common_Error_t the C FFI hands out (Java's
// KafkaException family and the java.lang runtime exceptions the core
// reports beside it, flattened into one class as the core's Error enum is).

/**
 * An error from the Kafka client.
 *
 * `code` is the `kafka_common_Error_e` value identifying the error's class,
 * `name` its enumerator name (`TOPIC_AUTHORIZATION_FAILED`,
 * `LOCAL_ILLEGAL_STATE`, ...), `message` the client's text. `retriable` and
 * `apiError` answer Java's `instanceof RetriableException` /
 * `instanceof ApiException`; `cause` is the error's cause, Java's
 * `getCause()`.
 */
class KafkaError extends Error {
  /**
   * @param {{code: number, name: string | null, message: string, retriable?: boolean,
   *          apiError?: boolean, cause?: unknown}} fields
   */
  constructor({ code, name, message, retriable = false, apiError = false, cause }) {
    super(message, cause === undefined ? undefined : { cause: KafkaError._from(cause) });
    Object.defineProperty(this, 'name', { value: name || 'KafkaError', enumerable: false, configurable: true, writable: true });
    /** @type {number} */
    this.code = code;
    /** @type {boolean} */
    this.retriable = retriable;
    /** @type {boolean} */
    this.apiError = apiError;
  }

  /** A KafkaError from the plain object the native layer builds (private). */
  static _from(fields) {
    if (fields instanceof Error || fields === null || typeof fields !== 'object') return fields;
    return new KafkaError(fields);
  }
}

// `kafka_common_Error_LOCAL_ILLEGAL_ARGUMENT` / `_LOCAL_ILLEGAL_STATE`: the
// core's codes for Java's IllegalArgumentException / IllegalStateException.
const LOCAL_ILLEGAL_ARGUMENT = -3;
const LOCAL_ILLEGAL_STATE = -4;

/** A binding-side check where Java throws `IllegalArgumentException` (private). */
function illegalArgument(message) {
  return new KafkaError({ code: LOCAL_ILLEGAL_ARGUMENT, name: 'LOCAL_ILLEGAL_ARGUMENT', message });
}

/** A binding-side check where Java throws `IllegalStateException` (private). */
function illegalState(message) {
  return new KafkaError({ code: LOCAL_ILLEGAL_STATE, name: 'LOCAL_ILLEGAL_STATE', message });
}

module.exports = { KafkaError, illegalArgument, illegalState };

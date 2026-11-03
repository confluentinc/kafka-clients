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
 * An error from the Kafka client: every error the C FFI reports.
 *
 * `code` is the `kafka_common_Error_e` value identifying the error's class,
 * `name` its enumerator name (`TOPIC_AUTHORIZATION_FAILED`,
 * `LOCAL_ILLEGAL_STATE`, ...), `message` the client's text. `retriable` and
 * `apiError` answer Java's `instanceof RetriableException` /
 * `instanceof ApiException`; `cause` is the error's cause, Java's
 * `getCause()`. KafkaErrors are made by the client.
 */
class KafkaError extends Error {
  /**
   * The `kafka_common_Error_e` value identifying the error's class.
   * @readonly
   * @type {number}
   */
  code;

  /**
   * Java's `instanceof RetriableException`.
   * @readonly
   * @type {boolean}
   */
  retriable;

  /**
   * Java's `instanceof ApiException`.
   * @readonly
   * @type {boolean}
   */
  apiError;

  /**
   * Made by the client, through `_from` (private).
   *
   * @private
   * @param {{code: number, name: string | null, message: string, retriable?: boolean,
   *          apiError?: boolean, cause?: unknown}} fields
   */
  constructor({ code, name, message, retriable = false, apiError = false, cause }) {
    super(message, cause === undefined ? undefined : { cause: KafkaError._from(cause) });
    // The enumerator name of `code` (`TOPIC_AUTHORIZATION_FAILED`, `LOCAL_ILLEGAL_STATE`, ...).
    Object.defineProperty(this, 'name', { value: name || 'KafkaError', enumerable: false, configurable: true, writable: true });
    this.code = code;
    this.retriable = retriable;
    this.apiError = apiError;
    /**
     * The error's cause, Java's `getCause()`: set by `super()` (`Error`'s
     * `options.cause`), declared here for the targets whose `lib` predates
     * ES2022's `Error.cause`. Not a class field, which would overwrite it.
     * @readonly
     * @type {unknown}
     */
    this.cause;
  }

  /**
   * A KafkaError from the plain object the native layer builds (private).
   *
   * @private
   * @param {any} fields
   * @returns {any}
   */
  static _from(fields) {
    if (fields instanceof Error || fields === null || typeof fields !== 'object') return fields;
    return new KafkaError(fields);
  }
}

module.exports = { KafkaError };

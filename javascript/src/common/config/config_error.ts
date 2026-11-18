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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java source of `org.apache.kafka.common.config.ConfigException`;
// `cargo xtask check-generated` checks it is up to date.

import { javaString } from '../../internal/java_string';
import * as P from '../../internal/payload';
import { setForms } from '../../internal/throwable';
import { KafkaError } from '../kafka_error';

/**
 * Thrown if the user supplies an invalid configuration
 *
 * Java: `org.apache.kafka.common.config.ConfigException`.
 */
export class ConfigError extends KafkaError {
  /**
   * Makes `ConfigError` nominal: TypeScript checks it by name, not by shape. It
   * holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_CONFIG`, for the binding and
   * the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -10;

  /**
   * Java's `(String message)` constructor.
   *
   * @param options - the message
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; name?: never });
  /**
   * Java's `(String message)` constructor. Given the message on its own, the
   * same as `{ message }`.
   *
   * @param message - the message; `null` reads as `''`
   */
  constructor(message: string | null);
  /**
   * Java's `(String name, Object value)` constructor. The message is the one
   * Java's constructor builds from the payload.
   *
   * @param options - the `name` and the `value`
   */
  constructor(options: { /** Java's `String name`. */ name: string; /** Java's `Object value`. */ value: unknown });
  /**
   * Java's `(String name, Object value, String message)` constructor. The
   * message is the one Java's constructor builds from the payload.
   *
   * @param options - the `name` and the `value` and the message
   */
  constructor(options: { /** Java's `String name`. */ name: string; /** Java's `Object value`. */ value: unknown; /** The message; `null` reads as `''`. */ message: string | null });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(ConfigError, [
  [['message'], 'message'],
  [['name', 'value'], (v) => 'Invalid value ' + javaString(v.value) + ' for configuration ' + javaString(v.name)],
  [['name', 'value', 'message'], (v) => 'Invalid value ' + javaString(v.value) + ' for configuration ' + javaString(v.name) + (v.message === null ? '' : ': ' + javaString(v.message))],
], {
  order: ['name', 'value', 'message'],
  params: { name: P.string, value: P.object },
});

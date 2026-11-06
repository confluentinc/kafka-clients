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

// Java's org.apache.kafka.common.security.auth.SecurityProtocol, as
// python/confluent_kafka/common/security/auth/security_protocol.py translates
// it: an enum whose constants are frozen instances carrying Java's two public
// fields, `id` and `name`.

import { IllegalArgumentError } from '../../../illegal_argument_error';

const JAVA_CLASS = 'org.apache.kafka.common.security.auth.SecurityProtocol';

/** The constructor's guard: only this module makes constants. */
const TOKEN = Symbol('SecurityProtocol');

/**
 * The security protocol of a connection: `PLAINTEXT`, `SSL`,
 * `SASL_PLAINTEXT` or `SASL_SSL` (the value of `security.protocol`).
 *
 * Java: `org.apache.kafka.common.security.auth.SecurityProtocol`.
 */
export class SecurityProtocol {
  /**
   * The permanent and immutable id of a security protocol, which can't change
   * and will be used in ZK, Java's `id`.
   */
  readonly id: number;

  /**
   * Name of the security protocol, Java's `name` (its constant's name).
   */
  readonly name: string;

  /**
   * Made by this module only (the enum's constants).
   */
  private constructor(token: symbol, id: number, name: string) {
    if (token !== TOKEN) throw new TypeError('SecurityProtocol is an enum: use its constants');
    this.id = id;
    this.name = name;
    Object.freeze(this);
  }

  /**
   * Un-authenticated, non-encrypted channel.
   */
  static readonly PLAINTEXT: SecurityProtocol = new SecurityProtocol(TOKEN, 0, 'PLAINTEXT');

  /**
   * SSL channel.
   */
  static readonly SSL: SecurityProtocol = new SecurityProtocol(TOKEN, 1, 'SSL');

  /**
   * SASL authenticated, non-encrypted channel.
   */
  static readonly SASL_PLAINTEXT: SecurityProtocol = new SecurityProtocol(TOKEN, 2, 'SASL_PLAINTEXT');

  /**
   * SASL authenticated, SSL channel.
   */
  static readonly SASL_SSL: SecurityProtocol = new SecurityProtocol(TOKEN, 3, 'SASL_SSL');

  /**
   * The constants in declaration order, Java's `values()`.
   */
  static values(): SecurityProtocol[] {
    return [SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL, SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL];
  }

  /**
   * The names of the protocols, in declaration order.
   */
  static names(): string[] {
    return SecurityProtocol.values().map((p) => p.name);
  }

  /**
   * The protocol whose `id` is `id`, or `null`, as Java's map lookup returns
   * `null`.
   */
  static forId(id: number): SecurityProtocol | null {
    return SecurityProtocol.values().find((p) => p.id === id) ?? null;
  }

  /**
   * The protocol named `name` in any case, as Java's
   * `valueOf(name.toUpperCase(Locale.ROOT))`.
   *
   * @throws `IllegalArgumentError` when no protocol has that name
   */
  static forName(name: string): SecurityProtocol {
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${name === null ? 'null' : typeof name}`);
    const upper = name.toUpperCase();
    const found = SecurityProtocol.values().find((p) => p.name === upper);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${upper}`);
    return found;
  }

  /**
   * Java's `Enum.toString()`: the name.
   */
  toString(): string {
    return this.name;
  }
}

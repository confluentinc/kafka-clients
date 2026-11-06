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

'use strict';

// Java's org.apache.kafka.common.security.auth.SecurityProtocol, as
// python/confluent_kafka/common/security/auth/security_protocol.py translates
// it: an enum whose constants are frozen instances carrying Java's two public
// fields, `id` and `name`.

const { IllegalArgumentError } = require('../../../illegal_argument_error');

const JAVA_CLASS = 'org.apache.kafka.common.security.auth.SecurityProtocol';

/** The constructor's guard: only this module makes constants. */
const TOKEN = Symbol('SecurityProtocol');

/**
 * The security protocol of a connection: `PLAINTEXT`, `SSL`,
 * `SASL_PLAINTEXT` or `SASL_SSL` (the value of `security.protocol`).
 *
 * Java: `org.apache.kafka.common.security.auth.SecurityProtocol`.
 */
class SecurityProtocol {
  /**
   * The permanent and immutable id of a security protocol, which can't change
   * and will be used in ZK, Java's `id`.
   * @readonly
   * @type {number}
   */
  id;

  /**
   * Name of the security protocol, Java's `name` (its constant's name).
   * @readonly
   * @type {string}
   */
  name;

  /**
   * Made by this module only (the enum's constants).
   *
   * @private
   * @param {symbol} token
   * @param {number} id
   * @param {string} name
   */
  constructor(token, id, name) {
    if (token !== TOKEN) throw new TypeError('SecurityProtocol is an enum: use its constants');
    this.id = id;
    this.name = name;
    Object.freeze(this);
  }

  /**
   * Un-authenticated, non-encrypted channel.
   * @readonly
   * @type {SecurityProtocol}
   */
  static PLAINTEXT = new SecurityProtocol(TOKEN, 0, 'PLAINTEXT');

  /**
   * SSL channel.
   * @readonly
   * @type {SecurityProtocol}
   */
  static SSL = new SecurityProtocol(TOKEN, 1, 'SSL');

  /**
   * SASL authenticated, non-encrypted channel.
   * @readonly
   * @type {SecurityProtocol}
   */
  static SASL_PLAINTEXT = new SecurityProtocol(TOKEN, 2, 'SASL_PLAINTEXT');

  /**
   * SASL authenticated, SSL channel.
   * @readonly
   * @type {SecurityProtocol}
   */
  static SASL_SSL = new SecurityProtocol(TOKEN, 3, 'SASL_SSL');

  /**
   * The constants in declaration order, Java's `values()`.
   * @returns {SecurityProtocol[]}
   */
  static values() {
    return [SecurityProtocol.PLAINTEXT, SecurityProtocol.SSL, SecurityProtocol.SASL_PLAINTEXT, SecurityProtocol.SASL_SSL];
  }

  /**
   * The names of the protocols, in declaration order.
   * @returns {string[]}
   */
  static names() {
    return SecurityProtocol.values().map((p) => p.name);
  }

  /**
   * The protocol whose `id` is `id`, or `null`, as Java's map lookup returns
   * `null`.
   *
   * @param {number} id
   * @returns {SecurityProtocol | null}
   */
  static forId(id) {
    return SecurityProtocol.values().find((p) => p.id === id) ?? null;
  }

  /**
   * The protocol named `name` in any case, as Java's
   * `valueOf(name.toUpperCase(Locale.ROOT))`.
   *
   * @param {string} name
   * @returns {SecurityProtocol}
   * @throws {IllegalArgumentError} when no protocol has that name
   */
  static forName(name) {
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${name === null ? 'null' : typeof name}`);
    const upper = name.toUpperCase();
    const found = SecurityProtocol.values().find((p) => p.name === upper);
    if (found === undefined) throw new IllegalArgumentError(`No enum constant ${JAVA_CLASS}.${upper}`);
    return found;
  }

  /**
   * Java's `Enum.toString()`: the name.
   * @returns {string}
   */
  toString() {
    return this.name;
  }
}

module.exports = { SecurityProtocol };

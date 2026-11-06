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

// Java's org.apache.kafka.common.config.SslClientAuth, as
// python/confluent_kafka/common/config/ssl_client_auth.py translates it: an
// enum whose constants are frozen instances.

/** The constructor's guard: only this module makes constants. */
const TOKEN = Symbol('SslClientAuth');

/**
 * Describes whether the server should require or request client
 * authentication.
 *
 * The constants are `REQUIRED`, `REQUESTED` and `NONE`; `name` is Java's
 * `name()`, `toString()` Java's override (the name in lower case), `VALUES`
 * Java's `List.of(values())`, a frozen array in Java's order.
 *
 * Java: `org.apache.kafka.common.config.SslClientAuth`.
 */
export class SslClientAuth {
  /**
   * The constant's name, Java's `name()`.
   */
  readonly name: string;

  /**
   * Made by this module only (the enum's constants).
   */
  private constructor(token: symbol, name: string) {
    if (token !== TOKEN) throw new TypeError('SslClientAuth is an enum: use its constants');
    this.name = name;
    Object.freeze(this);
  }

  static readonly REQUIRED: SslClientAuth = new SslClientAuth(TOKEN, 'REQUIRED');

  static readonly REQUESTED: SslClientAuth = new SslClientAuth(TOKEN, 'REQUESTED');

  static readonly NONE: SslClientAuth = new SslClientAuth(TOKEN, 'NONE');

  /**
   * The constants in Java's order, Java's `VALUES`.
   */
  static readonly VALUES: ReadonlyArray<SslClientAuth> = Object.freeze([SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE]);

  /**
   * The constant whose name is `key` in any case; `NONE` for `null` or
   * `undefined`, and `null` when no constant has that name, as Java's
   * `forConfig` returns `null`.
   */
  static forConfig(key: string | null | undefined): SslClientAuth | null {
    if (key === null || key === undefined) return SslClientAuth.NONE;
    if (typeof key !== 'string') throw new TypeError(`key must be a string, not ${typeof key}`);
    const upperCaseKey = key.toUpperCase();
    return SslClientAuth.VALUES.find((auth) => auth.name === upperCaseKey) ?? null;
  }

  /**
   * Java's `toString()` override: the name in lower case.
   */
  toString(): string {
    return this.name.toLowerCase();
  }
}

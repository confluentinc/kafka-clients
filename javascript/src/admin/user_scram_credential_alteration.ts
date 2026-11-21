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

// Java's org.apache.kafka.clients.admin.UserScramCredentialAlteration: a
// TypeScript abstract class whose constructor throws TypeError when called
// for the abstract class itself (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java's constructor is protected, and so is
// this one. Java's protected `user` field is read through `user()`: a
// TypeScript class cannot have a field and a method of one name.

import { JavaForms, kind } from '../internal/args';

const FORMS = new JavaForms('UserScramCredentialAlteration', [['user']]);
const USER_FORMS = new JavaForms('user', [[]], { reader: true });

/**
 * A request to alter a user's SASL/SCRAM credentials.
 *
 * See KIP-554: Add Broker-side SCRAM Config API.
 *
 * @remarks An abstract class, as Java's is: build a
 * `UserScramCredentialUpsertion` or a `UserScramCredentialDeletion`.
 */
export abstract class UserScramCredentialAlteration {
  readonly #user: string;

  /**
   * Called by a subclass.
   *
   * @param options - the mandatory user
   * @throws `TypeError` when called for `UserScramCredentialAlteration`
   *   itself, which is abstract, or the user is null or not a string
   */
  protected constructor(options: {
    /** The mandatory user. */
    user: string;
  }) {
    if (new.target === UserScramCredentialAlteration) {
      throw new TypeError('UserScramCredentialAlteration cannot be constructed directly; use one of its subclasses');
    }
    FORMS.match(options, arguments.length);
    const user = FORMS.value(options, 'user');
    if (user === null) throw new TypeError('user must not be null');
    if (typeof user !== 'string') throw new TypeError(`user must be a string, not ${kind(user)}`);
    this.#user = user;
  }

  /**
   * The user.
   *
   * @returns the always non-null user
   * @throws `TypeError` when it is given an argument: it only reads
   */
  user(): string {
    if (arguments.length !== 0) USER_FORMS.match(undefined, arguments.length);
    return this.#user;
  }
}

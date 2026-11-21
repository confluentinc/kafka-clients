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

// Java's org.apache.kafka.clients.admin.UserScramCredentialDeletion
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's public
// constructor and getter. Java overrides no equals, hashCode or toString.

import { JavaForms, kind } from '../internal/args';
import { ScramMechanism } from './scram_mechanism';
import { UserScramCredentialAlteration } from './user_scram_credential_alteration';

const FORMS = new JavaForms('UserScramCredentialDeletion', [['user', 'mechanism']]);
const MECHANISM_FORMS = new JavaForms('mechanism', [[]], { reader: true });

/**
 * A request to delete a SASL/SCRAM credential for a user.
 *
 * See KIP-554: Add Broker-side SCRAM Config API.
 */
export class UserScramCredentialDeletion extends UserScramCredentialAlteration {
  readonly #mechanism: ScramMechanism;

  /**
   * A deletion of one of a user's credentials.
   *
   * @param options - the mandatory user, and the mandatory mechanism
   * @throws `TypeError` when the user or the mechanism is null or of the
   *   wrong type
   */
  constructor(options: {
    /** The mandatory user. */
    user: string;
    /** The mandatory mechanism. */
    mechanism: ScramMechanism;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    super({ user: FORMS.value(options, 'user') as string });
    const mechanism = FORMS.value(options, 'mechanism');
    if (mechanism === null) throw new TypeError('mechanism must not be null');
    if (!(mechanism instanceof ScramMechanism)) throw new TypeError(`mechanism must be a ScramMechanism, not ${kind(mechanism)}`);
    this.#mechanism = mechanism;
  }

  /**
   * The mechanism.
   *
   * @returns the always non-null mechanism
   * @throws `TypeError` when it is given an argument: it only reads
   */
  mechanism(): ScramMechanism {
    if (arguments.length !== 0) MECHANISM_FORMS.match(undefined, arguments.length);
    return this.#mechanism;
  }

  /**
   * The deletion as `JSON.stringify` writes it: `{ user, mechanism }`, the
   * mechanism by name.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { user: string; mechanism: string } {
    return { user: this.user(), mechanism: this.#mechanism.name() };
  }
}

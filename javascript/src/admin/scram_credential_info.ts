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

// Java's org.apache.kafka.clients.admin.ScramCredentialInfo, a value class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's
// public constructor, getters, and Java's equals, hashCode and toString.

import { JavaForms, kind } from '../internal/args';
import { checkInt } from '../internal/java_number';
import { listHashCode, sameClass } from '../internal/java_object';
import { ScramMechanism } from './scram_mechanism';

const FORMS = new JavaForms('ScramCredentialInfo', [['mechanism', 'iterations']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const MECHANISM_FORMS = READER('mechanism');
const ITERATIONS_FORMS = READER('iterations');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Mechanism and iterations for a SASL/SCRAM credential associated with a
 * user.
 *
 * See KIP-554: Add Broker-side SCRAM Config API.
 */
export class ScramCredentialInfo {
  readonly #mechanism: ScramMechanism;
  readonly #iterations: number;

  /**
   * A credential's mechanism and iterations.
   *
   * @param options - the required mechanism, and the number of iterations
   *   used when creating the credential (a Java `int`)
   * @throws `TypeError` when the mechanism is null or not a ScramMechanism,
   *   or the iterations not an integer
   * @throws `RangeError` when the iterations are outside Java's `int`
   */
  constructor(options: {
    /** The required mechanism. */
    mechanism: ScramMechanism;
    /** The number of iterations used when creating the credential. */
    iterations: number;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const mechanism = FORMS.value(options, 'mechanism');
    if (mechanism === null) throw new TypeError('mechanism must not be null');
    if (!(mechanism instanceof ScramMechanism)) throw new TypeError(`mechanism must be a ScramMechanism, not ${kind(mechanism)}`);
    this.#mechanism = mechanism;
    this.#iterations = checkInt(FORMS.value(options, 'iterations'), 'iterations');
  }

  /**
   * The mechanism.
   *
   * @returns the mechanism
   * @throws `TypeError` when it is given an argument: it only reads
   */
  mechanism(): ScramMechanism {
    if (arguments.length !== 0) MECHANISM_FORMS.match(undefined, arguments.length);
    return this.#mechanism;
  }

  /**
   * The number of iterations used when creating the credential.
   *
   * @returns the number of iterations
   * @throws `TypeError` when it is given an argument: it only reads
   */
  iterations(): number {
    if (arguments.length !== 0) ITERATIONS_FORMS.match(undefined, arguments.length);
    return this.#iterations;
  }

  /**
   * Whether `o` is a ScramCredentialInfo with the same mechanism and
   * iterations.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ScramCredentialInfo;
    return this.#iterations === that.#iterations && this.#mechanism === that.#mechanism;
  }

  /**
   * Java's `Objects.hash(mechanism, iterations)`, the mechanism hashed by its
   * ordinal (Java's enum hash is the object's identity).
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#mechanism.ordinal(), this.#iterations]);
  }

  /**
   * Java's `toString()`: `ScramCredentialInfo{mechanism=SCRAM_SHA_256,
   * iterations=4096}`.
   *
   * @returns the text
   */
  toString(): string {
    return `ScramCredentialInfo{mechanism=${this.#mechanism.name()}, iterations=${this.#iterations}}`;
  }

  /**
   * The credential info as `JSON.stringify` writes it: `{ mechanism,
   * iterations }`, the mechanism by name.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { mechanism: string; iterations: number } {
    return { mechanism: this.#mechanism.name(), iterations: this.#iterations };
  }
}

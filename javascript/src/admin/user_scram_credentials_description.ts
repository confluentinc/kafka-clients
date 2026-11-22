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

// Java's org.apache.kafka.clients.admin.UserScramCredentialsDescription, a
// data class (root CLAUDE.md, JavaScript Binding Conventions, Class family;
// the admin descriptions, decided by the owner on 2026-11-20): Java's getters
// are readonly fields, set once in the constructor, and the object is frozen,
// its list a frozen array (Java's `List.copyOf`). equals, hashCode and
// toString are Java's.

import { JavaForms, kind } from '../internal/args';
import { listEquals, listHashCode, sameClass, stringHashCode } from '../internal/java_object';
import { javaString } from '../internal/java_string';
import { ScramCredentialInfo } from './scram_credential_info';

const FORMS = new JavaForms('UserScramCredentialsDescription', [['name', 'credentialInfos']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * Representation of all SASL/SCRAM credentials associated with a user that
 * can be retrieved, or an error indicating why credentials could not be
 * retrieved.
 *
 * See KIP-554: Add Broker-side SCRAM Config API.
 */
export class UserScramCredentialsDescription {
  /** The user name. */
  readonly name: string;

  /** The always non-null/unmodifiable list of SASL/SCRAM credential representations for the user. */
  readonly credentialInfos: readonly ScramCredentialInfo[];

  /**
   * A user's credentials.
   *
   * @param options - the required user name, and the required SASL/SCRAM
   *   credential representations for the user
   * @throws `TypeError` when the name or the list is null, the list holds
   *   null, or a value has the wrong type
   */
  constructor(options: {
    /** The required user name. */
    name: string;
    /** The required SASL/SCRAM credential representations for the user. */
    credentialInfos: Iterable<ScramCredentialInfo>;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const name = FORMS.value(options, 'name');
    if (name === null) throw new TypeError('name must not be null');
    if (typeof name !== 'string') throw new TypeError(`name must be a string, not ${kind(name)}`);
    const infos = FORMS.value(options, 'credentialInfos');
    if (infos === null) throw new TypeError('credentialInfos must not be null');
    if (typeof infos === 'string' || typeof infos !== 'object' || !(Symbol.iterator in infos)) {
      throw new TypeError(`credentialInfos must be an iterable of ScramCredentialInfo, not ${kind(infos)}`);
    }
    const out: ScramCredentialInfo[] = [];
    for (const info of infos as Iterable<unknown>) {
      if (info === null) throw new TypeError('credentialInfos must not contain null');
      if (!(info instanceof ScramCredentialInfo))
        throw new TypeError(`credentialInfos must contain only ScramCredentialInfo, not ${kind(info)}`);
      out.push(info);
    }
    this.name = name;
    this.credentialInfos = Object.freeze(out);
    Object.freeze(this);
  }

  /**
   * Whether `o` is a UserScramCredentialsDescription with the same name and
   * equal credential infos.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as UserScramCredentialsDescription;
    return this.name === that.name && listEquals(this.credentialInfos, that.credentialInfos);
  }

  /**
   * Java's `Objects.hash(name, credentialInfos)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (Math.imul(31, (31 + stringHashCode(this.name)) | 0) + listHashCode(this.credentialInfos)) | 0;
  }

  /**
   * Java's `toString()`: `UserScramCredentialsDescription{name='alice',
   * credentialInfos=[ScramCredentialInfo{...}]}`.
   *
   * @returns the text
   */
  toString(): string {
    return `UserScramCredentialsDescription{name='${this.name}', credentialInfos=${javaString(this.credentialInfos)}}`;
  }

  /**
   * The description as JSON: its fields.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { name: string; credentialInfos: readonly ScramCredentialInfo[] } {
    return { name: this.name, credentialInfos: this.credentialInfos };
  }
}

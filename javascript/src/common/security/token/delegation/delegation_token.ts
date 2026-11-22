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

// Java's org.apache.kafka.common.security.token.delegation.DelegationToken, a
// value class (root CLAUDE.md, JavaScript Binding Conventions, Class family):
// Java's public constructor, its getters, `hmacAsBase64String`, and Java's
// equals, hashCode and toString, which hides the HMAC. Java's `byte[]` is
// taken as a `Uint8Array` and returned as a `Buffer` (Types): the HMAC is held
// as given, as Java holds the array, and `hmac()` hands out a `Buffer` over
// the same bytes.

import { JavaForms, kind } from '../../../../internal/args';
import { bytesEquals, bytesHashCode, objectHashCode, objectsEquals, sameClass } from '../../../../internal/java_object';
import { javaString } from '../../../../internal/java_string';
import { TokenInformation } from './token_information';

const FORMS = new JavaForms('DelegationToken', [['tokenInformation', 'hmac']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const TOKEN_INFO_FORMS = READER('tokenInfo');
const HMAC_FORMS = READER('hmac');
const HMAC_AS_BASE64_STRING_FORMS = READER('hmacAsBase64String');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * A class representing a delegation token.
 */
export class DelegationToken {
  readonly #tokenInformation: TokenInformation | null;
  readonly #hmac: Buffer | null;

  /**
   * A delegation token.
   *
   * @param options - the token's details and its HMAC
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** The token's details. */
    tokenInformation: TokenInformation;
    /** The token's HMAC. */
    hmac: Uint8Array;
  });
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const info = FORMS.value(options, 'tokenInformation');
    const hmac = FORMS.value(options, 'hmac');
    if (info !== null && !(info instanceof TokenInformation)) {
      throw new TypeError(`tokenInformation must be a TokenInformation or null, not ${kind(info)}`);
    }
    if (hmac !== null && !(hmac instanceof Uint8Array)) throw new TypeError(`hmac must be a Uint8Array or null, not ${kind(hmac)}`);
    this.#tokenInformation = info;
    this.#hmac = hmac === null ? null : Buffer.isBuffer(hmac) ? hmac : Buffer.from(hmac.buffer, hmac.byteOffset, hmac.byteLength);
  }

  /**
   * The token's details.
   *
   * @returns the token's details
   * @throws `TypeError` when it is given an argument: it only reads
   */
  tokenInfo(): TokenInformation {
    if (arguments.length !== 0) TOKEN_INFO_FORMS.match(undefined, arguments.length);
    return this.#tokenInformation as TokenInformation;
  }

  /**
   * The token's HMAC.
   *
   * @returns the HMAC
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hmac(): Buffer {
    if (arguments.length !== 0) HMAC_FORMS.match(undefined, arguments.length);
    return this.#hmac as Buffer;
  }

  /**
   * The token's HMAC, Base64-encoded.
   *
   * @returns the HMAC's Base64 text
   * @throws `TypeError` when it is given an argument, or the HMAC is null
   *   (Java dereferences it)
   */
  hmacAsBase64String(): string {
    if (arguments.length !== 0) HMAC_AS_BASE64_STRING_FORMS.match(undefined, arguments.length);
    if (this.#hmac === null) throw new TypeError('hmac must not be null');
    return this.#hmac.toString('base64');
  }

  /**
   * Whether `o` is a DelegationToken with equal details and the same HMAC
   * bytes.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as DelegationToken;
    return objectsEquals(this.#tokenInformation, that.#tokenInformation) && bytesEquals(this.#hmac, that.#hmac);
  }

  /**
   * Java's `hashCode()`: `31 * tokenInformation.hashCode() +
   * Arrays.hashCode(hmac)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return (Math.imul(31, objectHashCode(this.#tokenInformation)) + bytesHashCode(this.#hmac)) | 0;
  }

  /**
   * Java's `toString()`: `DelegationToken{tokenInformation=<details>,
   * hmac=[*******]}`, the HMAC hidden.
   *
   * @returns the text
   */
  toString(): string {
    return `DelegationToken{tokenInformation=${javaString(this.#tokenInformation)}, hmac=[*******]}`;
  }

  /**
   * The token as `JSON.stringify` writes it: `{ tokenInformation }`, without
   * the HMAC, which `toString()` hides too.
   *
   * @returns the token's details
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { tokenInformation: TokenInformation | null } {
    return { tokenInformation: this.#tokenInformation };
  }
}

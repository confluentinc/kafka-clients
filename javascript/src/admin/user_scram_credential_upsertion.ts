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

// Java's org.apache.kafka.clients.admin.UserScramCredentialUpsertion
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's three
// public constructors, the password a string (turned into UTF-8 bytes as
// Java's `getBytes(UTF_8)` does) or bytes, with or without a salt; without
// one, a random salt as Java's `ScramFormatter.secureRandomBytes`: the UTF-8
// text of a random 130-bit number in base 36. Java's `byte[]` is taken as a
// `Uint8Array` and returned as a `Buffer` over the same bytes (Types). Java
// overrides no equals, hashCode or toString.

import { randomBytes } from 'node:crypto';
import { JavaForms, kind } from '../internal/args';
import { encodeLikeJava } from '../internal/encode';
import { IllegalArgumentError } from '../illegal_argument_error';
import { ScramCredentialInfo } from './scram_credential_info';
import { UserScramCredentialAlteration } from './user_scram_credential_alteration';

const FORMS = new JavaForms('UserScramCredentialUpsertion', [
  ['user', 'credentialInfo', 'password'],
  ['user', 'credentialInfo', 'password', 'salt'],
]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const CREDENTIAL_INFO_FORMS = READER('credentialInfo');
const SALT_FORMS = READER('salt');
const PASSWORD_FORMS = READER('password');

/**
 * Java's `ScramFormatter.secureRandomBytes(new SecureRandom())`: `new
 * BigInteger(130, random).toString(Character.MAX_RADIX)`, as UTF-8.
 *
 * @returns the salt
 */
function generateRandomSalt(): Buffer {
  const bytes = randomBytes(17);
  bytes[0] &= 0x03;
  return encodeLikeJava(BigInt(`0x${bytes.toString('hex')}`).toString(36));
}

/**
 * A `Buffer` over the bytes of `value`, without a copy.
 *
 * @param value - the bytes
 * @returns the buffer
 */
function bufferOf(value: Uint8Array): Buffer {
  return Buffer.isBuffer(value) ? value : Buffer.from(value.buffer, value.byteOffset, value.byteLength);
}

/**
 * A request to update/insert a SASL/SCRAM credential for a user.
 *
 * See KIP-554: Add Broker-side SCRAM Config API.
 */
export class UserScramCredentialUpsertion extends UserScramCredentialAlteration {
  readonly #info: ScramCredentialInfo;
  readonly #salt: Buffer;
  readonly #password: Buffer;

  /**
   * Constructor that generates a random salt.
   *
   * @param options - the user for which the credential is to be
   *   updated/inserted, the mechanism and iterations to be used, and the
   *   password, a string (its UTF-8 bytes) or bytes
   * @throws `TypeError` when a parameter is null or of the wrong type
   * @throws `IllegalArgumentError` when the password is null: neither form
   *   without a salt takes it
   */
  constructor(options: {
    /** The user for which the credential is to be updated/inserted. */
    user: string;
    /** The mechanism and iterations to be used. */
    credentialInfo: ScramCredentialInfo;
    /** The password. */
    password: string | Uint8Array;
    salt?: never;
  });
  /**
   * Constructor that accepts an explicit salt.
   *
   * @param options - the user for which the credential is to be
   *   updated/inserted, the mechanism and iterations to be used, the password
   *   and the salt to be used
   * @throws `TypeError` when a parameter is null or of the wrong type, the
   *   password included: Java takes a salt only with a `byte[]` password
   */
  constructor(options: {
    /** The user for which the credential is to be updated/inserted. */
    user: string;
    /** The mechanism and iterations to be used. */
    credentialInfo: ScramCredentialInfo;
    /** The password: bytes (Java has no form that takes a string password and a salt). */
    password: Uint8Array;
    /** The salt to be used. */
    salt: Uint8Array;
  });
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const user = FORMS.value(options, 'user');
    const info = FORMS.value(options, 'credentialInfo');
    let password = FORMS.value(options, 'password');
    let salt = FORMS.value(options, 'salt');
    if (form === 0) {
      // Java's (String) form encodes the password first; its (byte[]) form
      // makes the salt first: either way before the null checks.
      if (password === null) {
        throw new IllegalArgumentError(
          'UserScramCredentialUpsertion() takes a string or a Uint8Array password; got null, which fits neither form',
        );
      }
      if (typeof password === 'string') password = encodeLikeJava(password);
      else if (!(password instanceof Uint8Array)) throw new TypeError(`password must be a string or a Uint8Array, not ${kind(password)}`);
      salt = generateRandomSalt();
    }
    super({ user: user as string });
    if (info === null) throw new TypeError('credentialInfo must not be null');
    if (!(info instanceof ScramCredentialInfo)) throw new TypeError(`credentialInfo must be a ScramCredentialInfo, not ${kind(info)}`);
    if (password === null) throw new TypeError('password must not be null');
    if (!(password instanceof Uint8Array)) throw new TypeError(`password must be a Uint8Array, not ${kind(password)}`);
    if (salt === null) throw new TypeError('salt must not be null');
    if (!(salt instanceof Uint8Array)) throw new TypeError(`salt must be a Uint8Array, not ${kind(salt)}`);
    this.#info = info;
    this.#password = bufferOf(password);
    this.#salt = bufferOf(salt);
  }

  /**
   * The mechanism and iterations.
   *
   * @returns them
   * @throws `TypeError` when it is given an argument: it only reads
   */
  credentialInfo(): ScramCredentialInfo {
    if (arguments.length !== 0) CREDENTIAL_INFO_FORMS.match(undefined, arguments.length);
    return this.#info;
  }

  /**
   * The salt.
   *
   * @returns it
   * @throws `TypeError` when it is given an argument: it only reads
   */
  salt(): Buffer {
    if (arguments.length !== 0) SALT_FORMS.match(undefined, arguments.length);
    return this.#salt;
  }

  /**
   * The password.
   *
   * @returns it
   * @throws `TypeError` when it is given an argument: it only reads
   */
  password(): Buffer {
    if (arguments.length !== 0) PASSWORD_FORMS.match(undefined, arguments.length);
    return this.#password;
  }

  /**
   * The upsertion as `JSON.stringify` writes it: `{ user, credentialInfo }`,
   * without the password and the salt.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { user: string; credentialInfo: ScramCredentialInfo } {
    return { user: this.user(), credentialInfo: this.#info };
  }
}

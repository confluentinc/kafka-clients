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

// Java's org.apache.kafka.common.security.token.delegation.TokenInformation,
// a value class (root CLAUDE.md, JavaScript Binding Conventions, Class
// family): Java's two public constructors, its static `fromRecord`, its
// getters, the `setExpiryTimestamp` setter, `ownerOrRenewer`, and Java's
// equals, hashCode and toString. Java stores each argument as given, a null
// included, and fails only where it reads one (`ownerAsString`); so does this
// class. The renewers are held as a copy and handed out as a new array.

import { JavaForms, kind } from '../../../../internal/args';
import { checkLong } from '../../../../internal/java_number';
import { listEquals, listHashCode, objectsEquals, sameClass } from '../../../../internal/java_object';
import { javaString } from '../../../../internal/java_string';
import { KafkaPrincipal } from '../../auth/kafka_principal';

const FORMS = new JavaForms('TokenInformation', [
  ['tokenId', 'owner', 'renewers', 'issueTimestamp', 'maxTimestamp', 'expiryTimestamp'],
  ['tokenId', 'owner', 'tokenRequester', 'renewers', 'issueTimestamp', 'maxTimestamp', 'expiryTimestamp'],
]);
const FROM_RECORD_FORMS = new JavaForms('fromRecord', [
  ['tokenId', 'owner', 'tokenRequester', 'renewers', 'issueTimestamp', 'maxTimestamp', 'expiryTimestamp'],
]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const OWNER_FORMS = READER('owner');
const OWNER_AS_STRING_FORMS = READER('ownerAsString');
const TOKEN_REQUESTER_FORMS = READER('tokenRequester');
const TOKEN_REQUESTER_AS_STRING_FORMS = READER('tokenRequesterAsString');
const RENEWERS_FORMS = READER('renewers');
const RENEWERS_AS_STRING_FORMS = READER('renewersAsString');
const ISSUE_TIMESTAMP_FORMS = READER('issueTimestamp');
const EXPIRY_TIMESTAMP_FORMS = READER('expiryTimestamp');
const MAX_TIMESTAMP_FORMS = READER('maxTimestamp');
const TOKEN_ID_FORMS = READER('tokenId');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);
const SET_EXPIRY_TIMESTAMP_FORMS = new JavaForms('setExpiryTimestamp', [['expiryTimestamp']], {
  bareTypes: { expiryTimestamp: (value) => typeof value === 'bigint' || typeof value === 'number' },
});
const OWNER_OR_RENEWER_FORMS = new JavaForms('ownerOrRenewer', [['principal']], {
  bareTypes: { principal: (value) => value === null || value instanceof KafkaPrincipal },
});

/**
 * A principal or null, as Java stores it.
 *
 * @param value - the value given
 * @param name - the parameter's name
 * @returns the principal, or null
 */
function principalOrNull(value: unknown, name: string): KafkaPrincipal | null {
  if (value !== null && !(value instanceof KafkaPrincipal))
    throw new TypeError(`${name} must be a KafkaPrincipal or null, not ${kind(value)}`);
  return value;
}

/**
 * The renewers given, copied, or null.
 *
 * @param value - the value given
 * @returns the renewers, or null
 */
function renewersOf(value: unknown): Array<KafkaPrincipal | null> | null {
  if (value === null) return null;
  if (typeof value === 'string' || typeof value !== 'object' || !(Symbol.iterator in value)) {
    throw new TypeError(`renewers must be an iterable of KafkaPrincipal, not ${kind(value)}`);
  }
  const out: Array<KafkaPrincipal | null> = [];
  for (const r of value as Iterable<unknown>) out.push(principalOrNull(r, 'a renewer'));
  return out;
}

/** The seven fields, as the constructors take them. */
interface TokenInformationFields {
  tokenId: string;
  owner: KafkaPrincipal;
  tokenRequester: KafkaPrincipal;
  renewers: Iterable<KafkaPrincipal>;
  issueTimestamp: bigint | number;
  maxTimestamp: bigint | number;
  expiryTimestamp: bigint | number;
}

/**
 * A class representing a delegation token details.
 */
export class TokenInformation {
  readonly #owner: KafkaPrincipal | null;
  readonly #tokenRequester: KafkaPrincipal | null;
  readonly #renewers: Array<KafkaPrincipal | null> | null;
  readonly #issueTimestamp: bigint;
  readonly #maxTimestamp: bigint;
  #expiryTimestamp: bigint;
  readonly #tokenId: string | null;

  /**
   * The token's details, its requester the owner.
   *
   * @param options - the token id, the owner, the renewers and the issue,
   *   max and expiry timestamps (each a `long`)
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a timestamp outside Java's `long`
   */
  constructor(options: {
    /** The token id. */
    tokenId: string;
    /** The owner. */
    owner: KafkaPrincipal;
    tokenRequester?: never;
    /** The renewers. */
    renewers: Iterable<KafkaPrincipal>;
    /** The issue timestamp, in milliseconds. */
    issueTimestamp: bigint | number;
    /** The max timestamp, in milliseconds. */
    maxTimestamp: bigint | number;
    /** The expiry timestamp, in milliseconds. */
    expiryTimestamp: bigint | number;
  });
  /**
   * The token's details.
   *
   * @param options - the token id, the owner, the token requester, the
   *   renewers and the issue, max and expiry timestamps (each a `long`)
   * @throws `TypeError` for a value of the wrong type
   * @throws `RangeError` for a timestamp outside Java's `long`
   */
  constructor(options: {
    /** The token id. */
    tokenId: string;
    /** The owner. */
    owner: KafkaPrincipal;
    /** The principal that requested the token. */
    tokenRequester: KafkaPrincipal;
    /** The renewers. */
    renewers: Iterable<KafkaPrincipal>;
    /** The issue timestamp, in milliseconds. */
    issueTimestamp: bigint | number;
    /** The max timestamp, in milliseconds. */
    maxTimestamp: bigint | number;
    /** The expiry timestamp, in milliseconds. */
    expiryTimestamp: bigint | number;
  });
  constructor(options: unknown) {
    const form = FORMS.match(options, arguments.length);
    const value = (name: string): unknown => FORMS.value(options, name);
    const tokenId = value('tokenId');
    if (tokenId !== null && typeof tokenId !== 'string') throw new TypeError(`tokenId must be a string or null, not ${kind(tokenId)}`);
    this.#tokenId = tokenId;
    this.#owner = principalOrNull(value('owner'), 'owner');
    this.#tokenRequester = form === 0 ? this.#owner : principalOrNull(value('tokenRequester'), 'tokenRequester');
    this.#renewers = renewersOf(value('renewers'));
    this.#issueTimestamp = checkLong(value('issueTimestamp'), 'issueTimestamp');
    this.#maxTimestamp = checkLong(value('maxTimestamp'), 'maxTimestamp');
    this.#expiryTimestamp = checkLong(value('expiryTimestamp'), 'expiryTimestamp');
  }

  /**
   * Convert record elements into a TokenInformation.
   *
   * @param options - the token id, the owner, the token requester, the
   *   renewers and the issue, max and expiry timestamps
   * @returns the token's details
   * @throws `TypeError` for a value of the wrong type
   */
  static fromRecord(options: TokenInformationFields): TokenInformation;
  static fromRecord(options: unknown): TokenInformation {
    FROM_RECORD_FORMS.match(options, arguments.length);
    return new TokenInformation(options as TokenInformationFields);
  }

  /**
   * The owner.
   *
   * @returns the owner
   * @throws `TypeError` when it is given an argument: it only reads
   */
  owner(): KafkaPrincipal {
    if (arguments.length !== 0) OWNER_FORMS.match(undefined, arguments.length);
    return this.#owner as KafkaPrincipal;
  }

  /**
   * The owner as its `toString()` writes it.
   *
   * @returns the owner's text
   * @throws `TypeError` when it is given an argument, or the owner is null
   *   (Java dereferences it)
   */
  ownerAsString(): string {
    if (arguments.length !== 0) OWNER_AS_STRING_FORMS.match(undefined, arguments.length);
    if (this.#owner === null) throw new TypeError('owner must not be null');
    return this.#owner.toString();
  }

  /**
   * The principal that requested the token.
   *
   * @returns the token requester
   * @throws `TypeError` when it is given an argument: it only reads
   */
  tokenRequester(): KafkaPrincipal {
    if (arguments.length !== 0) TOKEN_REQUESTER_FORMS.match(undefined, arguments.length);
    return this.#tokenRequester as KafkaPrincipal;
  }

  /**
   * The token requester as its `toString()` writes it.
   *
   * @returns the token requester's text
   * @throws `TypeError` when it is given an argument, or the token requester
   *   is null (Java dereferences it)
   */
  tokenRequesterAsString(): string {
    if (arguments.length !== 0) TOKEN_REQUESTER_AS_STRING_FORMS.match(undefined, arguments.length);
    if (this.#tokenRequester === null) throw new TypeError('tokenRequester must not be null');
    return this.#tokenRequester.toString();
  }

  /**
   * The renewers.
   *
   * @returns a new array of the renewers
   * @throws `TypeError` when it is given an argument: it only reads
   */
  renewers(): KafkaPrincipal[] {
    if (arguments.length !== 0) RENEWERS_FORMS.match(undefined, arguments.length);
    return (this.#renewers === null ? null : [...this.#renewers]) as KafkaPrincipal[];
  }

  /**
   * The renewers, each as its `toString()` writes it.
   *
   * @returns a new array of the renewers' texts
   * @throws `TypeError` when it is given an argument, or the renewers or one
   *   of them is null (Java dereferences them)
   */
  renewersAsString(): string[] {
    if (arguments.length !== 0) RENEWERS_AS_STRING_FORMS.match(undefined, arguments.length);
    if (this.#renewers === null) throw new TypeError('renewers must not be null');
    return this.#renewers.map((r) => {
      if (r === null) throw new TypeError('renewers must not contain null');
      return r.toString();
    });
  }

  /**
   * The issue timestamp.
   *
   * @returns the issue timestamp, in milliseconds
   * @throws `TypeError` when it is given an argument: it only reads
   */
  issueTimestamp(): bigint {
    if (arguments.length !== 0) ISSUE_TIMESTAMP_FORMS.match(undefined, arguments.length);
    return this.#issueTimestamp;
  }

  /**
   * The expiry timestamp.
   *
   * @returns the expiry timestamp, in milliseconds
   * @throws `TypeError` when it is given an argument: it only reads
   */
  expiryTimestamp(): bigint {
    if (arguments.length !== 0) EXPIRY_TIMESTAMP_FORMS.match(undefined, arguments.length);
    return this.#expiryTimestamp;
  }

  /**
   * Sets the expiry timestamp.
   *
   * @param expiryTimestamp - the expiry timestamp, in milliseconds, a `long`,
   *   on its own
   * @throws `TypeError` when it is not a whole number
   * @throws `RangeError` when it is outside Java's `long`
   */
  setExpiryTimestamp(expiryTimestamp: bigint | number): void;
  /**
   * The same as `setExpiryTimestamp(expiryTimestamp)`, the parameter given by
   * name.
   *
   * @param options - the expiry timestamp
   */
  setExpiryTimestamp(options: {
    /** The expiry timestamp, in milliseconds. */
    expiryTimestamp: bigint | number;
  }): void;
  setExpiryTimestamp(expiryTimestamp: unknown): void {
    SET_EXPIRY_TIMESTAMP_FORMS.match(expiryTimestamp, arguments.length);
    this.#expiryTimestamp = checkLong(SET_EXPIRY_TIMESTAMP_FORMS.value(expiryTimestamp, 'expiryTimestamp'), 'expiryTimestamp');
  }

  /**
   * The token id.
   *
   * @returns the token id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  tokenId(): string {
    if (arguments.length !== 0) TOKEN_ID_FORMS.match(undefined, arguments.length);
    return this.#tokenId as string;
  }

  /**
   * The max timestamp.
   *
   * @returns the max timestamp, in milliseconds
   * @throws `TypeError` when it is given an argument: it only reads
   */
  maxTimestamp(): bigint {
    if (arguments.length !== 0) MAX_TIMESTAMP_FORMS.match(undefined, arguments.length);
    return this.#maxTimestamp;
  }

  /**
   * Whether `principal` is the token's owner, its requester or one of its
   * renewers.
   *
   * @param principal - the principal, on its own or as `{ principal }`
   * @returns whether it is
   * @throws `TypeError` when the owner or the token requester is null, or the
   *   renewers are (Java dereferences them)
   */
  ownerOrRenewer(principal: KafkaPrincipal | null): boolean;
  /**
   * The same as `ownerOrRenewer(principal)`, the parameter given by name.
   *
   * @param options - the principal
   * @returns whether it is
   */
  ownerOrRenewer(options: {
    /** The principal. */
    principal: KafkaPrincipal | null;
  }): boolean;
  ownerOrRenewer(principal: unknown): boolean {
    OWNER_OR_RENEWER_FORMS.match(principal, arguments.length);
    const p = OWNER_OR_RENEWER_FORMS.value(principal, 'principal');
    if (this.#owner === null) throw new TypeError('owner must not be null');
    if (this.#owner.equals(p)) return true;
    if (this.#tokenRequester === null) throw new TypeError('tokenRequester must not be null');
    if (this.#tokenRequester.equals(p)) return true;
    if (this.#renewers === null) throw new TypeError('renewers must not be null');
    return this.#renewers.some((r) => objectsEquals(p, r) || (p === null && r === null));
  }

  /**
   * Whether `o` is a TokenInformation with the same owner, token requester,
   * renewers, issue and max timestamps and token id (the expiry timestamp is
   * not compared, as in Java).
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as TokenInformation;
    return (
      this.#issueTimestamp === that.#issueTimestamp &&
      this.#maxTimestamp === that.#maxTimestamp &&
      objectsEquals(this.#owner, that.#owner) &&
      objectsEquals(this.#tokenRequester, that.#tokenRequester) &&
      listEquals(this.#renewers, that.#renewers) &&
      this.#tokenId === that.#tokenId
    );
  }

  /**
   * Java's `Objects.hash(owner, tokenRequester, renewers, issueTimestamp,
   * maxTimestamp, expiryTimestamp, tokenId)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([
      this.#owner,
      this.#tokenRequester,
      this.#renewers === null ? null : listHashCode(this.#renewers),
      this.#issueTimestamp,
      this.#maxTimestamp,
      this.#expiryTimestamp,
      this.#tokenId,
    ]);
  }

  /**
   * Java's `toString()`: `TokenInformation{owner=User:a, tokenRequester=User:a,
   * renewers=[User:b], issueTimestamp=1, maxTimestamp=2, expiryTimestamp=3,
   * tokenId='id'}`.
   *
   * @returns the text
   */
  toString(): string {
    return (
      `TokenInformation{owner=${javaString(this.#owner)}, tokenRequester=${javaString(this.#tokenRequester)}` +
      `, renewers=${javaString(this.#renewers)}, issueTimestamp=${this.#issueTimestamp}, maxTimestamp=${this.#maxTimestamp}` +
      `, expiryTimestamp=${this.#expiryTimestamp}, tokenId='${javaString(this.#tokenId)}'}`
    );
  }

  /**
   * The details as `JSON.stringify` writes them, each timestamp a decimal
   * string.
   *
   * @returns the fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): Record<string, unknown> {
    return {
      tokenId: this.#tokenId,
      owner: this.#owner,
      tokenRequester: this.#tokenRequester,
      renewers: this.#renewers,
      issueTimestamp: this.#issueTimestamp.toString(),
      maxTimestamp: this.#maxTimestamp.toString(),
      expiryTimestamp: this.#expiryTimestamp.toString(),
    };
  }
}

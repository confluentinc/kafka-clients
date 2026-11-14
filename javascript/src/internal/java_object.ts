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

// Java's equals and hashCode of the values the translated classes hold
// (private), so their own `equals` and `hashCode` are Java's bodies, word for
// word (root CLAUDE.md, JavaScript Binding Conventions, Class family, Q29:
// "`equals` and `hashCode` stay methods"). Every hash is Java's `int`
// arithmetic, in 32 bits.
//
// A JavaScript value stands for the Java object it translates: a string for a
// `String`, a number for an `Integer`, a bigint for a `Long`, a boolean for a
// `Boolean`, and an object with `equals` and `hashCode` methods for a
// translated class. Any other object (a `Uint8Array` key, Java's `byte[]`)
// has `Object`'s identity equality, as a Java array does, and a hash code
// that stays the same for the object (`identityHashCode`).

/** Java's `String.hashCode()`: `s[0]*31^(n-1) + ... + s[n-1]` over the UTF-16 code units. */
export function stringHashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}

/** Java's `Long.hashCode(long)`: `(int) (value ^ (value >>> 32))`. */
export function longHashCode(value: bigint): number {
  const v = BigInt.asUintN(64, value);
  return Number(BigInt.asIntN(32, v ^ (v >> 32n)));
}

/** Java's `Arrays.hashCode(byte[])`: 0 for null, else `31 * h + b` over the signed bytes, from 1. */
export function bytesHashCode(value: Uint8Array | null): number {
  if (value === null) return 0;
  let h = 1;
  for (let i = 0; i < value.length; i++) h = (Math.imul(31, h) + ((value[i] << 24) >> 24)) | 0;
  return h;
}

/** Java's `Arrays.equals(byte[], byte[])`: the same array, both null, or the same bytes. */
export function bytesEquals(a: Uint8Array | null, b: Uint8Array | null): boolean {
  if (a === b) return true;
  if (a === null || b === null || a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i] !== b[i]) return false;
  return true;
}

/** A value with Java's `equals` and `hashCode` methods (a translated class). */
interface JavaValue {
  equals(o: unknown): boolean;
  hashCode(): number;
}

function isJavaValue(value: object): value is JavaValue {
  const v = value as Partial<JavaValue>;
  return typeof v.equals === 'function' && typeof v.hashCode === 'function';
}

/**
 * `Object.hashCode()` of an object with no `hashCode` of its own. Java leaves
 * the identity hash code unspecified but for being the same over the object's
 * life; a byte array's content hash meets that, since the client never
 * changes the bytes it is given, and keeps no table of objects.
 */
function identityHashCode(value: object): number {
  return value instanceof Uint8Array ? bytesHashCode(value) : 0;
}

/** Java's `Objects.equals(a, b)`: `a == b`, or `a != null && a.equals(b)`. */
export function objectsEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || a === undefined || typeof a !== 'object') return false;
  return isJavaValue(a) ? a.equals(b) : false;
}

/** Java's `o != null ? o.hashCode() : 0` for a value standing for a Java object. */
export function objectHashCode(value: unknown): number {
  if (value === null || value === undefined) return 0;
  switch (typeof value) {
    case 'string':
      return stringHashCode(value);
    case 'number':
      // An Integer, as every number field these classes hold is.
      return value | 0;
    case 'bigint':
      return longHashCode(value);
    case 'boolean':
      return value ? 1231 : 1237;
    case 'object':
      return isJavaValue(value) ? value.hashCode() | 0 : identityHashCode(value);
    case 'function':
      return identityHashCode(value);
    default:
      return 0;
  }
}

/** Java's `getClass() == o.getClass()`: `o` is an object of exactly `self`'s class. */
export function sameClass(self: object, o: unknown): boolean {
  return typeof o === 'object' && o !== null && Object.getPrototypeOf(o) === Object.getPrototypeOf(self);
}

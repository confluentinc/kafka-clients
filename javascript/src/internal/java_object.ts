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
// word (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family:
// "`equals` and `hashCode` stay methods"). Every hash is Java's `int`
// arithmetic, in 32 bits.
//
// A JavaScript value stands for the Java object it translates: a string for a
// `String`, a number for an `Integer`, a bigint for a `Long`, a boolean for a
// `Boolean`, and an object with `equals` and `hashCode` methods for a
// translated class. Any other object (a `Uint8Array` key, Java's `byte[]`)
// has `Object`'s identity equality, as a Java array does, and a hash code
// that stays the same whatever is done to the object (`identityHashCode`).

/**
 * Java's `String.hashCode()`: `s[0]*31^(n-1) + ... + s[n-1]` over the UTF-16 code units.
 *
 * @param s - the string
 * @returns its hash code
 */
export function stringHashCode(s: string): number {
  let h = 0;
  for (let i = 0; i < s.length; i++) h = (Math.imul(31, h) + s.charCodeAt(i)) | 0;
  return h;
}

/**
 * Java's `Long.hashCode(long)`: `(int) (value ^ (value >>> 32))`.
 *
 * @param value - the long
 * @returns its hash code
 */
export function longHashCode(value: bigint): number {
  const v = BigInt.asUintN(64, value);
  return Number(BigInt.asIntN(32, v ^ (v >> 32n)));
}

/**
 * Java's `Arrays.hashCode(byte[])`: 0 for null, else `31 * h + b` over the signed bytes, from 1.
 *
 * @param value - the bytes, or null
 * @returns their hash code
 */
export function bytesHashCode(value: Uint8Array | null): number {
  if (value === null) return 0;
  let h = 1;
  for (let i = 0; i < value.length; i++) h = (Math.imul(31, h) + ((value[i] << 24) >> 24)) | 0;
  return h;
}

/**
 * Java's `Arrays.equals(byte[], byte[])`: the same array, both null, or the same bytes.
 *
 * @param a - the first bytes, or null
 * @param b - the second bytes, or null
 * @returns whether they are equal
 */
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
 * `Object.hashCode()` of an object with no `hashCode` of its own: 0, the same
 * for every such object. Java's identity hash code is unspecified but for
 * staying the same while the object lives, whatever is done to it. Its
 * equality is identity, so one constant keeps "equal objects have equal hash
 * codes" too. A hash of a `Uint8Array`'s bytes would not stay the same: the
 * app may write to or transfer the Buffer once `send()` settled, while the
 * record still equals itself. A per-object number would need a table of
 * objects, which the binding keeps none of.
 *
 * @param value - the object
 * @returns 0
 */
function identityHashCode(value: object): number {
  void value;
  return 0;
}

/**
 * Java's `Objects.equals(a, b)`: `a == b`, or `a != null && a.equals(b)`.
 *
 * @param a - the first value
 * @param b - the second value
 * @returns whether they are equal
 */
export function objectsEquals(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (a === null || a === undefined || typeof a !== 'object') return false;
  return isJavaValue(a) ? a.equals(b) : false;
}

/**
 * Java's `o != null ? o.hashCode() : 0` for a value standing for a Java object.
 *
 * @param value - the value, or null
 * @returns its hash code
 */
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

/**
 * Java's `getClass() == o.getClass()`: `o` is an object of exactly `self`'s class.
 *
 * @param self - the object whose class is compared
 * @param o - the other value
 * @returns whether `o` has exactly `self`'s class
 */
export function sameClass(self: object, o: unknown): boolean {
  return typeof o === 'object' && o !== null && Object.getPrototypeOf(o) === Object.getPrototypeOf(self);
}

/**
 * Java's `List.equals` / `Objects.equals` of two lists (arrays, or null):
 * the same length and each element `Objects.equals` its peer, in order.
 *
 * @param a - the first list, or null
 * @param b - the second list, or null
 * @returns whether they are equal
 */
export function listEquals(a: readonly unknown[] | null, b: readonly unknown[] | null): boolean {
  if (a === b) return true;
  if (a === null || b === null || a.length !== b.length) return false;
  return a.every((x, i) => objectsEquals(x, b[i]) || (Array.isArray(x) && Array.isArray(b[i]) && listEquals(x, b[i] as unknown[])));
}

/**
 * Java's `List.hashCode()`: `31 * h + hash(e)` over the elements, from 1; 0
 * for null.
 *
 * @param list - the list, or null
 * @returns its hash code
 */
export function listHashCode(list: readonly unknown[] | null): number {
  if (list === null) return 0;
  let h = 1;
  for (const e of list) h = (Math.imul(31, h) + (Array.isArray(e) ? listHashCode(e) : objectHashCode(e))) | 0;
  return h;
}

/**
 * Java's `Set.hashCode()`: the sum of the members' hash codes; 0 for null.
 *
 * @param set - the set, or null
 * @returns its hash code
 */
export function setHashCode(set: ReadonlySet<unknown> | null): number {
  if (set === null) return 0;
  let h = 0;
  for (const e of set) h = (h + objectHashCode(e)) | 0;
  return h;
}

/**
 * Java's `Set.equals` of two sets of values with Java's `equals` (or of
 * primitives): the same size, and each member of one equal to a member of the
 * other.
 *
 * @param a - the first set, or null
 * @param b - the second set, or null
 * @returns whether they are equal
 */
export function setEquals(a: ReadonlySet<unknown> | null, b: ReadonlySet<unknown> | null): boolean {
  if (a === b) return true;
  if (a === null || b === null || a.size !== b.size) return false;
  for (const x of a) if (!b.has(x) && ![...b].some((y) => objectsEquals(x, y))) return false;
  return true;
}

/**
 * Java's `Map.equals` of two maps (or null): the same keys, each value
 * `Objects.equals` its peer.
 *
 * @param a - the first map, or null
 * @param b - the second map, or null
 * @returns whether they are equal
 */
export function mapEquals(a: ReadonlyMap<unknown, unknown> | null, b: ReadonlyMap<unknown, unknown> | null): boolean {
  if (a === b) return true;
  if (a === null || b === null || a.size !== b.size) return false;
  for (const [k, v] of a) {
    if (!b.has(k)) return false;
    const w = b.get(k);
    if (!(objectsEquals(v, w) || (Array.isArray(v) && Array.isArray(w) && listEquals(v, w)))) return false;
  }
  return true;
}

/**
 * Java's `Map.hashCode()`: the sum of `hash(key) ^ hash(value)` over the
 * entries; 0 for null.
 *
 * @param map - the map, or null
 * @returns its hash code
 */
export function mapHashCode(map: ReadonlyMap<unknown, unknown> | null): number {
  if (map === null) return 0;
  let h = 0;
  for (const [k, v] of map) h = (h + (objectHashCode(k) ^ (Array.isArray(v) ? listHashCode(v) : objectHashCode(v)))) | 0;
  return h;
}

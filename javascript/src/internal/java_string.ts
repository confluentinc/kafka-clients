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

// Java's String.valueOf of the values the translated classes hold (private),
// for the messages Java builds by concatenation ("Invalid topics: " +
// invalidTopics) and Java's collection toString() (python/confluent_kafka's
// java_str): a value stands for the Java object it translates, as in
// java_object.ts.

/**
 * Java's `String.valueOf(value)`: `null` for null, a collection (an array or
 * a `Set`) as `AbstractCollection.toString()` writes it, `[a, b]`, a `Map` as
 * `AbstractMap.toString()`, `{k=v}`, and any other value as its own
 * `toString()` (a translated class's is Java's).
 *
 * @param value - the value
 * @returns its text
 */
export function javaString(value: unknown): string {
  if (value === null || value === undefined) return 'null';
  if (typeof value === 'string') return value;
  if (Array.isArray(value) || value instanceof Set) return `[${[...(value as Iterable<unknown>)].map(javaString).join(', ')}]`;
  if (value instanceof Map) return `{${[...(value as Map<unknown, unknown>)].map(([k, v]) => `${javaString(k)}=${javaString(v)}`).join(', ')}}`;
  // eslint-disable-next-line @typescript-eslint/no-base-to-string -- Java's String.valueOf calls toString(), whatever the object is
  return String(value);
}

/**
 * Java's `Double.toString(d)` (JDK 19 and later, the shortest digits that
 * read back as `d`): a whole number keeps `.0`, and a magnitude below
 * 10^-3 or from 10^7 is written `<digits>E<exponent>`, as in `1.0E7`.
 *
 * @param d - the number
 * @returns its text
 */
export function javaDoubleString(d: number): string {
  if (Number.isNaN(d)) return 'NaN';
  if (d === Infinity) return 'Infinity';
  if (d === -Infinity) return '-Infinity';
  if (d === 0) return Object.is(d, -0) ? '-0.0' : '0.0';
  const abs = Math.abs(d);
  if (abs >= 1e-3 && abs < 1e7) {
    const s = String(d);
    return s.includes('.') ? s : `${s}.0`;
  }
  const [mantissa, exponent] = d.toExponential().split('e');
  const digits = mantissa.includes('.') ? mantissa : `${mantissa}.0`;
  return `${digits}E${exponent.startsWith('+') ? exponent.slice(1) : exponent}`;
}

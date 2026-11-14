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

// The run-time checks of a Java `int` or `long` argument that TypeScript
// cannot enforce (root CLAUDE.md, JavaScript Binding Conventions, Types and
// Implementation over the FFI): a whole number in Java's range, and for a
// `long` a bigint too, or a whole number a JavaScript number holds exactly.

const INT32_MIN = -(2 ** 31);
const INT32_MAX = 2 ** 31 - 1;
const INT64_MIN = -(2n ** 63n);
const INT64_MAX = 2n ** 63n - 1n;

/**
 * A Java `int` argument named `what`.
 *
 * @throws `TypeError` when it is not a whole number
 * @throws `RangeError` when it is beyond Java's `int`
 */
export function checkInt(value: unknown, what: string): number {
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError(`${what} must be an integer`);
  if (value < INT32_MIN || value > INT32_MAX) throw new RangeError(`${what} must be from ${INT32_MIN} to ${INT32_MAX}`);
  return value;
}

/**
 * A Java `long` argument named `what`, as a bigint.
 *
 * @throws `TypeError` when it is neither a bigint nor a whole number
 * @throws `RangeError` when it is a bigint beyond Java's `long`, or a number
 *   beyond 2^53 - 1
 */
export function checkLong(value: unknown, what: string): bigint {
  if (typeof value === 'bigint') {
    if (value < INT64_MIN || value > INT64_MAX) throw new RangeError(`${what} must be from ${INT64_MIN} to ${INT64_MAX}`);
    return value;
  }
  if (typeof value !== 'number' || !Number.isInteger(value)) throw new TypeError(`${what} must be a bigint or an integer`);
  if (Math.abs(value) > Number.MAX_SAFE_INTEGER) {
    throw new RangeError(`${what} must be a bigint beyond 2^53 - 1: a number holds no larger integer exactly`);
  }
  return BigInt(value);
}

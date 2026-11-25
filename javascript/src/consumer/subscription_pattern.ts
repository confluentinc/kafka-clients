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

// Java's SubscriptionPattern, a class with methods (javascript/CLAUDE.md,
// JavaScript Binding Conventions, Class family). The pattern is kept as
// given, a null one included, as Java keeps it: `pattern()` returns it, and
// `hashCode()` dereferences it, as Java's does.

import { JavaForms } from '../internal/args';
import { stringHashCode } from '../internal/java_object';

// Java's constructor (src/internal/args.ts): the bare value or `{pattern}`.
const FORMS = new JavaForms('SubscriptionPattern', [['pattern']]);
const PATTERN_FORMS = new JavaForms('pattern', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['obj']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

/**
 * Represents a regular expression compatible with Google RE2/J, used to
 * subscribe to topics. This just keeps the String representation of the
 * pattern, and all validations to ensure it is RE2/J compatible are delegated
 * to the broker.
 */
export class SubscriptionPattern {
  readonly #pattern: string | null;

  /**
   * Constructs a new subscription pattern.
   *
   * @param options - the pattern
   * @throws `TypeError` when the pattern is neither a string nor null (a
   *   `RegExp` included: give its `source`)
   */
  constructor(options: {
    /** The regular expression pattern compatible with RE2/J. */
    pattern: string;
  });
  /**
   * Constructs a new subscription pattern, the same as `{pattern}`.
   *
   * @param pattern - The regular expression pattern compatible with RE2/J
   */
  constructor(pattern: string);
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const pattern = FORMS.value(options, 'pattern');
    if (pattern !== null && typeof pattern !== 'string') {
      throw new TypeError(
        pattern instanceof RegExp
          ? "pattern must be a string: a SubscriptionPattern takes an RE2/J pattern as text, such as a RegExp's source"
          : 'pattern must be a string',
      );
    }
    this.#pattern = pattern;
  }

  /**
   * The pattern.
   *
   * @returns Regular expression pattern compatible with RE2/J.
   * @throws `TypeError` when it is given an argument
   */
  pattern(): string {
    PATTERN_FORMS.match(undefined, arguments.length);
    return this.#pattern as string;
  }

  /**
   * The pattern, as Java's `toString()` returns it (`'null'` for a null one).
   *
   * @returns the pattern
   */
  toString(): string {
    return String(this.#pattern);
  }

  /**
   * Java's `hashCode()`: the pattern's hash code.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument, or the pattern is null
   *   ("pattern must not be null", as Java's dereference throws)
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    if (this.#pattern === null) throw new TypeError('pattern must not be null');
    return stringHashCode(this.#pattern);
  }

  /**
   * Whether `obj` is a `SubscriptionPattern` of an equal pattern, Java's
   * `equals`.
   *
   * @param options - the object to compare with
   * @returns true when `obj` is equal
   * @throws `IllegalArgumentError` when the given parameters are not `{obj}`
   */
  equals(options: {
    /** The object to compare with. */
    obj: unknown;
  }): boolean;
  /**
   * The same as `equals({obj})`, with the object on its own; a plain object
   * is read as the options object.
   *
   * @param obj - the object to compare with
   * @returns true when `obj` is equal
   */
  equals(obj: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const obj = EQUALS_FORMS.value(options, 'obj');
    return obj instanceof SubscriptionPattern && this.#pattern === obj.#pattern;
  }

  /**
   * The pattern as `JSON.stringify` writes it: `{ pattern }`. It ignores the
   * key `JSON.stringify` passes.
   *
   * @returns the pattern
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { pattern: string | null } {
    return { pattern: this.#pattern };
  }
}

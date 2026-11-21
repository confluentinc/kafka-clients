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

// Java's org.apache.kafka.common.acl.AclBinding, a value class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's public
// constructor, getters, toFilter, equals, hashCode and toString.

import { JavaForms } from '../../internal/args';
import { listHashCode, sameClass } from '../../internal/java_object';
import { ResourcePattern } from '../resource/resource_pattern';
import { AccessControlEntry } from './access_control_entry';
import { AclBindingFilter } from './acl_binding_filter';

const FORMS = new JavaForms('AclBinding', [['pattern', 'entry']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const IS_UNKNOWN_FORMS = READER('isUnknown');
const PATTERN_FORMS = READER('pattern');
const ENTRY_FORMS = READER('entry');
const TO_FILTER_FORMS = READER('toFilter');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Represents a binding between a resource pattern and an access control
 * entry.
 */
export class AclBinding {
  readonly #pattern: ResourcePattern;
  readonly #entry: AccessControlEntry;

  /**
   * Create an instance of this class with the provided parameters.
   *
   * @param options - the non-null resource pattern and the non-null entry
   * @throws `TypeError` when a parameter is null or of the wrong type
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{pattern, entry}`
   */
  constructor(options: {
    /** The resource pattern. */
    pattern: ResourcePattern;
    /** The access control entry. */
    entry: AccessControlEntry;
  }) {
    FORMS.match(options, arguments.length);
    const pattern = FORMS.value(options, 'pattern');
    const entry = FORMS.value(options, 'entry');
    if (pattern === null) throw new TypeError('pattern must not be null');
    if (!(pattern instanceof ResourcePattern)) throw new TypeError('pattern must be a ResourcePattern');
    if (entry === null) throw new TypeError('entry must not be null');
    if (!(entry instanceof AccessControlEntry)) throw new TypeError('entry must be an AccessControlEntry');
    this.#pattern = pattern;
    this.#entry = entry;
  }

  /**
   * Whether this binding has any UNKNOWN components.
   *
   * @returns true if this binding has any UNKNOWN components
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    if (arguments.length !== 0) IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this.#pattern.isUnknown() || this.#entry.isUnknown();
  }

  /**
   * The resource pattern for this binding.
   *
   * @returns the resource pattern for this binding
   * @throws `TypeError` when it is given an argument: it only reads
   */
  pattern(): ResourcePattern {
    if (arguments.length !== 0) PATTERN_FORMS.match(undefined, arguments.length);
    return this.#pattern;
  }

  /**
   * The access control entry for this binding.
   *
   * @returns the access control entry for this binding
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entry(): AccessControlEntry {
    if (arguments.length !== 0) ENTRY_FORMS.match(undefined, arguments.length);
    return this.#entry;
  }

  /**
   * Create a filter which matches only this AclBinding.
   *
   * @returns the filter
   * @throws `TypeError` when it is given an argument: it only reads
   */
  toFilter(): AclBindingFilter {
    if (arguments.length !== 0) TO_FILTER_FORMS.match(undefined, arguments.length);
    return new AclBindingFilter({ patternFilter: this.#pattern.toFilter(), entryFilter: this.#entry.toFilter() });
  }

  /**
   * Whether `o` is an AclBinding with an equal pattern and entry.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as AclBinding;
    return this.#pattern.equals(that.#pattern) && this.#entry.equals(that.#entry);
  }

  /**
   * Java's `hashCode()`: `Objects.hash(pattern, entry)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#pattern, this.#entry]);
  }

  /**
   * Java's `toString()`: `(pattern=<pattern>, entry=<entry>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(pattern=${this.#pattern.toString()}, entry=${this.#entry.toString()})`;
  }

  /**
   * The binding as `JSON.stringify` writes it: `{ pattern, entry }`, each
   * as its own `toJSON()` writes it.
   *
   * @returns the binding's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { pattern: ReturnType<ResourcePattern['toJSON']>; entry: ReturnType<AccessControlEntry['toJSON']> } {
    return { pattern: this.#pattern.toJSON(), entry: this.#entry.toJSON() };
  }

  /**
   * The key `AclBindingMap` stores the binding under: the fields `equals`
   * compares. Called by the map.
   *
   * @returns the key
   * @internal
   */
  internalKey(): string {
    const p = this.#pattern;
    const e = this.#entry;
    return JSON.stringify([
      p.resourceType().name(),
      p.name(),
      p.patternType().name(),
      e.principal(),
      e.host(),
      e.operation().name(),
      e.permissionType().name(),
    ]);
  }
}

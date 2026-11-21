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

// Java's org.apache.kafka.common.acl.AclBindingFilter, a value class (root
// CLAUDE.md, JavaScript Binding Conventions, Class family): Java's public
// constructor, ANY, getters, matches, equals, hashCode and toString.

import { JavaForms } from '../../internal/args';
import { listHashCode, sameClass } from '../../internal/java_object';
import { ResourcePatternFilter } from '../resource/resource_pattern_filter';
import { AccessControlEntryFilter } from './access_control_entry_filter';
import { AclBinding } from './acl_binding';

const FORMS = new JavaForms('AclBindingFilter', [['patternFilter', 'entryFilter']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const IS_UNKNOWN_FORMS = READER('isUnknown');
const PATTERN_FILTER_FORMS = READER('patternFilter');
const ENTRY_FILTER_FORMS = READER('entryFilter');
const MATCHES_AT_MOST_ONE_FORMS = READER('matchesAtMostOne');
const FIND_INDEFINITE_FIELD_FORMS = READER('findIndefiniteField');
const HASH_CODE_FORMS = READER('hashCode');
const MATCHES_FORMS = new JavaForms('matches', [['binding']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * A filter which can match AclBinding objects.
 */
export class AclBindingFilter {
  readonly #patternFilter: ResourcePatternFilter;
  readonly #entryFilter: AccessControlEntryFilter;

  /**
   * Create an instance of this filter with the provided parameters.
   *
   * @param options - the non-null pattern filter and the non-null access
   *   control entry filter
   * @throws `TypeError` when a parameter is null or of the wrong type
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{patternFilter, entryFilter}`
   */
  constructor(options: {
    /** The pattern filter. */
    patternFilter: ResourcePatternFilter;
    /** The access control entry filter. */
    entryFilter: AccessControlEntryFilter;
  }) {
    FORMS.match(options, arguments.length);
    const patternFilter = FORMS.value(options, 'patternFilter');
    const entryFilter = FORMS.value(options, 'entryFilter');
    if (patternFilter === null) throw new TypeError('patternFilter must not be null');
    if (!(patternFilter instanceof ResourcePatternFilter)) throw new TypeError('patternFilter must be a ResourcePatternFilter');
    if (entryFilter === null) throw new TypeError('entryFilter must not be null');
    if (!(entryFilter instanceof AccessControlEntryFilter)) throw new TypeError('entryFilter must be an AccessControlEntryFilter');
    this.#patternFilter = patternFilter;
    this.#entryFilter = entryFilter;
  }

  /** A filter which matches any ACL binding. */
  static readonly ANY: AclBindingFilter = new AclBindingFilter({
    patternFilter: ResourcePatternFilter.ANY,
    entryFilter: AccessControlEntryFilter.ANY,
  });

  /**
   * Whether this filter has any UNKNOWN components.
   *
   * @returns `true` if this filter has any UNKNOWN components
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    if (arguments.length !== 0) IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this.#patternFilter.isUnknown() || this.#entryFilter.isUnknown();
  }

  /**
   * The resource pattern filter.
   *
   * @returns the resource pattern filter
   * @throws `TypeError` when it is given an argument: it only reads
   */
  patternFilter(): ResourcePatternFilter {
    if (arguments.length !== 0) PATTERN_FILTER_FORMS.match(undefined, arguments.length);
    return this.#patternFilter;
  }

  /**
   * The access control entry filter.
   *
   * @returns the access control entry filter
   * @throws `TypeError` when it is given an argument: it only reads
   */
  entryFilter(): AccessControlEntryFilter {
    if (arguments.length !== 0) ENTRY_FILTER_FORMS.match(undefined, arguments.length);
    return this.#entryFilter;
  }

  /**
   * Return true if the resource and entry filters can only match one ACE. In
   * other words, if there are no ANY or UNKNOWN fields.
   *
   * @returns whether they can only match one ACE
   * @throws `TypeError` when it is given an argument: it only reads
   */
  matchesAtMostOne(): boolean {
    if (arguments.length !== 0) MATCHES_AT_MOST_ONE_FORMS.match(undefined, arguments.length);
    return this.#patternFilter.matchesAtMostOne() && this.#entryFilter.matchesAtMostOne();
  }

  /**
   * Return a string describing an ANY or UNKNOWN field, or null if there is
   * no such field.
   *
   * @returns the description, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  findIndefiniteField(): string | null {
    if (arguments.length !== 0) FIND_INDEFINITE_FIELD_FORMS.match(undefined, arguments.length);
    return this.#patternFilter.findIndefiniteField() ?? this.#entryFilter.findIndefiniteField();
  }

  /**
   * Return true if the resource filter matches the binding's resource and the
   * entry filter matches binding's entry.
   *
   * @param binding - the binding, on its own or as `{ binding }`
   * @returns whether it matches
   * @throws `TypeError` when the binding is null or not an AclBinding
   * @throws `IllegalArgumentError` from `ResourcePatternFilter.matches`
   */
  matches(binding: AclBinding): boolean;
  /**
   * The same as `matches(binding)`, the parameter given by name.
   *
   * @param options - the binding
   * @returns whether it matches
   */
  matches(options: {
    /** The binding. */
    binding: AclBinding;
  }): boolean;
  /**
   * Return true if the resource filter matches the binding's resource and the
   * entry filter matches binding's entry.
   *
   * @param binding - the binding, on its own or as `{ binding }`
   * @returns whether it matches
   */
  matches(binding: unknown): boolean {
    MATCHES_FORMS.match(binding, arguments.length);
    const given = MATCHES_FORMS.value(binding, 'binding');
    if (given === null) throw new TypeError('binding must not be null');
    if (!(given instanceof AclBinding)) throw new TypeError('binding must be an AclBinding');
    return this.#patternFilter.matches(given.pattern()) && this.#entryFilter.matches(given.entry());
  }

  /**
   * Whether `o` is an AclBindingFilter with an equal pattern filter and
   * entry filter.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as AclBindingFilter;
    return this.#patternFilter.equals(that.#patternFilter) && this.#entryFilter.equals(that.#entryFilter);
  }

  /**
   * Java's `hashCode()`: `Objects.hash(patternFilter, entryFilter)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#patternFilter, this.#entryFilter]);
  }

  /**
   * Java's `toString()`: `(patternFilter=<filter>, entryFilter=<filter>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(patternFilter=${this.#patternFilter.toString()}, entryFilter=${this.#entryFilter.toString()})`;
  }

  /**
   * The filter as `JSON.stringify` writes it: `{ patternFilter, entryFilter
   * }`, each as its own `toJSON()` writes it.
   *
   * @returns the filter's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): {
    patternFilter: ReturnType<ResourcePatternFilter['toJSON']>;
    entryFilter: ReturnType<AccessControlEntryFilter['toJSON']>;
  } {
    return { patternFilter: this.#patternFilter.toJSON(), entryFilter: this.#entryFilter.toJSON() };
  }

  /**
   * The key `AclBindingFilterMap` stores the filter under: the fields
   * `equals` compares. Called by the map.
   *
   * @returns the key
   * @internal
   */
  internalKey(): string {
    const p = this.#patternFilter;
    const e = this.#entryFilter;
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

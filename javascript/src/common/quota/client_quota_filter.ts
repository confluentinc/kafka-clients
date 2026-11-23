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

// Java's org.apache.kafka.common.quota.ClientQuotaFilter, a value class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's constructor
// is private, so instances come from its three static factories; its getters
// and Java's equals, hashCode and toString. Java stores the collection it is
// given, a null included; this class holds a copy and hands out a new array.

import { JavaForms, kind } from '../../internal/args';
import { listEquals, listHashCode, sameClass } from '../../internal/java_object';
import { javaString } from '../../internal/java_string';
import { ClientQuotaFilterComponent } from './client_quota_filter_component';

const isComponents = (value: unknown): boolean =>
  value === null || typeof value === 'string' || (typeof value === 'object' && Symbol.iterator in value);
const CONTAINS_FORMS = new JavaForms('contains', [['components']], { bareTypes: { components: isComponents } });
const CONTAINS_ONLY_FORMS = new JavaForms('containsOnly', [['components']], { bareTypes: { components: isComponents } });
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const ALL_FORMS = READER('all');
const COMPONENTS_FORMS = READER('components');
const STRICT_FORMS = READER('strict');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * The components given, copied, or null.
 *
 * @param value - the value given
 * @returns the components, or null
 */
function componentsOf(value: unknown): Array<ClientQuotaFilterComponent | null> | null {
  if (value === null) return null;
  if (typeof value === 'string' || typeof value !== 'object' || !(Symbol.iterator in value)) {
    throw new TypeError(`components must be an iterable of ClientQuotaFilterComponent, not ${kind(value)}`);
  }
  const out: Array<ClientQuotaFilterComponent | null> = [];
  for (const c of value as Iterable<unknown>) {
    if (c !== null && !(c instanceof ClientQuotaFilterComponent)) {
      throw new TypeError(`components must contain only ClientQuotaFilterComponent, not ${kind(c)}`);
    }
    out.push(c);
  }
  return out;
}

/**
 * Describes a client quota entity filter.
 */
export class ClientQuotaFilter {
  readonly #components: Array<ClientQuotaFilterComponent | null> | null;
  readonly #strict: boolean;

  /**
   * A filter to be applied to matching client quotas.
   *
   * @param components - the components to filter on
   * @param strict - whether the filter only includes specified components
   */
  private constructor(components: Array<ClientQuotaFilterComponent | null> | null, strict: boolean) {
    this.#components = components;
    this.#strict = strict;
  }

  /**
   * Constructs and returns a quota filter that matches all provided
   * components. Matching entities with entity types that are not specified by
   * a component will also be included in the result.
   *
   * @param components - the components for the filter, on its own or as
   *   `{ components }`
   * @returns the filter
   * @throws `TypeError` when it is not an iterable of components
   */
  static contains(components: Iterable<ClientQuotaFilterComponent>): ClientQuotaFilter;
  /**
   * The same as `contains(components)`, the parameter given by name.
   *
   * @param options - the components for the filter
   * @returns the filter
   */
  static contains(options: {
    /** The components for the filter. */
    components: Iterable<ClientQuotaFilterComponent>;
  }): ClientQuotaFilter;
  static contains(components: unknown): ClientQuotaFilter {
    CONTAINS_FORMS.match(components, arguments.length);
    return new ClientQuotaFilter(componentsOf(CONTAINS_FORMS.value(components, 'components')), false);
  }

  /**
   * Constructs and returns a quota filter that matches all provided
   * components. Matching entities with entity types that are not specified by
   * a component will *not* be included in the result.
   *
   * @param components - the components for the filter, on its own or as
   *   `{ components }`
   * @returns the filter
   * @throws `TypeError` when it is not an iterable of components
   */
  static containsOnly(components: Iterable<ClientQuotaFilterComponent>): ClientQuotaFilter;
  /**
   * The same as `containsOnly(components)`, the parameter given by name.
   *
   * @param options - the components for the filter
   * @returns the filter
   */
  static containsOnly(options: {
    /** The components for the filter. */
    components: Iterable<ClientQuotaFilterComponent>;
  }): ClientQuotaFilter;
  static containsOnly(components: unknown): ClientQuotaFilter {
    CONTAINS_ONLY_FORMS.match(components, arguments.length);
    return new ClientQuotaFilter(componentsOf(CONTAINS_ONLY_FORMS.value(components, 'components')), true);
  }

  /**
   * Constructs and returns a quota filter that matches all configured
   * entities.
   *
   * @returns the filter
   * @throws `TypeError` when it is given an argument
   */
  static all(): ClientQuotaFilter {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return new ClientQuotaFilter([], false);
  }

  /**
   * @returns the filter's components
   * @throws `TypeError` when it is given an argument: it only reads
   */
  components(): ClientQuotaFilterComponent[] {
    if (arguments.length !== 0) COMPONENTS_FORMS.match(undefined, arguments.length);
    return (this.#components === null ? null : [...this.#components]) as ClientQuotaFilterComponent[];
  }

  /**
   * @returns whether the filter is strict, i.e. only includes specified components
   * @throws `TypeError` when it is given an argument: it only reads
   */
  strict(): boolean {
    if (arguments.length !== 0) STRICT_FORMS.match(undefined, arguments.length);
    return this.#strict;
  }

  /**
   * Whether `o` is a ClientQuotaFilter with equal components and the same
   * strictness.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ClientQuotaFilter;
    return listEquals(this.#components, that.#components) && this.#strict === that.#strict;
  }

  /**
   * Java's `Objects.hash(components, strict)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#components, this.#strict]);
  }

  /**
   * Java's `toString()`: `ClientQuotaFilter(components=[...], strict=false)`.
   *
   * @returns the text
   */
  toString(): string {
    return `ClientQuotaFilter(components=${javaString(this.#components)}, strict=${String(this.#strict)})`;
  }

  /**
   * The filter as `JSON.stringify` writes it: `{ components, strict }`.
   *
   * @returns the filter's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { components: Array<ClientQuotaFilterComponent | null> | null; strict: boolean } {
    return { components: this.#components === null ? null : [...this.#components], strict: this.#strict };
  }
}

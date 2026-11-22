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

// Java's org.apache.kafka.common.resource.ResourcePatternFilter, a value
// class (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family):
// Java's public constructor, ANY, getters, matches, equals, hashCode and
// toString.

import { JavaForms } from '../../internal/args';
import { listHashCode, sameClass } from '../../internal/java_object';
import { IllegalArgumentError } from '../../illegal_argument_error';
import { PatternType } from './pattern_type';
import { ResourcePattern } from './resource_pattern';
import { ResourceType } from './resource_type';

const FORMS = new JavaForms('ResourcePatternFilter', [['resourceType', 'name', 'patternType']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const RESOURCE_TYPE_FORMS = READER('resourceType');
const NAME_FORMS = READER('name');
const PATTERN_TYPE_FORMS = READER('patternType');
const IS_UNKNOWN_FORMS = READER('isUnknown');
const MATCHES_AT_MOST_ONE_FORMS = READER('matchesAtMostOne');
const FIND_INDEFINITE_FIELD_FORMS = READER('findIndefiniteField');
const HASH_CODE_FORMS = READER('hashCode');
const MATCHES_FORMS = new JavaForms('matches', [['pattern']]);
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Represents a filter that can match `ResourcePattern`.
 */
export class ResourcePatternFilter {
  readonly #resourceType: ResourceType;
  readonly #name: string | null;
  readonly #patternType: PatternType;

  /**
   * Create a filter using the supplied parameters.
   *
   * @param options - the resource type (non-null: `ANY` ignores the
   *   pattern's type, any other matches only patterns of that type), the
   *   resource name or null (null ignores the name of resources,
   *   `ResourcePattern.WILDCARD_RESOURCE` matches only wildcard patterns),
   *   and the pattern type (non-null: `ANY` matches patterns regardless of
   *   pattern type, `MATCH` matches patterns that would match the supplied
   *   name, including a matching prefixed and wildcards patterns, and any
   *   other matches only patterns of that type)
   * @throws `TypeError` when the resource type or the pattern type is null,
   *   or a parameter is of the wrong type
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{resourceType, name, patternType}`
   */
  constructor(options: {
    /** The resource type: `ANY` ignores the pattern's type. */
    resourceType: ResourceType;
    /** The resource name, or null to ignore the name of resources. */
    name: string | null;
    /** The pattern type: `ANY` matches any, `MATCH` performs pattern matching. */
    patternType: PatternType;
  }) {
    FORMS.match(options, arguments.length);
    const resourceType = FORMS.value(options, 'resourceType');
    const name = FORMS.value(options, 'name');
    const patternType = FORMS.value(options, 'patternType');
    if (resourceType === null) throw new TypeError('resourceType must not be null');
    if (!(resourceType instanceof ResourceType)) throw new TypeError('resourceType must be a ResourceType');
    if (name !== null && typeof name !== 'string') throw new TypeError('name must be a string or null');
    if (patternType === null) throw new TypeError('patternType must not be null');
    if (!(patternType instanceof PatternType)) throw new TypeError('patternType must be a PatternType');
    this.#resourceType = resourceType;
    this.#name = name;
    this.#patternType = patternType;
  }

  /** Matches any resource pattern. */
  static readonly ANY: ResourcePatternFilter = new ResourcePatternFilter({
    resourceType: ResourceType.ANY,
    name: null,
    patternType: PatternType.ANY,
  });

  /**
   * @returns `true` if this filter has any UNKNOWN components.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    if (arguments.length !== 0) IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this.#resourceType.isUnknown() || this.#patternType.isUnknown();
  }

  /**
   * @returns the specific resource type this pattern matches
   * @throws `TypeError` when it is given an argument: it only reads
   */
  resourceType(): ResourceType {
    if (arguments.length !== 0) RESOURCE_TYPE_FORMS.match(undefined, arguments.length);
    return this.#resourceType;
  }

  /**
   * @returns the resource name.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string | null {
    if (arguments.length !== 0) NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * @returns the resource pattern type.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  patternType(): PatternType {
    if (arguments.length !== 0) PATTERN_TYPE_FORMS.match(undefined, arguments.length);
    return this.#patternType;
  }

  /**
   * @param pattern - the pattern, on its own or as `{ pattern }`
   * @returns `true` if this filter matches the given pattern.
   * @throws `TypeError` when the pattern is null where Java reads it, or not
   *   a ResourcePattern
   * @throws `IllegalArgumentError` "Unsupported PatternType: <type>" when the
   *   filter's `MATCH` meets a pattern that is neither literal nor prefixed
   */
  matches(pattern: ResourcePattern): boolean;
  /**
   * @param options - the pattern
   * @returns `true` if this filter matches the given pattern.
   */
  matches(options: {
    /** The pattern. */
    pattern: ResourcePattern;
  }): boolean;
  /**
   * @param pattern - the pattern, on its own or as `{ pattern }`
   * @returns `true` if this filter matches the given pattern.
   */
  matches(pattern: unknown): boolean {
    MATCHES_FORMS.match(pattern, arguments.length);
    const given = MATCHES_FORMS.value(pattern, 'pattern');
    // Java dereferences the pattern only where it reads it.
    const p = (): ResourcePattern => {
      if (given === null) throw new TypeError('pattern must not be null');
      if (!(given instanceof ResourcePattern)) throw new TypeError('pattern must be a ResourcePattern');
      return given;
    };
    if (this.#resourceType !== ResourceType.ANY && this.#resourceType !== p().resourceType()) return false;
    if (this.#patternType !== PatternType.ANY && this.#patternType !== PatternType.MATCH && this.#patternType !== p().patternType()) {
      return false;
    }
    if (this.#name === null) return true;
    if (this.#patternType === PatternType.ANY || this.#patternType === p().patternType()) return this.#name === p().name();
    switch (p().patternType()) {
      case PatternType.LITERAL:
        return this.#name === p().name() || p().name() === '*';
      case PatternType.PREFIXED:
        return this.#name.startsWith(p().name());
      default:
        throw new IllegalArgumentError(`Unsupported PatternType: ${p().patternType().name()}`);
    }
  }

  /**
   * Whether this filter could only match one pattern: in other words, if
   * there are no ANY or UNKNOWN fields.
   *
   * @returns `true` if this filter could only match one pattern
   * @throws `TypeError` when it is given an argument: it only reads
   */
  matchesAtMostOne(): boolean {
    if (arguments.length !== 0) MATCHES_AT_MOST_ONE_FORMS.match(undefined, arguments.length);
    return this.findIndefiniteField() === null;
  }

  /**
   * A string describing any ANY or UNKNOWN field, or null if there is no
   * such field.
   *
   * @returns the description, or null
   * @throws `TypeError` when it is given an argument: it only reads
   */
  findIndefiniteField(): string | null {
    if (arguments.length !== 0) FIND_INDEFINITE_FIELD_FORMS.match(undefined, arguments.length);
    if (this.#resourceType === ResourceType.ANY) return 'Resource type is ANY.';
    if (this.#resourceType === ResourceType.UNKNOWN) return 'Resource type is UNKNOWN.';
    if (this.#name === null) return 'Resource name is NULL.';
    if (this.#patternType === PatternType.MATCH) return 'Resource pattern type is MATCH.';
    if (this.#patternType === PatternType.UNKNOWN) return 'Resource pattern type is UNKNOWN.';
    return null;
  }

  /**
   * Whether `o` is a ResourcePatternFilter of the same type, name and
   * pattern type.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ResourcePatternFilter;
    return this.#resourceType === that.#resourceType && this.#name === that.#name && this.#patternType === that.#patternType;
  }

  /**
   * Java's `hashCode()`: `Objects.hash(resourceType, name, patternType)`.
   *
   * @returns the hash code
   * @throws `TypeError` when it is given an argument: it only reads
   */
  hashCode(): number {
    if (arguments.length !== 0) HASH_CODE_FORMS.match(undefined, arguments.length);
    return listHashCode([this.#resourceType, this.#name, this.#patternType]);
  }

  /**
   * Java's `toString()`: `ResourcePattern(resourceType=<type>, name=<name or
   * <any>>, patternType=<type>)`.
   *
   * @returns the text
   */
  toString(): string {
    const name = this.#name === null ? '<any>' : this.#name;
    return `ResourcePattern(resourceType=${this.#resourceType.name()}, name=${name}, patternType=${this.#patternType.name()})`;
  }

  /**
   * The filter as `JSON.stringify` writes it: `{ resourceType, name,
   * patternType }`, the enums by name, the name null for any.
   *
   * @returns the filter's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { resourceType: string; name: string | null; patternType: string } {
    return { resourceType: this.#resourceType.name(), name: this.#name, patternType: this.#patternType.name() };
  }
}

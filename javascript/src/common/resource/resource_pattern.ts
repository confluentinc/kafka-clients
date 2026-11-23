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

// Java's org.apache.kafka.common.resource.ResourcePattern, a value class
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): Java's
// public constructor, getters, equals, hashCode and toString.

import { JavaForms } from '../../internal/args';
import { listHashCode, sameClass } from '../../internal/java_object';
import { IllegalArgumentError } from '../../illegal_argument_error';
import { PatternType } from './pattern_type';
import { ResourcePatternFilter } from './resource_pattern_filter';
import { ResourceType } from './resource_type';

const FORMS = new JavaForms('ResourcePattern', [['resourceType', 'name', 'patternType']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const RESOURCE_TYPE_FORMS = READER('resourceType');
const NAME_FORMS = READER('name');
const PATTERN_TYPE_FORMS = READER('patternType');
const TO_FILTER_FORMS = READER('toFilter');
const IS_UNKNOWN_FORMS = READER('isUnknown');
const HASH_CODE_FORMS = READER('hashCode');
const EQUALS_FORMS = new JavaForms('equals', [['o']]);

/**
 * Represents a pattern that is used by ACLs to match zero or more
 * `Resource`s.
 */
export class ResourcePattern {
  /** A special literal resource name that corresponds to 'all resources of a certain type'. */
  static readonly WILDCARD_RESOURCE: string = '*';

  readonly #resourceType: ResourceType;
  readonly #name: string;
  readonly #patternType: PatternType;

  /**
   * Create a pattern using the supplied parameters.
   *
   * @param options - the resource type (non-null, specific), the resource
   *   name (non-null, which can be the `WILDCARD_RESOURCE`), and the pattern
   *   type (non-null, specific), which controls how the pattern will match
   *   resource names
   * @throws `TypeError` when a parameter is null or of the wrong type
   * @throws `IllegalArgumentError` when the resource type is `ANY`, or the
   *   pattern type `MATCH` or `ANY`, or the given parameters are not
   *   `{resourceType, name, patternType}`
   */
  constructor(options: {
    /** The specific resource type. */
    resourceType: ResourceType;
    /** The resource name, which can be the `WILDCARD_RESOURCE`. */
    name: string;
    /** The specific pattern type, which controls how the pattern will match resource names. */
    patternType: PatternType;
  }) {
    FORMS.match(options, arguments.length);
    const resourceType = FORMS.value(options, 'resourceType');
    const name = FORMS.value(options, 'name');
    const patternType = FORMS.value(options, 'patternType');
    if (resourceType === null) throw new TypeError('resourceType must not be null');
    if (!(resourceType instanceof ResourceType)) throw new TypeError('resourceType must be a ResourceType');
    if (name === null) throw new TypeError('name must not be null');
    if (typeof name !== 'string') throw new TypeError('name must be a string');
    if (patternType === null) throw new TypeError('patternType must not be null');
    if (!(patternType instanceof PatternType)) throw new TypeError('patternType must be a PatternType');
    if (resourceType === ResourceType.ANY) throw new IllegalArgumentError('resourceType must not be ANY');
    if (patternType === PatternType.MATCH || patternType === PatternType.ANY) {
      throw new IllegalArgumentError(`patternType must not be ${patternType.name()}`);
    }
    this.#resourceType = resourceType;
    this.#name = name;
    this.#patternType = patternType;
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
  name(): string {
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
   * @returns a filter which matches only this pattern.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  toFilter(): ResourcePatternFilter {
    if (arguments.length !== 0) TO_FILTER_FORMS.match(undefined, arguments.length);
    return new ResourcePatternFilter({ resourceType: this.#resourceType, name: this.#name, patternType: this.#patternType });
  }

  /**
   * @returns `true` if this Resource has any UNKNOWN components.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isUnknown(): boolean {
    if (arguments.length !== 0) IS_UNKNOWN_FORMS.match(undefined, arguments.length);
    return this.#resourceType.isUnknown() || this.#patternType.isUnknown();
  }

  /**
   * Whether `o` is a ResourcePattern of the same type, name and pattern type.
   *
   * @param o - the value to compare with, on its own or as `{ o }`
   * @returns whether they are equal
   */
  equals(o: unknown): boolean {
    EQUALS_FORMS.match(o, arguments.length);
    const other = EQUALS_FORMS.value(o, 'o');
    if (other === this) return true;
    if (!sameClass(this, other)) return false;
    const that = other as ResourcePattern;
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
   * Java's `toString()`: `ResourcePattern(resourceType=<type>, name=<name>, patternType=<type>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `ResourcePattern(resourceType=${this.#resourceType.name()}, name=${this.#name}, patternType=${this.#patternType.name()})`;
  }

  /**
   * The pattern as `JSON.stringify` writes it: `{ resourceType, name,
   * patternType }`, the enums by name.
   *
   * @returns the pattern's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { resourceType: string; name: string; patternType: string } {
    return { resourceType: this.#resourceType.name(), name: this.#name, patternType: this.#patternType.name() };
  }
}

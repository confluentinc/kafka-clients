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

// Java's MetricName: Java's methods (it is not a data class, the root
// conventions' list not naming it) over private fields. The tags, Java's
// Map<String, String>, are a Map, copied when taken and a new Map each call
// when returned. `equals` and
// `hashCode` are Java's bodies (Q29), the description left out as Java's.

import { JavaForms } from '../internal/args';
import { objectHashCode, sameClass, stringHashCode } from '../internal/java_object';

const FORMS = new JavaForms('MetricName', [['name', 'group', 'description', 'tags']]);
const NAME_FORMS = new JavaForms('name', [[]], { reader: true });
const GROUP_FORMS = new JavaForms('group', [[]], { reader: true });
const TAGS_FORMS = new JavaForms('tags', [[]], { reader: true });
const DESCRIPTION_FORMS = new JavaForms('description', [[]], { reader: true });
const EQUALS_FORMS = new JavaForms('equals', [['obj']]);
const HASH_CODE_FORMS = new JavaForms('hashCode', [[]], { reader: true });

// Java's Objects.requireNonNull(x): a NullPointerException, here a TypeError
// naming the parameter.
function checkString(value: unknown, what: string): string {
  if (value === null) throw new TypeError(`${what} must not be null`);
  if (typeof value !== 'string') throw new TypeError(`${what} must be a string`);
  return value;
}

// Java's Map<String, String> taken: a Map of string to string, copied.
function checkTags(value: unknown): ReadonlyMap<string, string> {
  if (value === null) throw new TypeError('tags must not be null');
  if (!(value instanceof Map)) throw new TypeError('tags must be a Map of string to string');
  const out = new Map<string, string>();
  for (const [key, tag] of value as Map<unknown, unknown>) {
    if (typeof key !== 'string' || typeof tag !== 'string') throw new TypeError('tags must be a Map of string to string');
    out.set(key, tag);
  }
  return out;
}

/**
 * The `MetricName` class encapsulates a metric's name, logical group and its
 * related attributes.
 *
 * This class captures the following parameters:
 *
 * - **name** The name of the metric
 * - **group** logical group name of the metrics to which this metric belongs.
 * - **description** A human-readable description to include in the metric.
 *   This is optional.
 * - **tags** additional key/value attributes of the metric. This is
 *   optional.
 *
 * group, tags parameters can be used to create unique metric names while
 * reporting in JMX or any custom reporting.
 */
export class MetricName {
  readonly #name: string;
  readonly #group: string;
  readonly #description: string;
  readonly #tags: ReadonlyMap<string, string>;

  /**
   * Creates a metric name.
   *
   * @param options - the name, the group, the description and the tags
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{name, group, description, tags}`
   * @throws `TypeError` when an argument is null (Java's
   *   `Objects.requireNonNull`), `undefined` or of the wrong type
   *
   * @remarks The tags, Java's `Map`, are copied.
   */
  constructor(options: {
    /** The name of the metric */
    name: string;
    /** logical group name of the metrics to which this metric belongs */
    group: string;
    /** A human-readable description to include in the metric */
    description: string;
    /** additional key/value attributes of the metric */
    tags: ReadonlyMap<string, string>;
  }) {
    FORMS.match(options, arguments.length);
    this.#name = checkString(FORMS.value(options, 'name'), 'name');
    this.#group = checkString(FORMS.value(options, 'group'), 'group');
    this.#description = checkString(FORMS.value(options, 'description'), 'description');
    this.#tags = checkTags(FORMS.value(options, 'tags'));
  }

  /**
   * The name of the metric.
   *
   * @returns the name
   * @throws `TypeError` when it is given an argument
   */
  name(): string {
    NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * The logical group name of the metric.
   *
   * @returns the group
   * @throws `TypeError` when it is given an argument
   */
  group(): string {
    GROUP_FORMS.match(undefined, arguments.length);
    return this.#group;
  }

  /**
   * The additional key/value attributes of the metric.
   *
   * @returns the tags, a new Map each call
   * @throws `TypeError` when it is given an argument
   */
  tags(): Map<string, string> {
    TAGS_FORMS.match(undefined, arguments.length);
    return new Map(this.#tags);
  }

  /**
   * The human-readable description of the metric.
   *
   * @returns the description
   * @throws `TypeError` when it is given an argument
   */
  description(): string {
    DESCRIPTION_FORMS.match(undefined, arguments.length);
    return this.#description;
  }

  /**
   * Java's hash code of the metric name: over the group, the name and the
   * tags (`Map.hashCode`: the sum of each entry's key and value hash codes
   * XORed); the description left out.
   *
   * @returns the hash code, a 32-bit integer
   * @throws `TypeError` when it is given an argument
   */
  hashCode(): number {
    HASH_CODE_FORMS.match(undefined, arguments.length);
    const prime = 31;
    let result = 1;
    result = (Math.imul(prime, result) + stringHashCode(this.#group)) | 0;
    result = (Math.imul(prime, result) + stringHashCode(this.#name)) | 0;
    let tags = 0;
    for (const [key, value] of this.#tags) tags = (tags + (objectHashCode(key) ^ objectHashCode(value))) | 0;
    return (Math.imul(prime, result) + tags) | 0;
  }

  /**
   * Whether `obj` is a `MetricName` of the same group, name and tags, Java's
   * `MetricName.equals`; the description is not compared.
   *
   * @param options - the object to compare with
   * @returns true when `obj` is an equal metric name
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
   * @returns true when `obj` is an equal metric name
   */
  equals(obj: unknown): boolean;
  equals(options: unknown): boolean {
    EQUALS_FORMS.match(options, arguments.length);
    const obj = EQUALS_FORMS.value(options, 'obj');
    if (this === obj) return true;
    if (obj === null || obj === undefined || !sameClass(this, obj)) return false;
    const other = obj as MetricName;
    if (this.#group !== other.#group || this.#name !== other.#name || this.#tags.size !== other.#tags.size) return false;
    for (const [key, value] of this.#tags) if (other.#tags.get(key) !== value) return false;
    return true;
  }

  /**
   * The metric name as Java writes it: `MetricName [name=<name>,
   * group=<group>, description=<description>, tags={<key>=<value>, ...}]`.
   *
   * @returns the metric name's text
   */
  toString(): string {
    const tags = [...this.#tags].map(([key, value]) => `${key}=${value}`).join(', ');
    return `MetricName [name=${this.#name}, group=${this.#group}, description=${this.#description}, tags={${tags}}]`;
  }

  /**
   * The key of a `MetricNameMap`: the group, the name and the tags sorted by
   * key, the fields `equals` compares. Called by `MetricNameMap`.
   *
   * @internal
   */
  internalKey(): string {
    const tags = [...this.#tags].sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    return JSON.stringify([this.#group, this.#name, tags]);
  }
}

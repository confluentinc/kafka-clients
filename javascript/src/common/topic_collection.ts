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

// Java's org.apache.kafka.common.TopicCollection: an abstract class whose
// constructor is private, with its two nested subclasses, TopicIdCollection
// and TopicNameCollection, classes of the namespace merged with it
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Names, Q11). Their constructors
// are private too: the static factories build them.

import { kind } from '../internal/args';
import { JavaForms } from '../internal/args';
import { Uuid } from './uuid';

const isIterable = (value: unknown): boolean =>
  value !== null && typeof value === 'object' && typeof (value as { [Symbol.iterator]?: unknown })[Symbol.iterator] === 'function';
// A bare null or string reaches the element check, which refuses it with
// Java's NullPointerException as a TypeError, or with the collection message
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).
const isCollectionArgument = (value: unknown): boolean => value === null || typeof value === 'string' || isIterable(value);
const OF_TOPIC_IDS_FORMS = new JavaForms('ofTopicIds', [['topics']], { bareTypes: { topics: isCollectionArgument } });
const OF_TOPIC_NAMES_FORMS = new JavaForms('ofTopicNames', [['topics']], { bareTypes: { topics: isCollectionArgument } });
const TOPIC_IDS_FORMS = new JavaForms('topicIds', [[]], { reader: true });
const TOPIC_NAMES_FORMS = new JavaForms('topicNames', [[]], { reader: true });

// The token the factories hand the constructors (Java's are private).
const MAKE: unique symbol = Symbol('TopicCollection');

/**
 * The elements of an iterable Java takes as a `Collection`, copied, as
 * Java's `new ArrayList<>(topics)`.
 *
 * @param value - the collection given
 * @param check - the element's check
 * @param what - the element's type, for the error
 * @returns the elements
 */
function elements<T>(value: unknown, check: (v: unknown) => boolean, what: string): T[] {
  if (value === null) throw new TypeError('topics must not be null');
  if (typeof value === 'string') throw new TypeError(`topics must be an iterable of ${what}, not string`);
  if (!isIterable(value)) throw new TypeError(`topics must be an iterable of ${what}, not ${kind(value)}`);
  const out: T[] = [];
  for (const v of value as Iterable<unknown>) {
    if (!check(v) && v !== null) throw new TypeError(`topics must be an iterable of ${what}, not one holding ${kind(v)}`);
    out.push(v as T);
  }
  return out;
}

/**
 * A class used to represent a collection of topics. This collection may
 * define topics by name or ID.
 *
 * @remarks An abstract class, as Java's is: build one with
 * `TopicCollection.ofTopicIds` or `TopicCollection.ofTopicNames`.
 */
export abstract class TopicCollection {
  /**
   * Makes `TopicCollection` nominal: TypeScript checks it by name, not by
   * shape, so a plain object is no TopicCollection.
   */
  readonly #brand = true;

  /**
   * Called by the subclasses only.
   *
   * @param token - the factories' token
   * @throws `TypeError` when called for `TopicCollection` itself, or not by
   *   the factories: Java's constructor is private
   */
  protected constructor(token: unknown) {
    if (token !== MAKE) {
      throw new TypeError('TopicCollection cannot be constructed directly; use TopicCollection.ofTopicIds or TopicCollection.ofTopicNames');
    }
  }

  /**
   * A collection of topics defined by topic ID.
   *
   * @param topics - the topic IDs, any iterable of Uuid, on its own
   * @returns a collection of topics defined by topic ID
   */
  static ofTopicIds(topics: Iterable<Uuid>): TopicCollection.TopicIdCollection;
  /**
   * The same as `ofTopicIds(topics)`, the topics given by name.
   *
   * @param options - the topic IDs
   * @returns a collection of topics defined by topic ID
   */
  static ofTopicIds(options: {
    /** The topic IDs, any iterable of Uuid. */
    topics: Iterable<Uuid>;
  }): TopicCollection.TopicIdCollection;
  /**
   * Builds a collection of topic IDs.
   *
   * @param topics - the topic IDs
   * @returns a collection of topics defined by topic ID
   * @throws `TypeError` when they are null or not an iterable of Uuid
   */
  static ofTopicIds(topics: unknown): TopicCollection.TopicIdCollection {
    OF_TOPIC_IDS_FORMS.match(topics, arguments.length);
    const ids = elements<Uuid>(OF_TOPIC_IDS_FORMS.value(topics, 'topics'), (v) => v instanceof Uuid, 'Uuid');
    return TopicCollection.TopicIdCollection.internalCreate(ids);
  }

  /**
   * A collection of topics defined by topic name.
   *
   * @param topics - the topic names, any iterable of strings, on its own
   * @returns a collection of topics defined by topic name
   */
  static ofTopicNames(topics: Iterable<string>): TopicCollection.TopicNameCollection;
  /**
   * The same as `ofTopicNames(topics)`, the topics given by name.
   *
   * @param options - the topic names
   * @returns a collection of topics defined by topic name
   */
  static ofTopicNames(options: {
    /** The topic names, any iterable of strings. */
    topics: Iterable<string>;
  }): TopicCollection.TopicNameCollection;
  /**
   * Builds a collection of topic names.
   *
   * @param topics - the topic names
   * @returns a collection of topics defined by topic name
   * @throws `TypeError` when they are null, a string, or not an iterable of strings
   */
  static ofTopicNames(topics: unknown): TopicCollection.TopicNameCollection {
    OF_TOPIC_NAMES_FORMS.match(topics, arguments.length);
    const names = elements<string>(OF_TOPIC_NAMES_FORMS.value(topics, 'topics'), (v) => typeof v === 'string', 'strings');
    return TopicCollection.TopicNameCollection.internalCreate(names);
  }
}

// eslint-disable-next-line @typescript-eslint/no-namespace -- Java's nested classes TopicCollection.TopicIdCollection and TopicNameCollection, reached through the class name as in Java
export namespace TopicCollection {
  /**
   * A class used to represent a collection of topics defined by their topic
   * ID. Subclassing this class beyond the classes provided here is not
   * supported.
   */
  export class TopicIdCollection extends TopicCollection {
    readonly #topicIds: readonly Uuid[];

    private constructor(topicIds: readonly Uuid[]) {
      super(MAKE);
      this.#topicIds = Object.freeze([...topicIds]);
    }

    /**
     * Java's private constructor. Called by `TopicCollection.ofTopicIds`.
     *
     * @param topicIds - the topic IDs, copied
     * @returns the collection
     * @internal
     */
    static internalCreate(topicIds: readonly Uuid[]): TopicIdCollection {
      return new TopicIdCollection(topicIds);
    }

    /**
     * A collection of topic IDs.
     *
     * @returns the topic IDs, a new array
     * @throws `TypeError` when it is given an argument: it only reads
     */
    topicIds(): Uuid[] {
      if (arguments.length !== 0) TOPIC_IDS_FORMS.match(undefined, arguments.length);
      return [...this.#topicIds];
    }

    /**
     * The collection as JSON: its topic IDs.
     *
     * @returns the topic IDs
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, unknown> {
      return { topicIds: [...this.#topicIds] };
    }
  }

  /**
   * A class used to represent a collection of topics defined by their topic
   * name. Subclassing this class beyond the classes provided here is not
   * supported.
   */
  export class TopicNameCollection extends TopicCollection {
    readonly #topicNames: readonly string[];

    private constructor(topicNames: readonly string[]) {
      super(MAKE);
      this.#topicNames = Object.freeze([...topicNames]);
    }

    /**
     * Java's private constructor. Called by `TopicCollection.ofTopicNames`.
     *
     * @param topicNames - the topic names, copied
     * @returns the collection
     * @internal
     */
    static internalCreate(topicNames: readonly string[]): TopicNameCollection {
      return new TopicNameCollection(topicNames);
    }

    /**
     * A collection of topic names.
     *
     * @returns the topic names, a new array
     * @throws `TypeError` when it is given an argument: it only reads
     */
    topicNames(): string[] {
      if (arguments.length !== 0) TOPIC_NAMES_FORMS.match(undefined, arguments.length);
      return [...this.#topicNames];
    }

    /**
     * The collection as JSON: its topic names.
     *
     * @returns the topic names
     *
     * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
     * once released.
     */
    toJSON(): Record<string, unknown> {
      return { topicNames: [...this.#topicNames] };
    }
  }
}

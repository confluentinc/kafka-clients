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

// Java's org.apache.kafka.clients.admin.TopicListing, a value class that
// keeps Java's methods (it is not one of the admin data classes).

import { JavaForms } from '../internal/args';
import { Uuid } from '../common/uuid';

const FORMS = new JavaForms('TopicListing', [['name', 'topicId', 'internal']]);
const READER = (name: string): JavaForms => new JavaForms(name, [[]], { reader: true });
const TOPIC_ID_FORMS = READER('topicId');
const NAME_FORMS = READER('name');
const IS_INTERNAL_FORMS = READER('isInternal');

/**
 * A listing of a topic in the cluster.
 */
export class TopicListing {
  readonly #name: string | null;
  readonly #topicId: Uuid | null;
  readonly #internal: boolean;

  /**
   * Create an instance with the specified parameters.
   *
   * @param options - the topic name, the topic id, and whether the topic is
   *   internal to Kafka
   * @throws `IllegalArgumentError` when the given parameters are not
   *   `{name, topicId, internal}`
   * @throws `TypeError` for a value of the wrong type
   */
  constructor(options: {
    /** The topic name. */
    name: string | null;
    /** The topic id. */
    topicId: Uuid | null;
    /** Whether the topic is internal to Kafka. */
    internal: boolean;
  }) {
    FORMS.match(options, arguments.length);
    const name = FORMS.value(options, 'name');
    const topicId = FORMS.value(options, 'topicId');
    const internal = FORMS.value(options, 'internal');
    if (name !== null && typeof name !== 'string') throw new TypeError('name must be a string or null');
    if (topicId !== null && !(topicId instanceof Uuid)) throw new TypeError('topicId must be a Uuid or null');
    if (typeof internal !== 'boolean') throw new TypeError('internal must be a boolean');
    this.#name = name;
    this.#topicId = topicId;
    this.#internal = internal;
  }

  /**
   * The id of the topic.
   *
   * @returns the topic id
   * @throws `TypeError` when it is given an argument: it only reads
   */
  topicId(): Uuid | null {
    if (arguments.length !== 0) TOPIC_ID_FORMS.match(undefined, arguments.length);
    return this.#topicId;
  }

  /**
   * The name of the topic.
   *
   * @returns the topic name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  name(): string | null {
    if (arguments.length !== 0) NAME_FORMS.match(undefined, arguments.length);
    return this.#name;
  }

  /**
   * Whether the topic is internal to Kafka. An example of an internal topic
   * is the offsets and group management topic: __consumer_offsets.
   *
   * @returns whether the topic is internal
   * @throws `TypeError` when it is given an argument: it only reads
   */
  isInternal(): boolean {
    if (arguments.length !== 0) IS_INTERNAL_FORMS.match(undefined, arguments.length);
    return this.#internal;
  }

  /**
   * Java's `toString()`: `(name=<name>, topicId=<id>, internal=<internal>)`.
   *
   * @returns the text
   */
  toString(): string {
    return `(name=${String(this.#name)}, topicId=${String(this.#topicId)}, internal=${String(this.#internal)})`;
  }

  /**
   * The listing as `JSON.stringify` writes it: `{ name, topicId, internal }`,
   * the id as its base64 text.
   *
   * @returns the listing's fields
   *
   * @remarks *(js-only)* JavaScript's JSON protocol, whose output is frozen
   * once released.
   */
  toJSON(): { name: string | null; topicId: string | null; internal: boolean } {
    return { name: this.#name, topicId: this.#topicId === null ? null : this.#topicId.toString(), internal: this.#internal };
  }
}

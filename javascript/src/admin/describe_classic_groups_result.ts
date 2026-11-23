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
// Java's org.apache.kafka.clients.admin.DescribeClassicGroupsResult, whose Java constructor is
// public: it takes Java's `Map<String, KafkaFuture<ClassicGroupDescription>>` as a `Map`
// or a plain object (javascript/CLAUDE.md, JavaScript Binding Conventions, Types,
// Q25), and `describedGroups()` returns it as a `Map`.

import { isPlainObject, JavaForms, kind } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { ClassicGroupDescription } from './classic_group_description';

const FORMS = new JavaForms('DescribeClassicGroupsResult', [['futures']], {
  bareTypes: { futures: (value) => value === null || value instanceof Map },
});
const DESCRIBED_GROUPS_FORMS = new JavaForms('describedGroups', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.describeClassicGroups(groupIds)` call.
 */
export class DescribeClassicGroupsResult {
  readonly #futures: Map<string, KafkaFuture<ClassicGroupDescription>> | null;

  /**
   * The result over the futures of each group.
   *
   * @param futures - the futures, by group id, on its own
   * @throws `TypeError` when they are not a map of futures (null is stored,
   *   as Java's constructor stores it)
   */
  constructor(futures: ReadonlyMap<string, KafkaFuture<ClassicGroupDescription>>);
  /**
   * The same as `new DescribeClassicGroupsResult(futures)`.
   *
   * @param options - the futures, by group id, as a `Map` or a plain object
   * @throws `TypeError` when they are not a map of futures (null is stored,
   *   as Java's constructor stores it)
   */
  constructor(options: {
    /** The futures, by group id. */
    futures: ReadonlyMap<string, KafkaFuture<ClassicGroupDescription>> | Readonly<Record<string, KafkaFuture<ClassicGroupDescription>>>;
  });
  /**
   * Builds the result.
   *
   * @param options - the futures
   * @throws `TypeError` when they are not a map of futures (null is stored,
   *   as Java's constructor stores it)
   */
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const futures = FORMS.value(options, 'futures');
    // Java's constructor stores a null map; all() and describedGroups()
    // dereference it.
    if (futures === null) {
      this.#futures = null;
      return;
    }
    let entries: Array<[unknown, unknown]>;
    if (futures instanceof Map) entries = [...(futures as Map<unknown, unknown>)];
    else if (isPlainObject(futures)) entries = Object.entries(futures);
    else throw new TypeError(`futures must be a Map or a plain object, not ${kind(futures)}`);
    const out = new Map<string, KafkaFuture<ClassicGroupDescription>>();
    for (const [k, v] of entries) {
      if (typeof k !== 'string') throw new TypeError(`futures must be keyed by strings, not ${kind(k)}`);
      if (!(v instanceof KafkaFuture)) throw new TypeError(`futures must map to KafkaFuture, not ${kind(v)}`);
      out.set(k, v as KafkaFuture<ClassicGroupDescription>);
    }
    this.#futures = out;
  }

  /**
   * Return a map from group id to futures which yield group descriptions.
   *
   * @returns a new map of the futures, by group id
   * @throws `TypeError` when it is given an argument: it only reads, or when
   *   the futures are null (Java dereferences them)
   */
  describedGroups(): Map<string, KafkaFuture<ClassicGroupDescription>> {
    if (arguments.length !== 0) DESCRIBED_GROUPS_FORMS.match(undefined, arguments.length);
    if (this.#futures === null) throw new TypeError('futures must not be null');
    return new Map(this.#futures);
  }

  /**
   * Return a future which yields all ClassicGroupDescription objects, if all the describes succeed.
   *
   * @returns the future of the descriptions, by group id
   * @throws `TypeError` when it is given an argument: it only reads, or when
   *   the futures are null (Java dereferences them)
   */
  all(): KafkaFuture<Map<string, ClassicGroupDescription>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const futures = this.#futures;
    if (futures === null) throw new TypeError('futures must not be null');
    return KafkaFuture.allOf([...futures.values()]).thenApply(() => {
      const descriptions = new Map<string, ClassicGroupDescription>();
      for (const [groupId, future] of futures) descriptions.set(groupId, future.getNow(null) as ClassicGroupDescription);
      return descriptions;
    });
  }
}

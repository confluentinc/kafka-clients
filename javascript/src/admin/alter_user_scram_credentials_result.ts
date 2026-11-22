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

// Java's org.apache.kafka.clients.admin.AlterUserScramCredentialsResult,
// whose Java constructor is public, refuses a null map and keeps an
// unmodifiable copy; `all()` is Java's `allOf` over its futures. Java's
// `Map<String, KafkaFuture<Void>>` is a `Map`; `values()` hands out a new one
// each call (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family).

import { JavaForms, kind } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';

const FORMS = new JavaForms('AlterUserScramCredentialsResult', [['futures']], {
  bareTypes: { futures: (value) => value === null || value instanceof Map },
});
const VALUES_FORMS = new JavaForms('values', [[]], { reader: true });
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.alterUserScramCredentials(alterations)` call.
 */
export class AlterUserScramCredentialsResult {
  readonly #futures: ReadonlyMap<string, KafkaFuture<void>>;

  /**
   * The result over the futures of each user.
   *
   * @param futures - the required map from user names to futures
   *   representing the results of the alteration(s) for each user
   * @throws `TypeError` when they are null, or not a map of strings to
   *   KafkaFutures
   */
  constructor(futures: ReadonlyMap<string, KafkaFuture<void>>);
  /**
   * The same as `new AlterUserScramCredentialsResult(futures)`.
   *
   * @param options - the required map from user names to futures
   * @throws `TypeError` when they are null, or not a map of strings to
   *   KafkaFutures
   */
  constructor(options: {
    /** The required map from user names to futures representing the results of the alteration(s) for each user. */
    futures: ReadonlyMap<string, KafkaFuture<void>> | Readonly<Record<string, KafkaFuture<void>>>;
  });
  /**
   * Builds the result.
   *
   * @param options - the futures
   * @throws `TypeError` when they are null, or not a map of strings to
   *   KafkaFutures
   */
  constructor(options: unknown) {
    FORMS.match(options, arguments.length);
    const futures = FORMS.value(options, 'futures');
    if (futures === null) throw new TypeError('futures must not be null');
    const isPlain = typeof futures === 'object' && Object.getPrototypeOf(futures) === Object.prototype;
    if (!(futures instanceof Map) && !isPlain) throw new TypeError(`futures must be a Map or a plain object, not ${kind(futures)}`);
    const entries = futures instanceof Map ? [...(futures as Map<unknown, unknown>)] : Object.entries(futures as Record<string, unknown>);
    // Java's requireNonNull refuses only a null map: a null user or a null
    // future is kept, and `all()` fails on a null future as Java's allOf does.
    for (const [user, future] of entries) {
      if (user !== null && typeof user !== 'string') throw new TypeError(`futures must have string keys, not ${kind(user)}`);
      if (future !== null && !(future instanceof KafkaFuture)) {
        throw new TypeError(`futures must have KafkaFuture values, not ${kind(future)}`);
      }
    }
    this.#futures = new Map(entries as Array<[string, KafkaFuture<void>]>);
  }

  /**
   * Return a map from user names to futures, which can be used to check the
   * status of the alteration(s) for each user.
   *
   * @returns a new map of the futures, by user name
   * @throws `TypeError` when it is given an argument: it only reads
   */
  values(): Map<string, KafkaFuture<void>> {
    if (arguments.length !== 0) VALUES_FORMS.match(undefined, arguments.length);
    return new Map(this.#futures);
  }

  /**
   * Return a future which succeeds only if all the user SCRAM credential
   * alterations succeed.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads, or when
   *   a future is null (Java's allOf dereferences it)
   */
  all(): KafkaFuture<void> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return KafkaFuture.allOf([...this.#futures.values()]);
  }
}

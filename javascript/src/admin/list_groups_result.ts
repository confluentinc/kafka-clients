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
// Java's org.apache.kafka.clients.admin.ListGroupsResult. Its Java
// constructor is package-private, over one future of the listings and the
// errors mixed: a private constructor here, built by the `@internal`
// `internalCreate` (Java's constructor, splitting that future as Java does)
// or, for the client, `internalOfClient`, over the core's own `all()`,
// `valid()` and `errors()` futures, as Python's. Java's `Collection`s are
// arrays (root CLAUDE.md, JavaScript Binding Conventions, Types).

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import { GroupListing } from './group_listing';

const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const VALID_FORMS = new JavaForms('valid', [[]], { reader: true });
const ERRORS_FORMS = new JavaForms('errors', [[]], { reader: true });

/**
 * The result of the `Admin.listGroups()` call.
 *
 * Instances come from the admin client's `listGroups()`.
 */
export class ListGroupsResult {
  readonly #all: KafkaFuture<GroupListing[]>;
  readonly #valid: KafkaFuture<GroupListing[]>;
  readonly #errors: KafkaFuture<unknown[]>;

  private constructor(all: KafkaFuture<GroupListing[]>, valid: KafkaFuture<GroupListing[]>, errors: KafkaFuture<unknown[]>) {
    this.#all = all;
    this.#valid = valid;
    this.#errors = errors;
  }

  /**
   * Java's package-private constructor: the listings and the errors of one
   * future, split as Java splits them.
   *
   * @param future - the future of the listings and the errors
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<Iterable<unknown>>): ListGroupsResult {
    const all = KafkaFuture.internalCreate<GroupListing[]>(false);
    const valid = KafkaFuture.internalCreate<GroupListing[]>(false);
    const errors = KafkaFuture.internalCreate<unknown[]>(false);
    future.thenApply((results) => {
      const curErrors: unknown[] = [];
      const curValid: GroupListing[] = [];
      for (const result of results) {
        if (result instanceof GroupListing) curValid.push(result);
        else curErrors.push(result);
      }
      if (curErrors.length > 0) all.internalComplete(null as unknown as GroupListing[], curErrors[0]);
      else all.internalComplete(Object.freeze([...curValid]) as GroupListing[], null);
      valid.internalComplete(Object.freeze([...curValid]) as GroupListing[], null);
      errors.internalComplete(Object.freeze([...curErrors]) as unknown[], null);
      return null;
    });
    return new ListGroupsResult(all, valid, errors);
  }

  /**
   * The result over the core's own futures. Called by the admin client.
   *
   * @param all - the future of `all()`
   * @param valid - the future of `valid()`
   * @param errors - the future of `errors()`
   * @returns the result
   * @internal
   */
  static internalOfClient(
    all: KafkaFuture<GroupListing[]>,
    valid: KafkaFuture<GroupListing[]>,
    errors: KafkaFuture<unknown[]>,
  ): ListGroupsResult {
    return new ListGroupsResult(all, valid, errors);
  }

  /**
   * Returns a future that yields either an error, or the full set of group
   * listings.
   *
   * In the event of a failure, the future yields nothing but the first error
   * which occurred.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<GroupListing[]> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return this.#all;
  }

  /**
   * Returns a future which yields just the valid listings.
   *
   * This future never fails with an error, no matter what happens. Errors
   * are completely ignored. If nothing can be fetched, an empty collection is
   * yielded. If there is an error, but some results can be returned, this
   * future will yield those partial results. When using this future, it is a
   * good idea to also check the errors future so that errors can be
   * displayed and handled.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  valid(): KafkaFuture<GroupListing[]> {
    if (arguments.length !== 0) VALID_FORMS.match(undefined, arguments.length);
    return this.#valid;
  }

  /**
   * Returns a future which yields just the errors which occurred.
   *
   * If this future yields a non-empty collection, it is very likely that
   * elements are missing from the valid() set.
   *
   * This future itself never fails with an error. In the event of an error,
   * this future will successfully yield a collection containing at least one
   * error.
   *
   * @returns the future
   * @throws `TypeError` when it is given an argument: it only reads
   */
  errors(): KafkaFuture<unknown[]> {
    if (arguments.length !== 0) ERRORS_FORMS.match(undefined, arguments.length);
    return this.#errors;
  }
}

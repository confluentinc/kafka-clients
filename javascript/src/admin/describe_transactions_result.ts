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

// Java's org.apache.kafka.clients.admin.DescribeTransactionsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family, Q16). Java keys its futures by the internal
// `CoordinatorKey.byTransactionalId`, which compares by the id alone: here a
// `Map` keyed by the id. `all()` is Java's `allOf` then `thenApply`.

import { JavaForms, kind } from '../internal/args';
import { illegalArgument } from '../internal/errors';
import { KafkaError } from '../common/kafka_error';
import { KafkaFuture } from '../common/kafka_future';
import type { TransactionDescription } from './transaction_description';

const DESCRIPTION_FORMS = new JavaForms('description', [['transactionalId']], {
  bareTypes: { transactionalId: (value) => value === null || typeof value === 'string' },
});
const ALL_FORMS = new JavaForms('all', [[]], { reader: true });

/**
 * The result of the `Admin.describeTransactions(transactionalIds)` call.
 *
 * Instances come from the admin client's `describeTransactions()`.
 */
export class DescribeTransactionsResult {
  readonly #futures: ReadonlyMap<string, KafkaFuture<TransactionDescription>>;

  private constructor(futures: ReadonlyMap<string, KafkaFuture<TransactionDescription>>) {
    this.#futures = futures;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param futures - the futures, by transactional id
   * @returns the result
   * @internal
   */
  static internalCreate(futures: ReadonlyMap<string, KafkaFuture<TransactionDescription>>): DescribeTransactionsResult {
    return new DescribeTransactionsResult(futures);
  }

  /**
   * Get the description of a specific transactional ID.
   *
   * @param transactionalId - the transactional ID to describe
   * @returns a future which completes when the transaction description of a
   *   particular transactional ID is available.
   * @throws `IllegalArgumentError` if the `transactionalId` was not included
   *   in the respective call to `Admin.describeTransactions(transactionalIds, options)`.
   */
  description(transactionalId: string): KafkaFuture<TransactionDescription>;
  /**
   * The same as `description(transactionalId)`, the id given by name.
   *
   * @param options - the transactional ID to describe
   * @returns the future
   */
  description(options: {
    /** The transactional ID to describe. */
    transactionalId: string;
  }): KafkaFuture<TransactionDescription>;
  description(transactionalId: unknown): KafkaFuture<TransactionDescription> {
    DESCRIPTION_FORMS.match(transactionalId, arguments.length);
    const id = DESCRIPTION_FORMS.value(transactionalId, 'transactionalId');
    if (id !== null && typeof id !== 'string') throw new TypeError(`transactionalId must be a string, not ${kind(id)}`);
    const future = id === null ? undefined : this.#futures.get(id);
    if (future === undefined) throw illegalArgument(`TransactionalId \`${String(id)}\` was not included in the request`);
    return future;
  }

  /**
   * Get a future which returns a map of the transaction descriptions
   * requested in the respective call to
   * `Admin.describeTransactions(transactionalIds, options)`.
   *
   * If the description fails on any of the transactional IDs in the request,
   * then this future will also fail.
   *
   * @returns a future which either completes when all transaction
   *   descriptions complete or fails if any of the descriptions cannot be
   *   obtained
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<Map<string, TransactionDescription>> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    const entries = [...this.#futures];
    return KafkaFuture.allOf(entries.map(([, f]) => f)).thenApply(() => {
      const results = new Map<string, TransactionDescription>();
      for (const [id, future] of entries) {
        try {
          results.set(id, future.getNow({ valueIfAbsent: null }) as TransactionDescription);
        } catch (e) {
          // This should be unreachable, because allOf ensured that all the
          // futures completed successfully.
          throw new KafkaError({ cause: e });
        }
      }
      return results;
    });
  }
}

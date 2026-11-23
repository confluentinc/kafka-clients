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

// Java's org.apache.kafka.clients.admin.ListTransactionsResult. Its Java
// constructor is package-private: a private constructor here, the client
// building it through `internalCreate` (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). It holds Java's one future of a map of
// futures by broker; `byBrokerId()` and `allByBrokerId()` make a new future
// each call, as Java's do, and `all()` flattens the latter's lists.

import { JavaForms } from '../internal/args';
import { KafkaFuture } from '../common/kafka_future';
import type { TransactionListing } from './transaction_listing';

const ALL_FORMS = new JavaForms('all', [[]], { reader: true });
const BY_BROKER_ID_FORMS = new JavaForms('byBrokerId', [[]], { reader: true });
const ALL_BY_BROKER_ID_FORMS = new JavaForms('allByBrokerId', [[]], { reader: true });

/**
 * The result of the `Admin.listTransactions()` call.
 */
export class ListTransactionsResult {
  readonly #future: KafkaFuture<ReadonlyMap<number, KafkaFuture<TransactionListing[]>>>;

  /**
   * Instances come from the admin client's `listTransactions()`.
   *
   * @param future - the future of each broker's future, by broker id
   */
  private constructor(future: KafkaFuture<ReadonlyMap<number, KafkaFuture<TransactionListing[]>>>) {
    this.#future = future;
  }

  /**
   * Java's package-private constructor. Called by the admin client.
   *
   * @param future - the future of each broker's future, by broker id
   * @returns the result
   * @internal
   */
  static internalCreate(future: KafkaFuture<ReadonlyMap<number, KafkaFuture<TransactionListing[]>>>): ListTransactionsResult {
    return new ListTransactionsResult(future);
  }

  /**
   * Get all transaction listings. If any of the underlying requests fail,
   * then the future returned from this method will also fail with the first
   * encountered error.
   *
   * @returns A future containing the collection of transaction listings. The
   *   future completes when all transaction listings are available and fails
   *   after any non-retriable error.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  all(): KafkaFuture<TransactionListing[]> {
    if (arguments.length !== 0) ALL_FORMS.match(undefined, arguments.length);
    return this.allByBrokerId().thenApply((map) => {
      const allListings: TransactionListing[] = [];
      for (const listings of map.values()) allListings.push(...listings);
      return allListings;
    });
  }

  /**
   * Get a future which returns a map containing the underlying listing future
   * for each broker in the cluster. This is useful, for example, if a partial
   * listing of transactions is sufficient, or if you want more granular error
   * details.
   *
   * @returns A future containing a map of futures by broker which complete
   *   individually when their respective transaction listings are available.
   *   The top-level future returned from this method may fail if the admin
   *   client is unable to lookup the available brokers in the cluster.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  byBrokerId(): KafkaFuture<Map<number, KafkaFuture<TransactionListing[]>>> {
    if (arguments.length !== 0) BY_BROKER_ID_FORMS.match(undefined, arguments.length);
    const result = KafkaFuture.internalCreate<Map<number, KafkaFuture<TransactionListing[]>>>(false);
    this.#future.whenComplete((brokerFutures, error) => {
      if (brokerFutures !== null && brokerFutures !== undefined) result.internalComplete(new Map(brokerFutures), null);
      else result.internalComplete(null as unknown as Map<number, KafkaFuture<TransactionListing[]>>, error);
    });
    return result;
  }

  /**
   * Get all transaction listings in a map which is keyed by the ID of
   * respective broker that is currently managing them. If any of the
   * underlying requests fail, then the future returned from this method will
   * also fail with the first encountered error.
   *
   * @returns A future containing a map from the broker ID to the transactions
   *   hosted by that broker respectively. This future completes when all
   *   transaction listings are available and fails after any non-retriable
   *   error.
   * @throws `TypeError` when it is given an argument: it only reads
   */
  allByBrokerId(): KafkaFuture<Map<number, TransactionListing[]>> {
    if (arguments.length !== 0) ALL_BY_BROKER_ID_FORMS.match(undefined, arguments.length);
    const allFuture = KafkaFuture.internalCreate<Map<number, TransactionListing[]>>(false);
    const allListingsMap = new Map<number, TransactionListing[]>();
    this.#future.whenComplete((map, topLevelError) => {
      if (topLevelError !== null && topLevelError !== undefined) {
        allFuture.internalComplete(null as unknown as Map<number, TransactionListing[]>, topLevelError);
        return;
      }
      const brokers = map as ReadonlyMap<number, KafkaFuture<TransactionListing[]>>;
      const remainingResponses = new Set(brokers.keys());
      // Java completes nothing for an empty map: its future stays pending.
      for (const [brokerId, future] of brokers) {
        future.whenComplete((listings, brokerError) => {
          if (brokerError !== null && brokerError !== undefined) {
            allFuture.internalComplete(null as unknown as Map<number, TransactionListing[]>, brokerError);
          } else if (!allFuture.isDone()) {
            allListingsMap.set(brokerId, listings as TransactionListing[]);
            remainingResponses.delete(brokerId);
            if (remainingResponses.size === 0) allFuture.internalComplete(allListingsMap, null);
          }
        });
      }
    });
    return allFuture;
  }
}

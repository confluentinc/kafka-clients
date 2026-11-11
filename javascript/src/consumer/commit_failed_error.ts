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

// GENERATED, DO NOT EDIT. Produced by `cargo xtask generate-error-codes` from
// the Java source of `org.apache.kafka.clients.consumer.CommitFailedException`;
// `cargo xtask check-generated` checks it is up to date.

import { setForms } from '../internal/throwable';
import { KafkaError } from '../common/kafka_error';

/**
 * This exception is raised when an offset commit with
 * `KafkaConsumer.commitSync()` fails with an unrecoverable error. This
 * exception is generated on the client side, typically when a group rebalance
 * completes before the commit could be successfully applied. In this case, the
 * commit cannot generally be retried because some of the partitions may have
 * already been assigned to another member in the group.
 *
 * Java: `org.apache.kafka.clients.consumer.CommitFailedException`.
 */
export class CommitFailedError extends KafkaError {
  /**
   * Makes `CommitFailedError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * The FFI id of the class, `kafka_common_Error_CONSUMER_COMMIT_FAILED`, for
   * the binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = -19;

  /**
   * Java's `(String message)` constructor.
   *
   * @param options - the message
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; name?: never });
  /**
   * Java's `(String message)` constructor. Given the message on its own, the
   * same as `{ message }`.
   *
   * @param message - the message; `null` reads as `''`
   */
  constructor(message: string | null);
  /**
   * Java's no-argument constructor. The message is Java's own: "Commit cannot
   * be completed since the group has already rebalanced and assigned the
   * partitions to another member. This means that the time between subsequent
   * calls to poll() was longer than the configured max.poll.interval.ms, which
   * typically implies that the poll loop is spending too much time message
   * processing. You can address this either by increasing max.poll.interval.ms
   * or by reducing the maximum size of batches returned in poll() with
   * max.poll.records.".
   */
  constructor();
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
  }
}

setForms(CommitFailedError, [
  [['message'], 'message'],
  [[], { text: 'Commit cannot be completed since the group has already rebalanced and assigned the partitions to another member. This means that the time between subsequent calls to poll() was longer than the configured max.poll.interval.ms, which typically implies that the poll loop is spending too much time message processing. You can address this either by increasing max.poll.interval.ms or by reducing the maximum size of batches returned in poll() with max.poll.records.' }],
]);

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
// the Java source of `org.apache.kafka.common.errors.RecordTooLargeException`;
// `cargo xtask check-generated` checks it is up to date.

import * as P from '../../internal/payload';
import { payloadOf, setForms } from '../../internal/throwable';
import type { TopicPartitionMap } from '../topic_partition_map';
import { ApiError } from './api_error';

/**
 * This record is larger than the maximum allowable size
 *
 * Java: `org.apache.kafka.common.errors.RecordTooLargeException`.
 */
export class RecordTooLargeError extends ApiError {
  /**
   * Makes `RecordTooLargeError` nominal: TypeScript checks it by name, not by
   * shape. It holds no state.
   */
  readonly #brand = true;

  /**
   * Java's field `Map<TopicPartition, Long> recordTooLargePartitions`.
   */
  readonly #recordTooLargePartitions: TopicPartitionMap<bigint> | null;

  /**
   * The FFI id of the class, `kafka_common_Error_MESSAGE_TOO_LARGE`, for the
   * binding and the test harness.
   *
   * @internal
   */
  static override readonly _ffiId: number = 10;

  /**
   * Java's no-argument constructor. The message is empty, Java's null.
   */
  constructor();
  /**
   * Java's `(String message, Throwable cause)` constructor.
   *
   * @param options - the message and the cause
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
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
   * Java's `(Throwable cause)` constructor. The message is the cause's
   * `String()`, as Java's is `cause.toString()` (`''` for a null cause).
   *
   * @param options - the cause
   */
  constructor(options: { /** The cause (`Error`'s `cause`). */ cause: unknown; name?: never });
  /**
   * Java's `(Throwable cause)` constructor. The message is the cause's
   * `String()`, as Java's is `cause.toString()` (`''` for a null cause). Given
   * the cause on its own, the same as `{ cause }`.
   *
   * @param cause - the cause (`Error`'s `cause`)
   */
  constructor(cause: Error);
  /**
   * Java's `(String message, Map<TopicPartition, Long>
   * recordTooLargePartitions)` constructor.
   *
   * @param options - the message and the `recordTooLargePartitions`
   */
  constructor(options: { /** The message; `null` reads as `''`. */ message: string | null; /** Java's `Map<TopicPartition, Long> recordTooLargePartitions`. */ recordTooLargePartitions: TopicPartitionMap<bigint | number> | null; name?: never });
  constructor(...args: unknown[]) {
    super(...(args as [{ message: string | null; cause: unknown; name?: never }]));
    const fields = payloadOf(this);
    this.#recordTooLargePartitions = fields.recordTooLargePartitions as TopicPartitionMap<bigint> | null;
  }

  /**
   * Java's `recordTooLargePartitions()`.
   *
   * @returns Java's `recordTooLargePartitions`, as a new TopicPartitionMap the
   * caller may change @throws `TypeError` when it is given an argument: it only
   * reads
   */
  recordTooLargePartitions(): TopicPartitionMap<bigint> | null {
    P.readOnly('recordTooLargePartitions', arguments.length);
    return P.topicPartitionLongMap.copy(this.#recordTooLargePartitions) as TopicPartitionMap<bigint> | null;
  }
}

setForms(RecordTooLargeError, [
  [[], null, () => ({ recordTooLargePartitions: null })],
  [['message', 'cause'], 'message', () => ({ recordTooLargePartitions: null })],
  [['message'], 'message', () => ({ recordTooLargePartitions: null })],
  [['cause'], 'cause.toString()', () => ({ recordTooLargePartitions: null })],
  [['message', 'recordTooLargePartitions'], 'message', (v) => ({ recordTooLargePartitions: v.recordTooLargePartitions })],
], {
  order: ['message', 'cause', 'recordTooLargePartitions'],
  params: { recordTooLargePartitions: P.topicPartitionLongMap },
  fields: { recordTooLargePartitions: [P.topicPartitionLongMap, null] },
});

// Copyright 2025 Confluent Inc.
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

// Java's Producer interface, with the members KafkaProducer translates
// (root CLAUDE.md, JavaScript Binding Conventions, Class family). Java's
// Producer has no static member, so it has no runtime object.
//
// Left out, as on KafkaProducer (the producer package's documentation, in
// index.ts): by request, initTransactions, beginTransaction,
// sendOffsetsToTransaction, commitTransaction and abortTransaction; for want
// of an FFI entry point, registerMetricForSubscription,
// unregisterMetricFromSubscription and clientInstanceId; and, not translated
// yet, partitionsFor and metrics.

import type { KafkaFuture } from '../common/kafka_future';
import type { Duration } from '../duration';
import type { Callback } from './callback';
import type { CloseArgs } from './kafka_producer';
import type { ProducerRecord } from './producer_record';
import type { RecordMetadata } from './record_metadata';

/**
 * The interface for the `KafkaProducer`.
 *
 * @see KafkaProducer
 */
export interface Producer {
  /**
   * See `KafkaProducer.send({ record, callback })`.
   *
   * @param options - the record and the callback
   * @returns resolving at hand-over to the future of the record's metadata
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord;
    /**
     * A user-supplied callback to execute when the record has been
     * acknowledged by the server (null indicates no callback).
     */
    callback: Callback | null;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * See `KafkaProducer.send(record)`.
   *
   * @param options - the record
   * @returns resolving at hand-over to the future of the record's metadata
   */
  send(options: {
    /** The record to send. */
    record: ProducerRecord;
    callback?: never;
  }): Promise<KafkaFuture<RecordMetadata>>;
  /**
   * See `KafkaProducer.send(record)`: the record on its own.
   *
   * @param record - The record to send
   * @returns resolving at hand-over to the future of the record's metadata
   */
  send(record: ProducerRecord): Promise<KafkaFuture<RecordMetadata>>;

  /**
   * See `KafkaProducer.flush()`.
   *
   * @returns resolving once the records sent before it completed
   */
  flush(): Promise<void>;

  /**
   * See `KafkaProducer.close()`.
   *
   * @returns resolving once the producer is closed
   */
  close(): Promise<void>;
  /**
   * See `KafkaProducer.close({ timeout })`.
   *
   * @param options - the timeout
   * @returns resolving once the producer is closed
   */
  close(options: CloseArgs): Promise<void>;
  /**
   * See `KafkaProducer.close(timeout)`: the timeout on its own.
   *
   * @param timeout - The maximum time to wait
   * @returns resolving once the producer is closed
   */
  close(timeout: Duration): Promise<void>;

  /**
   * Closes the producer as `close()` does, so that `await using` closes it.
   *
   * @returns resolving once the producer is closed
   *
   * @remarks *(js-only)* JavaScript's async disposal protocol, the form
   * Java's `Closeable.close()` takes in a `try`-with-resources block.
   */
  [Symbol.asyncDispose](): Promise<void>;
}

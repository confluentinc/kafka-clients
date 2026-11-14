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

/**
 * The public classes of Java's `org.apache.kafka.clients.producer` this
 * client has.
 *
 * Transactions aren't in the preview: `KafkaProducer`'s transaction calls
 * (`initTransactions`, `beginTransaction`, `sendOffsetsToTransaction`,
 * `commitTransaction`, `abortTransaction`) come in a later release, and so
 * do `MockProducer`'s (those five, `fenceProducer`, `transactionInitialized`,
 * `transactionInFlight`, `transactionCommitted`, `transactionAborted`,
 * `sentOffsets`, `commitCount`, `uncommittedRecords`,
 * `consumerGroupOffsetsHistory`, `uncommittedOffsets` and their error
 * fields).
 *
 * Left out because the C FFI has no entry point for them:
 * `KafkaProducer.clientInstanceId`, `registerMetricForSubscription` and
 * `unregisterMetricFromSubscription`, and `MockProducer`'s
 * `clientInstanceId`, `disableTelemetry`, `injectTimeoutException`,
 * `setClientInstanceId`, `registerMetricForSubscription`,
 * `unregisterMetricFromSubscription` and `addedMetrics`.
 * `MockProducer.setMockMetrics` is left out too: the FFI takes a native
 * `KafkaMetric`, which the binding cannot build.
 *
 * Left out because it takes an untranslated type (Q30):
 * `MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)`, so a
 * mock's cluster is always Java's `Cluster.empty()`.
 *
 * Serializers: whether a serializer's methods must all be async is not
 * decided yet (Q50).
 *
 * @packageDocumentation
 */

export { KafkaProducer } from './kafka_producer';
export { MockProducer } from './mock_producer';
export type { Partitioner } from './partitioner';
export type { Producer } from './producer';
export type { CloseArgs } from './kafka_producer';
export type { ProducerConfigs } from './producer_configs';
export type { Callback } from './callback';
export { ProducerRecord } from './producer_record';
export { RecordMetadata } from './record_metadata';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { BufferExhaustedError } from './buffer_exhausted_error';
export { ProducerConfig } from './producer_config';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;

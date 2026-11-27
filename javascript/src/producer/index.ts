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
 *
 * Left out because it takes an untranslated type:
 * `MockProducer(Cluster, boolean, Partitioner, Serializer, Serializer)`, so a
 * mock's cluster is always Java's `Cluster.empty()`.
 *
 * `ProducerConfig` holds Java's constants only, as its `ConfigDef` machinery
 * is not translated: `ProducerConfig(Properties)`, `ProducerConfig(Map)`,
 * `configNames()`, `configDef()` and `main(String[])` are left out, and
 * constructing it throws `TypeError`. Its constants include those Java's class
 * inherits from `AbstractConfig`, such as `CONFIG_PROVIDERS_CONFIG`.
 *
 * Serializers: whether a serializer's methods must all be async is not
 * decided yet. The functions of `key.serializer` and `value.serializer` get
 * the configuration with the generated `client.id`, once the core has checked
 * it. The keys a serde config function reads reach the core, which lists the
 * ones it doesn't use *(deviation: Java's `originals()` marks them used, so
 * Java doesn't list them. Why: a client copies its config handle
 * (`copy_for_client`) without the handle's record of the keys used, so an
 * `ignore()` on the handle can't reach the client's unused-keys line. Goes
 * when: the core keeps that record in the copy; the binding then marks them
 * with `ignore`, an additive change)*.
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

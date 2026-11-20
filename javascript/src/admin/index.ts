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
 * The public classes of Java's `org.apache.kafka.clients.admin` this client
 * has.
 *
 * Left out because the C FFI has no entry point for them: `Admin`'s
 * `addRaftVoter`, `removeRaftVoter`, `describeMetadataQuorum`,
 * `unregisterBroker`, `metrics`, `clientInstanceId`,
 * `registerMetricForSubscription`, `unregisterMetricFromSubscription`, the
 * share-group calls and the streams-group calls; `MockAdminClient`'s
 * `brokers()`, `broker(int)`, `injectTimeoutException`,
 * `handleDescribeTopicsUsingIds`, `setMockMetrics`, `addedMetrics`,
 * `disableTelemetry`, `setClientInstanceId`, `advanceTimeOnClientInstanceId`
 * and `addStreamsGroupDescription`. `AdminClient`, Java's abstract base class
 * of `KafkaAdminClient`, is not translated.
 *
 * `MockAdminClient` wraps the C FFI's mock, as the .NET client's does; Java
 * ships it in its test fixtures, and it is public here, as in the Rust core.
 *
 * @packageDocumentation
 */

export { AbstractOptions } from './abstract_options';
export { Admin } from './admin';
export type { AdminClientConfigs } from './admin_client_configs';
export { DescribeClusterOptions } from './describe_cluster_options';
export { DescribeClusterResult } from './describe_cluster_result';
export { KafkaAdminClient } from './kafka_admin_client';
export type { CloseArgs } from './kafka_admin_client';
export { ListTopicsOptions } from './list_topics_options';
export { ListTopicsResult } from './list_topics_result';
export { MockAdminClient } from './mock_admin_client';
export { TopicListing } from './topic_listing';

// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { AdminClientConfig } from './admin_client_config';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;

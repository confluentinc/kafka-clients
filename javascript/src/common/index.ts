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

// The public classes of Java's org.apache.kafka.common this client has. Each
// subpackage has its own import path (`@confluentinc/kafka-javascript/common/errors`),
// which this index does not re-export.

export { KafkaFuture } from './kafka_future';
export { Node } from './node';
export { PartitionInfo } from './partition_info';
export { TopicPartition } from './topic_partition';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { InvalidRecordError } from './invalid_record_error';
export { KafkaError } from './kafka_error';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;

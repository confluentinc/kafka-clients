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

// The common namespace: the public classes of Java's org.apache.kafka.common
// this client has, and its subpackages' namespaces (`common.errors` is
// org.apache.kafka.common.errors, `common.config` org.apache.kafka.common.config).

export { KafkaFuture } from './kafka_future';
export * as config from './config';
export * as security from './security';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { InvalidRecordError } from './invalid_record_error';
export { KafkaError } from './kafka_error';
export * as errors from './errors';
export * as metrics from './metrics';
export * as network from './network';
export * as protocol from './protocol';
export * as requests from './requests';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;

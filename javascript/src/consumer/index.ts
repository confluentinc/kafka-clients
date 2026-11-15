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

/**
 * The public classes of Java's `org.apache.kafka.clients.consumer` this
 * client has before the consumer: `OffsetAndMetadata`, which
 * `LogTruncationError`'s payload holds, and the consumer's error classes,
 * generated from the Java sources.
 *
 * @packageDocumentation
 */

export { OffsetAndMetadata } from './offset_and_metadata';
// BEGIN GENERATED EXPORTS (cargo xtask generate-error-codes; do not edit)
export { CommitFailedError } from './commit_failed_error';
export { InvalidOffsetError } from './invalid_offset_error';
export { LogTruncationError } from './log_truncation_error';
export { NoOffsetForPartitionError } from './no_offset_for_partition_error';
export { OffsetOutOfRangeError } from './offset_out_of_range_error';
export { RetriableCommitFailedError } from './retriable_commit_failed_error';
// END GENERATED EXPORTS

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined` (import by
// name or `import * as` instead; Node's own ESM default import still works).
export declare const __esModule: true;

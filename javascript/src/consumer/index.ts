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
// the Java sources (the generated modules of src/consumer/);
// `cargo xtask check-generated` checks it is up to date.

// `src/consumer`: the generated modules of src/consumer/.

/**
 * The classes of the Java package this client has, generated from the Java
 * sources.
 *
 * What they leave out:
 *
 * - `LogTruncationError`: Not generated: Java's constructors that take a type
 *   the client does not translate: `LogTruncationException(Map<TopicPartition,
 *   Long>, Map<TopicPartition, OffsetAndMetadata>)` and
 *   `LogTruncationException(String, Map<TopicPartition, Long>,
 *   Map<TopicPartition, OffsetAndMetadata>)`.
 * - `LogTruncationError`: Not generated, for a type the client does not
 *   translate: the field `Map<TopicPartition, OffsetAndMetadata>
 *   divergentOffsets` and `divergentOffsets()`.
 *
 * @packageDocumentation
 */

export { CommitFailedError } from './commit_failed_error';
export { InvalidOffsetError } from './invalid_offset_error';
export { LogTruncationError } from './log_truncation_error';
export { NoOffsetForPartitionError } from './no_offset_for_partition_error';
export { OffsetOutOfRangeError } from './offset_out_of_range_error';
export { RetriableCommitFailedError } from './retriable_commit_failed_error';

// tsc's CommonJS output sets `exports.__esModule`: declared, a default import
// under `esModuleInterop` is a compile error rather than `undefined`.
export declare const __esModule: true;

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

// Java's OffsetCommitCallback interface, a single method: a JS function taking
// one object of named parameters, `({ offsets, exception }) => unknown`, as the
// producer's Callback is (javascript/CLAUDE.md, JavaScript Binding
// Conventions, Class family). This module holds only its type.

import type { TopicPartitionMap } from '../common/topic_partition_map';
import type { OffsetAndMetadata } from './offset_and_metadata';

/**
 * A callback interface that the user can implement to trigger custom actions
 * when a commit request completes. The callback may be executed in any thread
 * calling `Consumer.poll()`.
 *
 * A callback method the user can implement to provide asynchronous handling of
 * commit request completion. This method will be called when the commit
 * request sent to the server has been acknowledged.
 *
 * @param options - the offsets and the error
 * @returns anything: a Promise is awaited
 *
 * @remarks Java's `onComplete(offsets, exception)` is one function taking one
 * object, named as Java names the parameters. *(deviation)* A callback that
 * returns a Promise is awaited, as JavaScript cannot block; a rejection is
 * handled as a throw.
 */
export type OffsetCommitCallback = (options: {
  /** A map of the offsets and associated metadata that this callback applies to. */
  offsets: TopicPartitionMap<OffsetAndMetadata> | null;
  /**
   * The error thrown during processing of the request, or null if the commit
   * completed successfully.
   */
  exception: Error | null;
}) => unknown;

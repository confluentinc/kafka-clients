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

// The default bodies of Java's ConsumerRebalanceListener interface
// (javascript/CLAUDE.md, JavaScript Binding Conventions, Class family): what
// the consumer runs for a member the user's listener leaves out.

import type { TopicPartitionSet } from '../common/topic_partition_set';
import type { ConsumerRebalanceListener } from '../consumer/consumer_rebalance_listener';

/**
 * Java's `ConsumerRebalanceListener` default methods, for a listener that
 * lacks them: the binding runs each with the user's listener as `this`
 * (`ConsumerRebalanceListenerDefaults.prototype.onPartitionsLost.call(listener,
 * { partitions })`), as Java runs an interface's default method on the object
 * that implements it.
 */
export class ConsumerRebalanceListenerDefaults {
  /**
   * Java's default `onPartitionsLost`: it calls `onPartitionsRevoked`.
   *
   * @param options - the partitions that were lost
   * @returns what the listener's `onPartitionsRevoked` returns
   */
  onPartitionsLost(this: ConsumerRebalanceListener, options: { partitions: TopicPartitionSet }): unknown {
    return this.onPartitionsRevoked({ partitions: options.partitions });
  }
}

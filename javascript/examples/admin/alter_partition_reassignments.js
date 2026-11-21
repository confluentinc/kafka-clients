#!/usr/bin/env node
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

'use strict';

// Starts, or with `cancel` cancels, partition reassignments.

const { NewPartitionReassignment } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { commaList, fail, futureOf, groups, main, parseInt32, topicPartition, usage } = require('./common');

const USAGE = 'usage: node examples/admin/alter_partition_reassignments.js [-X key=value]... <topic> <partition> <broker_id,...|cancel>...';

main(USAGE, async (admin, args) => {
  // null cancels the partition's reassignment, as Java's empty Optional.
  /** @type {TopicPartitionMap<NewPartitionReassignment | null>} */
  const reassignments = new TopicPartitionMap();
  for (const group of groups(args.positional(), 3, true)) {
    const partition = topicPartition(group);
    if (group[2] === 'cancel') {
      reassignments.set(partition, null);
      continue;
    }
    const replicas = commaList(group[2]).map((text) => parseInt32('broker id', text));
    if (replicas.length === 0) usage(`no replicas given for partition ${partition}`);
    reassignments.set(partition, new NewPartitionReassignment(replicas));
  }
  const result = admin.alterPartitionReassignments(reassignments);
  const futures = result.values();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const partition of reassignments.keys()) {
    const reassignment = reassignments.get(partition);
    const target = reassignment ? `[${reassignment.targetReplicas().join(',')}]` : null;
    try {
      await futureOf(futures, partition).get();
      console.log(
        target === null
          ? `Reassignment of partition ${partition} cancelled`
          : `Reassignment of partition ${partition} to ${target} started`,
      );
    } catch (error) {
      fail(
        target === null
          ? `Failed to cancel the reassignment of partition ${partition}`
          : `Failed to reassign partition ${partition} to ${target}`,
        error,
      );
    }
  }
});

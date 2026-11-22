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

// Moves replicas to other log dirs of their brokers.

const { TopicPartitionReplica, TopicPartitionReplicaMap } = require('@confluentinc/kafka-javascript/common');
const { fail, futureOf, groups, main, parseInt32 } = require('./common');

const USAGE = 'usage: node examples/admin/alter_replica_log_dirs.js [-X key=value]... <topic> <partition> <broker_id> <log_dir>...';

main(USAGE, async (admin, args) => {
  /** @type {TopicPartitionReplicaMap<string>} */
  const replicaAssignment = new TopicPartitionReplicaMap();
  for (const [topic, partition, broker, logDir] of groups(args.positional(), 4, true)) {
    const replica = new TopicPartitionReplica({
      topic,
      partition: parseInt32('partition', partition),
      brokerId: parseInt32('broker id', broker),
    });
    replicaAssignment.set(replica, logDir);
  }
  const result = admin.alterReplicaLogDirs(replicaAssignment);
  const futures = result.values();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const replica of replicaAssignment.keys()) {
    const logDir = replicaAssignment.get(replica);
    try {
      await futureOf(futures, replica).get();
      console.log(`Replica ${replica} moving to ${logDir}`);
    } catch (error) {
      fail(`Failed to move replica ${replica} to ${logDir}`, error);
    }
  }
});

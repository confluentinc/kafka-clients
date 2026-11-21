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

// Describes where replicas live: their log dir and any move in progress.

const { TopicPartitionReplica, TopicPartitionReplicaMap } = require('@confluentinc/kafka-javascript/common');
const { fail, futureOf, groups, main, parseInt32 } = require('./common');

const USAGE = 'usage: node examples/admin/describe_replica_log_dirs.js [-X key=value]... <topic> <partition> <broker_id>...';

main(USAGE, async (admin, args) => {
  // A replica named twice is described once.
  /** @type {TopicPartitionReplicaMap<TopicPartitionReplica>} */
  const replicas = new TopicPartitionReplicaMap();
  for (const [topic, partition, broker] of groups(args.positional(), 3, true)) {
    const replica = new TopicPartitionReplica({
      topic,
      partition: parseInt32('partition', partition),
      brokerId: parseInt32('broker id', broker),
    });
    replicas.set(replica, replica);
  }
  const result = admin.describeReplicaLogDirs([...replicas.keys()]);
  const futures = result.values();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const replica of replicas.keys()) {
    try {
      const info = await futureOf(futures, replica).get();
      let line = `Replica ${replica}: log dir ${info.getCurrentReplicaLogDir() ?? 'none'}, offset lag ${info.getCurrentReplicaOffsetLag()}`;
      const futureDir = info.getFutureReplicaLogDir();
      if (futureDir !== null) line += `, moving to ${futureDir}, future offset lag ${info.getFutureReplicaOffsetLag()}`;
      console.log(line);
    } catch (error) {
      fail(`Failed to describe replica ${replica}`, error);
    }
  }
});

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

// Deletes the records of partitions before the given offsets.

const { RecordsToDelete } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { fail, futureOf, groups, main, parseLong, topicPartition } = require('./common');

const USAGE = 'usage: node examples/admin/delete_records.js [-X key=value]... <topic> <partition> <offset>...';

main(USAGE, async (admin, args) => {
  /** @type {TopicPartitionMap<RecordsToDelete>} */
  const recordsToDelete = new TopicPartitionMap();
  const partitions = [];
  for (const group of groups(args.positional(), 3, true)) {
    const partition = topicPartition(group);
    partitions.push(partition);
    recordsToDelete.set(partition, RecordsToDelete.beforeOffset(parseLong('offset', group[2])));
  }
  const result = admin.deleteRecords(recordsToDelete);
  const futures = result.lowWatermarks();
  // In command-line order, as Rust's example prints; a partition named twice prints twice.
  for (const partition of partitions) {
    try {
      const deleted = await futureOf(futures, partition).get();
      console.log(`Deleted records of partition ${partition}: low watermark ${deleted.lowWatermark()}`);
    } catch (error) {
      fail(`Failed to delete records of partition ${partition}`, error);
    }
  }
});

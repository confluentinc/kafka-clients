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

// Deletes a group's committed offsets of some partitions.

const { TopicPartitionSet } = require('@confluentinc/kafka-javascript/common');
const { fail, main, topicPartitions, usage } = require('./common');

const USAGE = 'usage: node examples/admin/delete_consumer_group_offsets.js [-X key=value]... <group> (<topic> <partition>)...';

main(USAGE, async (admin, args) => {
  const [groupId, ...rest] = args.positional();
  if (groupId === undefined) usage('no group given');
  // A partition named twice is deleted once.
  const partitions = [...new TopicPartitionSet(topicPartitions(rest, true))];
  const result = admin.deleteConsumerGroupOffsets({ groupId, partitions });
  for (const partition of partitions) {
    try {
      await result.partitionResult(partition).get();
      console.log(`Group ${groupId}: ${partition} offset deleted`);
    } catch (error) {
      fail(`Failed to delete group ${groupId} offset for ${partition}`, error);
    }
  }
});

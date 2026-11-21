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

// Sets a group's committed offsets.

const { OffsetAndMetadata } = require('@confluentinc/kafka-javascript/consumer');
const { TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { fail, groups, main, parseLong, topicPartition, usage } = require('./common');

const USAGE = 'usage: node examples/admin/alter_consumer_group_offsets.js [-X key=value]... <group> (<topic> <partition> <offset>)...';

main(USAGE, async (admin, args) => {
  const [groupId, ...rest] = args.positional();
  if (groupId === undefined) usage('no group given');
  /** @type {TopicPartitionMap<OffsetAndMetadata>} */
  const offsets = new TopicPartitionMap();
  for (const spec of groups(rest, 3, true)) offsets.set(topicPartition(spec), new OffsetAndMetadata(parseLong('offset', spec[2])));
  const result = admin.alterConsumerGroupOffsets({ groupId, offsets });
  for (const [partition, offset] of offsets) {
    try {
      await result.partitionResult(partition).get();
      console.log(`Group ${groupId}: ${partition} offset set to ${offset.offset}`);
    } catch (error) {
      fail(`Failed to alter group ${groupId} offset for ${partition}`, error);
    }
  }
});

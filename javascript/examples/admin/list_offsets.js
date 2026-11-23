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

// Looks up partitions' offsets: the earliest, the latest, or by timestamp.

const { ListOffsetsOptions, OffsetSpec } = require('@confluentinc/kafka-javascript/admin');
const { IsolationLevel, TopicPartitionMap } = require('@confluentinc/kafka-javascript/common');
const { choose, fail, groups, main, parseLong, topicPartition, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/list_offsets.js [-X key=value]... <read_committed|read_uncommitted> ' +
  '<topic> <partition> <earliest|latest|max_timestamp|earliest_local|latest_tiered|earliest_pending_upload|timestamp:ms>...';

/**
 * @param {string} text
 * @returns {OffsetSpec}
 */
function offsetSpec(text) {
  if (text.startsWith('timestamp:')) return OffsetSpec.forTimestamp(parseLong('timestamp', text.slice('timestamp:'.length)));
  return choose('offset spec', text, [
    ['earliest', OffsetSpec.earliest()],
    ['latest', OffsetSpec.latest()],
    ['max_timestamp', OffsetSpec.maxTimestamp()],
    ['earliest_local', OffsetSpec.earliestLocal()],
    ['latest_tiered', OffsetSpec.latestTiered()],
    ['earliest_pending_upload', OffsetSpec.earliestPendingUpload()],
  ]);
}

main(USAGE, async (admin, args) => {
  const [level, ...specs] = args.positional();
  if (level === undefined) usage('no isolation level given');
  const isolationLevel = choose('isolation level', level, [
    ['read_committed', IsolationLevel.READ_COMMITTED],
    ['read_uncommitted', IsolationLevel.READ_UNCOMMITTED],
  ]);
  /** @type {TopicPartitionMap<OffsetSpec>} */
  const topicPartitionOffsets = new TopicPartitionMap();
  for (const group of groups(specs, 3, true)) topicPartitionOffsets.set(topicPartition(group), offsetSpec(group[2]));
  const result = admin.listOffsets({ topicPartitionOffsets, options: new ListOffsetsOptions(isolationLevel) });
  for (const partition of topicPartitionOffsets.keys()) {
    try {
      const info = await result.partitionResult(partition).get();
      console.log(`${partition}: offset ${info.offset()}, timestamp ${info.timestamp()}, leader epoch ${info.leaderEpoch() ?? 'none'}`);
    } catch (error) {
      fail(`Failed to list offsets of ${partition}`, error);
    }
  }
});

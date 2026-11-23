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

// Lists a group's committed offsets, of every partition or of some.

const { ListConsumerGroupOffsetsOptions, ListConsumerGroupOffsetsSpec } = require('@confluentinc/kafka-javascript/admin');
const { compareStrings, main, topicPartitions, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/list_consumer_group_offsets.js [-X key=value]... [--require-stable] <group> [<topic> <partition>]...';

main(USAGE, async (admin, args) => {
  const requireStable = args.flag('require-stable');
  const [group, ...rest] = args.positional();
  if (group === undefined) usage('no group given');
  const partitions = topicPartitions(rest, false);
  // No partition: every partition the group committed.
  const spec =
    partitions.length === 0 ? new ListConsumerGroupOffsetsSpec() : new ListConsumerGroupOffsetsSpec({ topicPartitions: partitions });
  const result = admin.listConsumerGroupOffsets({
    groupSpecs: new Map([[group, spec]]),
    options: new ListConsumerGroupOffsetsOptions({ requireStable }),
  });
  const offsets = [...(await result.partitionsToOffsetAndMetadata(group).get())].sort(
    ([a], [b]) => compareStrings(String(a.topic), String(b.topic)) || a.partition - b.partition,
  );
  console.log(`Group ${group}: ${offsets.length} partitions`);
  for (const [partition, offset] of offsets) {
    if (offset === null) console.log(`  ${partition}: no committed offset`);
    else
      console.log(
        `  ${partition}: offset ${offset.offset}, leader epoch ${offset.leaderEpoch ?? 'none'}, metadata ${JSON.stringify(offset.metadata)}`,
      );
  }
});

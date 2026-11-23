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

// Elects partition leaders: the preferred replica, or an unclean election.

const { ElectionType } = require('@confluentinc/kafka-javascript/common');
const { choose, compareStrings, fail, main, topicPartitions, usage } = require('./common');

const USAGE = 'usage: node examples/admin/elect_leaders.js [-X key=value]... <preferred|unclean> [<topic> <partition>]...';

main(USAGE, async (admin, args) => {
  const [type, ...rest] = args.positional();
  if (type === undefined) usage('no election type given');
  const electionType = choose('election type', type, [
    ['preferred', ElectionType.PREFERRED],
    ['unclean', ElectionType.UNCLEAN],
  ]);
  const partitions = topicPartitions(rest, false);
  // No partition is Java's null: every partition.
  const result = admin.electLeaders({ electionType, partitions: partitions.length === 0 ? null : partitions });
  // One future for the whole call; each partition's value is its error, or null.
  const outcomes = [...(await result.partitions().get())].sort(
    ([a], [b]) => compareStrings(String(a.topic), String(b.topic)) || a.partition - b.partition,
  );
  if (outcomes.length === 0) console.log('No leader election needed');
  for (const [partition, error] of outcomes) {
    if (error === null) console.log(`Leader elected for ${partition}`);
    else fail(`Failed to elect leader for ${partition}`, error);
  }
});

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

// Raises topics' partition counts.

const { NewPartitions } = require('@confluentinc/kafka-javascript/admin');
const { fail, groups, main, parseInt32 } = require('./common');

const USAGE = 'usage: node examples/admin/create_partitions.js [-X key=value]... <topic> <total_partitions>...';

main(USAGE, async (admin, args) => {
  /** @type {Map<string, NewPartitions>} */
  const newPartitions = new Map();
  for (const [topic, total] of groups(args.positional(), 2, true)) {
    newPartitions.set(topic, NewPartitions.increaseTo(parseInt32('partition count', total)));
  }
  const result = admin.createPartitions(newPartitions);
  for (const [topic, partitions] of newPartitions) {
    try {
      await result.values().get(topic)?.get();
      console.log(`Topic ${topic} now has ${partitions.totalCount()} partitions`);
    } catch (error) {
      fail(`Failed to add partitions to topic ${topic}`, error);
    }
  }
});

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

// Describes topics by name: their partitions, leaders and replicas.

const { DescribeTopicsOptions } = require('@confluentinc/kafka-javascript/admin');
const { TopicCollection } = require('@confluentinc/kafka-javascript/common');
const { fail, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/describe_topics.js [-X key=value]... [--authorized-operations] <topic>...';

/**
 * @param {import('@confluentinc/kafka-javascript/common').Node[]} nodes
 * @returns {string}
 */
function ids(nodes) {
  return `[${nodes.map((node) => node.id).join(',')}]`;
}

main(USAGE, async (admin, args) => {
  const includeAuthorizedOperations = args.flag('authorized-operations');
  const topics = args.positional();
  if (topics.length === 0) usage('no topic given');
  const result = admin.describeTopics({
    topics: TopicCollection.ofTopicNames(topics),
    options: new DescribeTopicsOptions({ includeAuthorizedOperations }),
  });
  // A by-name call keys its futures by topic name.
  const futures = result.topicNameValues();
  if (futures === null) return;
  for (const topic of new Set(topics)) {
    try {
      const description = await futures.get(topic)?.get();
      if (description === undefined) continue;
      console.log(`Topic ${description.name} (id ${description.topicId})${description.isInternal ? ' internal' : ''}`);
      for (const partition of description.partitions) {
        const leader = partition.leader();
        console.log(
          `  partition ${partition.partition()}: leader ${leader === null ? 'none' : leader.id}, ` +
            `replicas ${ids(partition.replicas())}, isr ${ids(partition.isr())}`,
        );
      }
      if (description.authorizedOperations !== null) {
        console.log(`  authorized operations: ${[...description.authorizedOperations].join(', ')}`);
      }
    } catch (error) {
      fail(`Failed to describe topic ${topic}`, error);
    }
  }
});

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

// Creates topics, or with --validate-only checks that they could be created.

const { CreateTopicsOptions, NewTopic } = require('@confluentinc/kafka-javascript/admin');
const { causeOf, describe, fail, futureOf, main, parseInt16, parseInt32, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/create_topics.js [-X key=value]... [--validate-only] <topic>[:partitions[:replication_factor]]...';

main(USAGE, async (admin, args) => {
  const validateOnly = args.flag('validate-only');
  const specs = args.positional();
  if (specs.length === 0) usage('no topic given');
  const newTopics = specs.map((spec) => {
    const [name, partitions, replicationFactor, ...rest] = spec.split(':');
    if (name === '' || rest.length > 0) usage(`invalid topic spec ${JSON.stringify(spec)}`);
    // A missing count is null: the broker's num.partitions / default.replication.factor.
    return new NewTopic({
      name,
      numPartitions: partitions === undefined ? null : parseInt32('partition count', partitions),
      replicationFactor: replicationFactor === undefined ? null : parseInt16('replication factor', replicationFactor),
    });
  });
  // The call returns at once; each topic's outcome arrives on its own future.
  const result = admin.createTopics({ newTopics, options: new CreateTopicsOptions({ validateOnly }) });
  const verb = validateOnly ? 'validated' : 'created';
  for (const newTopic of newTopics) {
    const name = newTopic.name();
    try {
      await futureOf(result.values(), name).get();
    } catch (error) {
      fail(`Failed to create topic ${name}`, error);
      continue;
    }
    // The broker also returns what it created: these refinements of the
    // topic's future fail on their own when it withholds them, for example
    // from a caller without DESCRIBE_CONFIGS on the topic, which is still
    // created.
    try {
      const topicId = await result.topicId(name).get();
      const numPartitions = await result.numPartitions(name).get();
      const replicationFactor = await result.replicationFactor(name).get();
      console.log(`Topic ${name} ${verb}: id ${topicId}, ${numPartitions} partitions, replication factor ${replicationFactor}`);
    } catch (error) {
      console.log(`Topic ${name} ${verb}; its details were not returned: ${describe(causeOf(error))}`);
    }
  }
});

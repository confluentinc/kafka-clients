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

// Deletes topics by name.

const { TopicCollection } = require('@confluentinc/kafka-javascript/common');
const { fail, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/delete_topics.js [-X key=value]... <topic>...';

main(USAGE, async (admin, args) => {
  const topics = args.positional();
  if (topics.length === 0) usage('no topic given');
  const result = admin.deleteTopics(TopicCollection.ofTopicNames(topics));
  // A by-name call keys its futures by topic name.
  const futures = result.topicNameValues();
  if (futures === null) return;
  for (const topic of new Set(topics)) {
    try {
      await futures.get(topic)?.get();
      console.log(`Topic ${topic} deleted`);
    } catch (error) {
      fail(`Failed to delete topic ${topic}`, error);
    }
  }
});

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

// Lists the topics, with --internal the internal ones too.

const { ListTopicsOptions } = require('@confluentinc/kafka-javascript/admin');
const { compareStrings, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/list_topics.js [-X key=value]... [--internal]';

main(USAGE, async (admin, args) => {
  const listInternal = args.flag('internal');
  if (args.positional().length > 0) usage('unexpected argument');
  // One future for the whole listing.
  const listings = await admin.listTopics(new ListTopicsOptions({ listInternal })).listings().get();
  listings.sort((a, b) => compareStrings(String(a.name()), String(b.name())));
  for (const listing of listings) {
    console.log(`Topic ${listing.name()} (id ${listing.topicId()})${listing.isInternal() ? ' internal' : ''}`);
  }
});

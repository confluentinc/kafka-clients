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

// Deletes groups.

const { fail, futureOf, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/delete_consumer_groups.js [-X key=value]... <group>...';

main(USAGE, async (admin, args) => {
  const groupIds = args.positional();
  if (groupIds.length === 0) usage('no group given');
  const futures = admin.deleteConsumerGroups(groupIds).deletedGroups();
  // In command-line order, as Rust's example prints; a group named twice prints twice.
  for (const group of groupIds) {
    try {
      await futureOf(futures, group).get();
      console.log(`Group ${group} deleted`);
    } catch (error) {
      fail(`Failed to delete group ${group}`, error);
    }
  }
});

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

// Describes transactions by transactional id.

const { compareStrings, fail, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/describe_transactions.js [-X key=value]... <transactional_id>...';

main(USAGE, async (admin, args) => {
  const transactionalIds = args.positional();
  if (transactionalIds.length === 0) usage('no transactional id given');
  const result = admin.describeTransactions(transactionalIds);
  for (const transactionalId of new Set(transactionalIds)) {
    try {
      const description = await result.description(transactionalId).get();
      console.log(
        `Transaction ${transactionalId}: state ${description.state}, coordinator ${description.coordinatorId}, ` +
          `producer id ${description.producerId}, producer epoch ${description.producerEpoch}, ` +
          `timeout ${description.transactionTimeoutMs} ms, start time ${description.transactionStartTimeMs ?? 'none'}`,
      );
      const partitions = [...(description.topicPartitions ?? [])].sort(
        (a, b) => compareStrings(String(a.topic), String(b.topic)) || a.partition - b.partition,
      );
      for (const partition of partitions) console.log(`  partition ${partition}`);
    } catch (error) {
      fail(`Failed to describe transaction ${transactionalId}`, error);
    }
  }
});

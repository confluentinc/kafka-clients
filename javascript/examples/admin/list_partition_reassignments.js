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

// Lists the ongoing partition reassignments, of every partition or of some.

const { compareStrings, main, topicPartitions } = require('./common');

const USAGE = 'usage: node examples/admin/list_partition_reassignments.js [-X key=value]... [<topic> <partition>]...';

main(USAGE, async (admin, args) => {
  const partitions = topicPartitions(args.positional(), false);
  const result = partitions.length === 0 ? admin.listPartitionReassignments() : admin.listPartitionReassignments(partitions);
  const reassignments = [...(await result.reassignments().get())].sort(
    ([a], [b]) => compareStrings(String(a.topic), String(b.topic)) || a.partition - b.partition,
  );
  if (reassignments.length === 0) console.log('No ongoing reassignments');
  for (const [partition, reassignment] of reassignments) {
    console.log(
      `Partition ${partition}: replicas [${reassignment.replicas().join(',')}], ` +
        `adding [${reassignment.addingReplicas().join(',')}], removing [${reassignment.removingReplicas().join(',')}]`,
    );
  }
});

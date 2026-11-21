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

// Describes consumer groups: their state, coordinator and members.

const { DescribeConsumerGroupsOptions } = require('@confluentinc/kafka-javascript/admin');
const { compareStrings, fail, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/describe_consumer_groups.js [-X key=value]... [--authorized-operations] <group>...';

/**
 * @param {import('@confluentinc/kafka-javascript/admin').MemberAssignment} assignment
 * @returns {string}
 */
function partitions(assignment) {
  const sorted = [...assignment.topicPartitions()].sort(
    (a, b) => compareStrings(String(a.topic), String(b.topic)) || a.partition - b.partition,
  );
  return `[${sorted.join(',')}]`;
}

main(USAGE, async (admin, args) => {
  const includeAuthorizedOperations = args.flag('authorized-operations');
  const groupIds = args.positional();
  if (groupIds.length === 0) usage('no group given');
  const result = admin.describeConsumerGroups({ groupIds, options: new DescribeConsumerGroupsOptions({ includeAuthorizedOperations }) });
  const futures = result.describedGroups();
  for (const group of new Set(groupIds)) {
    try {
      const description = await futures.get(group)?.get();
      if (description === undefined) continue;
      const coordinator = description.coordinator === null ? 'none' : description.coordinator.id;
      console.log(
        `Group ${description.groupId}: type ${description.type}, state ${description.groupState}, ` +
          `assignor ${JSON.stringify(description.partitionAssignor)}, coordinator ${coordinator}, simple ${description.isSimpleConsumerGroup}`,
      );
      if (description.groupEpoch !== null && description.targetAssignmentEpoch !== null) {
        console.log(`  group epoch ${description.groupEpoch}, target assignment epoch ${description.targetAssignmentEpoch}`);
      }
      for (const member of description.members) {
        console.log(
          `  member ${member.consumerId}: group instance id ${member.groupInstanceId ?? 'none'}, ` +
            `client id ${JSON.stringify(member.clientId)}, host ${member.host}, assignment ${partitions(member.assignment)}`,
        );
      }
      if (description.authorizedOperations !== null) {
        console.log(`  authorized operations: ${[...description.authorizedOperations].join(', ')}`);
      }
    } catch (error) {
      fail(`Failed to describe group ${group}`, error);
    }
  }
});

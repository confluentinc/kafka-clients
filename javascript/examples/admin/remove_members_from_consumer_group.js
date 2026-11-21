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

// Removes static members from a group by group instance id, or every member.

const { MemberToRemove, RemoveMembersFromConsumerGroupOptions } = require('@confluentinc/kafka-javascript/admin');
const { fail, main, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/remove_members_from_consumer_group.js [-X key=value]... [--reason=text] <group> [<instance_id>]...';

main(USAGE, async (admin, args) => {
  const reason = args.value('reason');
  const [groupId, ...instanceIds] = args.positional();
  if (groupId === undefined) usage('no group given');
  const members = instanceIds.map((id) => new MemberToRemove(id));
  // No member: every member of the group.
  const options = members.length === 0 ? new RemoveMembersFromConsumerGroupOptions() : new RemoveMembersFromConsumerGroupOptions(members);
  options.reason(reason);
  const result = admin.removeMembersFromConsumerGroup({ groupId, options });
  if (members.length === 0) {
    try {
      await result.all().get();
      console.log(`Group ${groupId}: all members removed`);
    } catch (error) {
      fail(`Failed to remove all members from group ${groupId}`, error);
    }
    return;
  }
  for (const member of members) {
    const instanceId = member.groupInstanceId();
    try {
      await result.memberResult(member).get();
      console.log(`Group ${groupId}: member ${instanceId} removed`);
    } catch (error) {
      fail(`Failed to remove member ${instanceId} from group ${groupId}`, error);
    }
  }
});

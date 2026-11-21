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

// Lists the groups, optionally only those in some states or of some types.

const { ListGroupsOptions } = require('@confluentinc/kafka-javascript/admin');
const { GroupState, GroupType } = require('@confluentinc/kafka-javascript/common');
const { choose, commaList, compareStrings, fail, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/list_groups.js [-X key=value]... [--states=state,...] [--types=type,...]';

/** @type {Array<[string, GroupState]>} */
const STATES = [
  ['preparing_rebalance', GroupState.PREPARING_REBALANCE],
  ['completing_rebalance', GroupState.COMPLETING_REBALANCE],
  ['stable', GroupState.STABLE],
  ['dead', GroupState.DEAD],
  ['empty', GroupState.EMPTY],
  ['assigning', GroupState.ASSIGNING],
  ['reconciling', GroupState.RECONCILING],
  ['not_ready', GroupState.NOT_READY],
  ['unknown', GroupState.UNKNOWN],
];

/** @type {Array<[string, GroupType]>} */
const TYPES = [
  ['consumer', GroupType.CONSUMER],
  ['classic', GroupType.CLASSIC],
  ['share', GroupType.SHARE],
  ['streams', GroupType.STREAMS],
];

main(USAGE, async (admin, args) => {
  const states = commaList(args.value('states') ?? '').map((state) => choose('group state', state, STATES));
  const types = commaList(args.value('types') ?? '').map((type) => choose('group type', type, TYPES));
  if (args.positional().length > 0) usage('unexpected positional argument');
  const result = admin.listGroups(new ListGroupsOptions({ groupStates: new Set(states), types: new Set(types) }));
  const listings = await result.valid().get();
  listings.sort((a, b) => compareStrings(String(a.groupId()), String(b.groupId())));
  for (const listing of listings) {
    console.log(
      `Group ${listing.groupId()}: type ${listing.type() ?? 'none'}, state ${listing.groupState() ?? 'none'}, ` +
        `protocol ${JSON.stringify(listing.protocol())}, simple ${listing.isSimpleConsumerGroup()}`,
    );
  }
  // The brokers that failed to answer, each with its error.
  for (const error of await result.errors().get()) fail('Failed to list groups', error);
});

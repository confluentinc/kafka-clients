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

// Lists the transactions, optionally filtered.

const { ListTransactionsOptions, TransactionState } = require('@confluentinc/kafka-javascript/admin');
const { choose, commaList, compareStrings, main, parseLong, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/list_transactions.js [-X key=value]... [--states=<state>,...] ' +
  '[--producer-ids=<id>,...] [--duration-ms=<ms>] [--pattern=<regex>]';

/** @type {Array<[string, TransactionState]>} */
const STATES = [
  ['ongoing', TransactionState.ONGOING],
  ['prepareabort', TransactionState.PREPARE_ABORT],
  ['preparecommit', TransactionState.PREPARE_COMMIT],
  ['completeabort', TransactionState.COMPLETE_ABORT],
  ['completecommit', TransactionState.COMPLETE_COMMIT],
  ['empty', TransactionState.EMPTY],
  ['prepareepochfence', TransactionState.PREPARE_EPOCH_FENCE],
];

main(USAGE, async (admin, args) => {
  const states = args.value('states');
  const producerIds = args.value('producer-ids');
  const duration = args.value('duration-ms');
  const pattern = args.value('pattern');
  if (args.positional().length > 0) usage('unexpected positional argument');
  const options = new ListTransactionsOptions();
  if (states !== null) options.filterStates(commaList(states).map((state) => choose('state', state, STATES)));
  if (producerIds !== null) options.filterProducerIds(commaList(producerIds).map((id) => parseLong('producer id', id)));
  if (duration !== null) options.filterOnDuration(parseLong('duration', duration));
  if (pattern !== null) options.filterOnTransactionalIdPattern(pattern);
  const listings = await admin.listTransactions(options).all().get();
  if (listings.length === 0) console.log('No transactions');
  listings.sort((a, b) => compareStrings(String(a.transactionalId()), String(b.transactionalId())));
  for (const listing of listings) {
    console.log(`Transaction ${listing.transactionalId()}: producer id ${listing.producerId()}, state ${listing.state()}`);
  }
});

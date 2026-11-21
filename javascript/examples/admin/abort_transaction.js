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

// Aborts a hanging transaction on one partition.

const { AbortTransactionSpec } = require('@confluentinc/kafka-javascript/admin');
const { main, parseInt16, parseInt32, parseLong, topicPartition, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/abort_transaction.js [-X key=value]... <topic> <partition> <producer_id> <producer_epoch> <coordinator_epoch>';

main(USAGE, async (admin, args) => {
  const positional = args.positional();
  if (positional.length !== 5) usage('expected a topic, a partition, a producer id, a producer epoch and a coordinator epoch');
  const partition = topicPartition(positional);
  const producerId = parseLong('producer id', positional[2]);
  const producerEpoch = parseInt16('producer epoch', positional[3]);
  const coordinatorEpoch = parseInt32('coordinator epoch', positional[4]);
  const spec = new AbortTransactionSpec({
    topicPartition: partition,
    producerId,
    producerEpoch,
    coordinatorEpoch: Math.max(coordinatorEpoch, 0),
  });
  await admin.abortTransaction(spec).all().get();
  console.log(`Aborted transaction of producer id ${producerId}, producer epoch ${producerEpoch} on ${partition}`);
});

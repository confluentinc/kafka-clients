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

// Describes the cluster: its id, its controller and its brokers.

const { DescribeClusterOptions } = require('@confluentinc/kafka-javascript/admin');
const { main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/describe_cluster.js [-X key=value]... [--authorized-operations] [--fenced]';

/**
 * @param {import('@confluentinc/kafka-javascript/common').Node} node
 * @returns {string}
 */
function node(node) {
  return `${node.id} at ${node.host}:${node.port}${node.rack === null ? '' : `, rack ${node.rack}`}${node.isFenced ? ', fenced' : ''}`;
}

main(USAGE, async (admin, args) => {
  const includeAuthorizedOperations = args.flag('authorized-operations');
  const includeFencedBrokers = args.flag('fenced');
  if (args.positional().length > 0) usage('unexpected argument');
  const result = admin.describeCluster(new DescribeClusterOptions({ includeAuthorizedOperations, includeFencedBrokers }));
  console.log(`Cluster id: ${await result.clusterId().get()}`);
  const controller = await result.controller().get();
  console.log(`Controller: ${controller === null ? 'none' : node(controller)}`);
  const nodes = await result.nodes().get();
  for (const broker of nodes.sort((a, b) => a.id - b.id)) console.log(`Broker ${node(broker)}`);
  const operations = await result.authorizedOperations().get();
  if (operations !== null) console.log(`Authorized operations: ${[...operations].join(', ')}`);
});

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

// Describes the cluster's finalized and supported features.

const { DescribeFeaturesOptions } = require('@confluentinc/kafka-javascript/admin');
const { compareStrings, main, parseInt32, usage } = require('./common');

const USAGE = 'usage: node examples/admin/describe_features.js [-X key=value]... [--node-id=<id>]';

main(USAGE, async (admin, args) => {
  const nodeId = args.value('node-id');
  if (args.positional().length > 0) usage('unexpected positional argument');
  // No node id: the client picks a broker.
  const options = nodeId === null ? new DescribeFeaturesOptions() : new DescribeFeaturesOptions({ nodeId: parseInt32('node id', nodeId) });
  const metadata = await admin.describeFeatures(options).featureMetadata().get();
  console.log(`Finalized features epoch: ${metadata.finalizedFeaturesEpoch() ?? 'none'}`);
  for (const [name, range] of [...metadata.finalizedFeatures()].sort(([a], [b]) => compareStrings(a, b))) {
    console.log(`Finalized feature ${name}: min level ${range.minVersionLevel()}, max level ${range.maxVersionLevel()}`);
  }
  for (const [name, range] of [...metadata.supportedFeatures()].sort(([a], [b]) => compareStrings(a, b))) {
    console.log(`Supported feature ${name}: min version ${range.minVersion()}, max version ${range.maxVersion()}`);
  }
});

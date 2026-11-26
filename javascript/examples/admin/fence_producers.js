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

// Fences the producers of transactional ids, bumping their epochs.

const { fail, main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/fence_producers.js [-X key=value]... <transactional_id>...';

main(USAGE, async (admin, args) => {
  const transactionalIds = args.positional();
  if (transactionalIds.length === 0) usage('no transactional id given');
  const result = admin.fenceProducers(transactionalIds);
  for (const transactionalId of new Set(transactionalIds)) {
    try {
      const producerId = await result.producerId(transactionalId).get();
      const epoch = await result.epochId(transactionalId).get();
      console.log(`Fenced ${transactionalId}: producer id ${producerId}, epoch ${epoch}`);
    } catch (error) {
      fail(`Failed to fence ${transactionalId}`, error);
    }
  }
});

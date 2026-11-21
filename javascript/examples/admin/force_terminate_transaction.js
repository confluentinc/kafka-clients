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

// Forcefully terminates a transaction by transactional id.

const { main, usage } = require('./common');

const USAGE = 'usage: node examples/admin/force_terminate_transaction.js [-X key=value]... <transactional_id>';

main(USAGE, async (admin, args) => {
  const positional = args.positional();
  if (positional.length !== 1) usage('expected exactly one transactional id');
  const [transactionalId] = positional;
  await admin.forceTerminateTransaction(transactionalId).result().get();
  console.log(`Transaction ${transactionalId} terminated`);
});

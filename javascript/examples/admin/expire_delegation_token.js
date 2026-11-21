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

// Expires a delegation token, given its HMAC in hex.

const { ExpireDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
const { fromHex, main, parseLong, usage } = require('./common');

const USAGE = 'usage: node examples/admin/expire_delegation_token.js [-X key=value]... [--period-ms=<ms>] <hmac_hex>';

main(USAGE, async (admin, args) => {
  const period = args.value('period-ms');
  const positional = args.positional();
  if (positional.length !== 1) usage('expected exactly one HMAC');
  const hmac = fromHex(positional[0]);
  const options = new ExpireDelegationTokenOptions();
  if (period !== null) options.expiryTimePeriodMs(parseLong('expiry period', period));
  const expiryTimestamp = await admin.expireDelegationToken({ hmac, options }).expiryTimestamp().get();
  console.log(`Delegation token expired, expiry timestamp: ${expiryTimestamp}`);
});

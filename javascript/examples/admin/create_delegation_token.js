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

// Creates a delegation token and prints it, its HMAC included.

const { CreateDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
const { main, parseLong, principal, toHex, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/create_delegation_token.js [-X key=value]... [--renewer=<type>:<name>]... ' +
  '[--owner=<type>:<name>] [--max-lifetime-ms=<ms>]';

main(USAGE, async (admin, args) => {
  const renewers = args.values('renewer').map(principal);
  const owner = args.value('owner');
  const maxLifetime = args.value('max-lifetime-ms');
  if (args.positional().length > 0) usage('unexpected positional argument');
  const options = new CreateDelegationTokenOptions({ renewers, owner: owner === null ? null : principal(owner) });
  if (maxLifetime !== null) options.maxLifetimeMs(parseLong('max lifetime', maxLifetime));
  const token = await admin.createDelegationToken(options).delegationToken().get();
  const info = token.tokenInfo();
  console.log(`Created delegation token ${info.tokenId()}`);
  console.log(`Owner: ${info.ownerAsString()}`);
  console.log(`Requester: ${info.tokenRequesterAsString()}`);
  console.log(`Renewers: ${info.renewersAsString().join(',')}`);
  console.log(`Issue timestamp: ${info.issueTimestamp()}`);
  console.log(`Expiry timestamp: ${info.expiryTimestamp()}`);
  console.log(`Max timestamp: ${info.maxTimestamp()}`);
  console.log(`HMAC (base64): ${token.hmacAsBase64String()}`);
  console.log(`HMAC (hex): ${toHex(token.hmac())}`);
});

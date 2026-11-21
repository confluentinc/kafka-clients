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

// Describes the delegation tokens, all of them or those of some owners.

const { DescribeDelegationTokenOptions } = require('@confluentinc/kafka-javascript/admin');
const { compareStrings, main, principal, toHex } = require('./common');

const USAGE = 'usage: node examples/admin/describe_delegation_token.js [-X key=value]... [--show-hmac] [<type>:<name>]...';

main(USAGE, async (admin, args) => {
  const showHmac = args.flag('show-hmac');
  const owners = args.positional().map(principal);
  // No owner: every token the caller may describe.
  const options = owners.length === 0 ? new DescribeDelegationTokenOptions() : new DescribeDelegationTokenOptions({ owners });
  const tokens = await admin.describeDelegationToken(options).delegationTokens().get();
  if (tokens.length === 0) console.log('No delegation tokens');
  tokens.sort((a, b) => compareStrings(a.tokenInfo().tokenId(), b.tokenInfo().tokenId()));
  for (const token of tokens) {
    const info = token.tokenInfo();
    console.log(
      `Token ${info.tokenId()}: owner ${info.ownerAsString()}, requester ${info.tokenRequesterAsString()}, ` +
        `renewers [${info.renewersAsString().join(',')}], issued ${info.issueTimestamp()}, expires ${info.expiryTimestamp()}, ` +
        `max ${info.maxTimestamp()}`,
    );
    if (showHmac) console.log(`  HMAC (hex): ${toHex(token.hmac())}`);
  }
});

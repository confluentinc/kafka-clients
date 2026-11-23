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

// Describes users' SCRAM credentials: their mechanisms and iterations.

const { compareStrings, fail, main } = require('./common');

const USAGE = 'usage: node examples/admin/describe_user_scram_credentials.js [-X key=value]... [<user>...]';

/** @param {import('@confluentinc/kafka-javascript/admin').UserScramCredentialsDescription} description */
function printDescription(description) {
  const credentials = description.credentialInfos.map((info) => `${info.mechanism().mechanismName()} iterations=${info.iterations()}`);
  console.log(`User ${description.name}: ${credentials.sort().join(', ')}`);
}

main(USAGE, async (admin, args) => {
  const users = args.positional();
  if (users.length === 0) {
    // Every user with a credential, in one future.
    const descriptions = await admin.describeUserScramCredentials().all().get();
    for (const [, description] of [...descriptions].sort(([a], [b]) => compareStrings(a, b))) printDescription(description);
    return;
  }
  const result = admin.describeUserScramCredentials(users);
  for (const user of new Set(users)) {
    try {
      printDescription(await result.description(user).get());
    } catch (error) {
      fail(`Failed to describe SCRAM credentials of user ${user}`, error);
    }
  }
});

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

// Upserts or deletes users' SCRAM credentials.

const {
  ScramCredentialInfo,
  ScramMechanism,
  UserScramCredentialDeletion,
  UserScramCredentialUpsertion,
} = require('@confluentinc/kafka-javascript/admin');
const { fail, futureOf, main, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/alter_user_scram_credentials.js [-X key=value]... ' +
  '(UPSERT <user> <mechanism> <iterations> <password> [<salt>] | DELETE <user> <mechanism>)...';

/**
 * @param {string} text
 * @returns {ScramMechanism}
 */
function mechanism(text) {
  const parsed = ScramMechanism.fromMechanismName(text.toUpperCase());
  // The password may follow: never repeat the arguments in the error.
  if (parsed === ScramMechanism.UNKNOWN) usage('invalid mechanism, expected SCRAM-SHA-256 or SCRAM-SHA-512');
  return parsed;
}

/**
 * @param {string[]} args
 * @returns {Array<UserScramCredentialUpsertion | UserScramCredentialDeletion>}
 */
function alterations(args) {
  const out = [];
  let at = 0;
  /** @param {string} keyword @param {number} count */
  const take = (keyword, count) => {
    if (args.length - at < count) usage(`${keyword} needs ${count} arguments`);
    at += count;
    return args.slice(at - count, at);
  };
  while (at < args.length) {
    const keyword = args[at++];
    if (keyword === 'UPSERT') {
      const [user, mechanismName, iterations, password] = take('UPSERT', 4);
      // Rust's i32 parse: a whole number within Java's int.
      const count = /^[+-]?\d+$/.test(iterations) ? Number(iterations) : NaN;
      if (!(count >= -(2 ** 31) && count < 2 ** 31)) usage('invalid iterations, expected a number');
      const credentialInfo = new ScramCredentialInfo({ mechanism: mechanism(mechanismName), iterations: count });
      const salt = args[at];
      if (salt !== undefined && salt !== 'UPSERT' && salt !== 'DELETE') {
        at++;
        // With a salt Java takes the password as bytes.
        out.push(
          new UserScramCredentialUpsertion({
            user,
            credentialInfo,
            password: Buffer.from(password, 'utf8'),
            salt: Buffer.from(salt, 'utf8'),
          }),
        );
      } else {
        out.push(new UserScramCredentialUpsertion({ user, credentialInfo, password }));
      }
    } else if (keyword === 'DELETE') {
      const [user, mechanismName] = take('DELETE', 2);
      out.push(new UserScramCredentialDeletion({ user, mechanism: mechanism(mechanismName) }));
    } else {
      usage('expected UPSERT or DELETE');
    }
  }
  if (out.length === 0) usage('no alteration given');
  return out;
}

main(USAGE, async (admin, args) => {
  const requested = alterations(args.positional());
  const result = admin.alterUserScramCredentials(requested);
  const futures = result.values();
  // One future per user, covering all of that user's alterations, in
  // command-line order (first occurrence), as Rust's example prints.
  for (const user of new Set(requested.map((alteration) => alteration.user()))) {
    try {
      await futureOf(futures, user).get();
      console.log(`Altered SCRAM credentials of user ${user}`);
    } catch (error) {
      fail(`Failed to alter SCRAM credentials of user ${user}`, error);
    }
  }
});

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

// Describes brokers' log dirs and the replicas they hold.

const { compareStrings, fail, futureOf, main, parseInt32, usage } = require('./common');

const USAGE = 'usage: node examples/admin/describe_log_dirs.js [-X key=value]... <broker_id>...';

/**
 * @param {bigint | null} value
 * @returns {string}
 */
function bytes(value) {
  return value === null ? 'unknown' : String(value);
}

main(USAGE, async (admin, args) => {
  const positional = args.positional();
  if (positional.length === 0) usage('no broker given');
  const brokers = [...new Set(positional.map((text) => parseInt32('broker id', text)))];
  const result = admin.describeLogDirs(brokers);
  const futures = result.descriptions();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const broker of brokers) {
    let descriptions;
    try {
      descriptions = await futureOf(futures, broker).get();
    } catch (error) {
      fail(`Failed to describe log dirs of broker ${broker}`, error);
      continue;
    }
    console.log(`Broker ${broker}: ${descriptions.size} log dirs`);
    for (const [dir, description] of [...descriptions].sort(([a], [b]) => compareStrings(a, b))) {
      if (description.error !== null) {
        fail(`Failed to describe log dir ${dir} of broker ${broker}`, description.error);
        continue;
      }
      console.log(
        `  log dir ${dir}: total ${bytes(description.totalBytes)} bytes, usable ${bytes(description.usableBytes)} bytes` +
          `${description.isCordoned ? ', cordoned' : ''}`,
      );
      const replicas = [...description.replicaInfos].sort(
        ([a], [b]) => compareStrings(String(a.topic), String(b.topic)) || a.partition - b.partition,
      );
      for (const [partition, info] of replicas) {
        console.log(`    replica ${partition}: size ${info.size()}, offset lag ${info.offsetLag()}${info.isFuture() ? ', future' : ''}`);
      }
    }
  }
});

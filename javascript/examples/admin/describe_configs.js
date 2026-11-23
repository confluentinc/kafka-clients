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

// Describes the configs of topics, brokers, broker loggers, groups or client
// metrics subscriptions.

const { DescribeConfigsOptions } = require('@confluentinc/kafka-javascript/admin');
const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
const { compareStrings, fail, futureOf, groups, main, RESOURCE_TYPES, resourceLabel, resourceType } = require('./common');

const USAGE = `usage: node examples/admin/describe_configs.js [-X key=value]... [--synonyms] [--documentation] <type> <name>...\n${RESOURCE_TYPES}`;

/**
 * @param {import('@confluentinc/kafka-javascript/admin').ConfigEntry} entry
 * @returns {string}
 */
function entryLine(entry) {
  const value = entry.isSensitive() ? '******' : (entry.value() ?? 'null');
  const flags = [
    [entry.isDefault(), 'default'],
    [entry.isSensitive(), 'sensitive'],
    [entry.isReadOnly(), 'read-only'],
  ]
    .filter(([set]) => set)
    .map(([, flag]) => flag);
  return `${entry.name()} = ${value} [${entry.source()}]${flags.length === 0 ? '' : ` (${flags.join(', ')})`}`;
}

main(USAGE, async (admin, args) => {
  const includeSynonyms = args.flag('synonyms');
  const includeDocumentation = args.flag('documentation');
  // A resource named twice is described once.
  /** @type {ConfigResourceMap<ConfigResource>} */
  const resources = new ConfigResourceMap();
  for (const [type, name] of groups(args.positional(), 2, true)) {
    const resource = new ConfigResource({ type: resourceType(type), name });
    resources.set(resource, resource);
  }
  const result = admin.describeConfigs({
    resources: [...resources.keys()],
    options: new DescribeConfigsOptions({ includeSynonyms, includeDocumentation }),
  });
  const futures = result.values();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const resource of resources.keys()) {
    try {
      const config = await futureOf(futures, resource).get();
      console.log(`Configs of ${resourceLabel(resource)}:`);
      for (const entry of config.entries().sort((a, b) => compareStrings(a.name(), b.name()))) {
        console.log(`  ${entryLine(entry)}`);
        for (const synonym of entry.synonyms() ?? []) {
          const value = entry.isSensitive() ? '******' : (synonym.value() ?? 'null');
          console.log(`    synonym ${synonym.name()} = ${value} [${synonym.source()}]`);
        }
        const documentation = entry.documentation();
        if (documentation) console.log(`    documentation: ${documentation}`);
      }
    } catch (error) {
      fail(`Failed to describe configs of ${resourceLabel(resource)}`, error);
    }
  }
});

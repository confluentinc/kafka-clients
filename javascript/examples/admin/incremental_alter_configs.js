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

// Alters configs one key at a time: SET, APPEND, SUBTRACT or DELETE.

const { AlterConfigOp, ConfigEntry } = require('@confluentinc/kafka-javascript/admin');
const { ConfigResource, ConfigResourceMap } = require('@confluentinc/kafka-javascript/common/config');
const { choose, fail, futureOf, groups, main, RESOURCE_TYPES, resourceLabel, resourceType, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/incremental_alter_configs.js [-X key=value]... <type> <name> <key=OP:value;key=DELETE;...>...\n' +
  `${RESOURCE_TYPES}; OP: SET, APPEND, SUBTRACT`;

/**
 * @param {string} text
 * @returns {AlterConfigOp[]}
 */
function operations(text) {
  const ops = text
    .split(';')
    .filter((spec) => spec !== '')
    .map((spec) => {
      const expected = `invalid operation ${JSON.stringify(spec)}: expected key=OP:value or key=DELETE`;
      const at = spec.indexOf('=');
      if (at < 0) usage(expected);
      const name = spec.slice(0, at);
      const operation = spec.slice(at + 1);
      if (operation.toLowerCase() === 'delete') {
        return new AlterConfigOp({ configEntry: new ConfigEntry({ name, value: null }), operationType: AlterConfigOp.OpType.DELETE });
      }
      const colon = operation.indexOf(':');
      if (colon < 0) usage(expected);
      const operationType = choose('operation', operation.slice(0, colon), [
        ['set', AlterConfigOp.OpType.SET],
        ['append', AlterConfigOp.OpType.APPEND],
        ['subtract', AlterConfigOp.OpType.SUBTRACT],
      ]);
      return new AlterConfigOp({ configEntry: new ConfigEntry({ name, value: operation.slice(colon + 1) }), operationType });
    });
  if (ops.length === 0) usage(`no operation in ${JSON.stringify(text)}`);
  return ops;
}

main(USAGE, async (admin, args) => {
  /** @type {ConfigResourceMap<AlterConfigOp[]>} */
  const configs = new ConfigResourceMap();
  for (const [type, name, ops] of groups(args.positional(), 3, true)) {
    const resource = new ConfigResource({ type: resourceType(type), name });
    configs.set(resource, [...(configs.get(resource) ?? []), ...operations(ops)]);
  }
  const result = admin.incrementalAlterConfigs(configs);
  const futures = result.values();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const resource of configs.keys()) {
    try {
      await futureOf(futures, resource).get();
      console.log(`Altered configs of ${resourceLabel(resource)}`);
    } catch (error) {
      fail(`Failed to alter configs of ${resourceLabel(resource)}`, error);
    }
  }
});

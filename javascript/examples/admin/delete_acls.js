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

// Deletes the ACLs matching filters.

const { ACL_ARGUMENTS, aclBindingFilter, bindingText, fail, futureOf, groups, main } = require('./common');

const USAGE = `usage: node examples/admin/delete_acls.js [-X key=value]... (${ACL_ARGUMENTS})...`;

/**
 * @param {import('@confluentinc/kafka-javascript/common/acl').AclBindingFilter} filter
 * @returns {string}
 */
function filterText(filter) {
  const pattern = filter.patternFilter();
  const entry = filter.entryFilter();
  return [
    pattern.resourceType(),
    pattern.name() ?? 'ANY',
    pattern.patternType(),
    entry.principal() ?? 'ANY',
    entry.host() ?? 'ANY',
    entry.operation(),
    entry.permissionType(),
  ].join(' ');
}

main(USAGE, async (admin, args) => {
  const filters = groups(args.positional(), 7, true).map(aclBindingFilter);
  const result = admin.deleteAcls(filters);
  const futures = result.values();
  // In command-line order, as Rust's example prints.
  for (const filter of filters) {
    let results;
    try {
      results = (await futureOf(futures, filter).get()).values();
    } catch (error) {
      fail(`Failed to delete ACLs matching ${filterText(filter)}`, error);
      continue;
    }
    console.log(`Filter ${filterText(filter)}: ${results.length} matching ACLs`);
    // A matched ACL can fail to delete on its own.
    for (const filterResult of results) {
      const binding = filterResult.binding();
      const error = filterResult.exception();
      if (error !== null)
        fail(
          binding === null ? `Failed to delete an ACL matching ${filterText(filter)}` : `Failed to delete ACL ${bindingText(binding)}`,
          error,
        );
      else if (binding !== null) console.log(`  deleted ACL ${bindingText(binding)}`);
    }
  }
});

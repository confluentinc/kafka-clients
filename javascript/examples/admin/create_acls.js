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

// Creates ACLs.

const { ACL_ARGUMENTS, aclBinding, bindingText, fail, futureOf, groups, main } = require('./common');

const USAGE = `usage: node examples/admin/create_acls.js [-X key=value]... (${ACL_ARGUMENTS})...`;

main(USAGE, async (admin, args) => {
  const acls = groups(args.positional(), 7, true).map(aclBinding);
  const result = admin.createAcls(acls);
  const futures = result.values();
  // In command-line order, as Rust's example prints.
  for (const binding of acls) {
    try {
      await futureOf(futures, binding).get();
      console.log(`Created ACL ${bindingText(binding)}`);
    } catch (error) {
      fail(`Failed to create ACL ${bindingText(binding)}`, error);
    }
  }
});

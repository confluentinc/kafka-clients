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

// Describes the ACLs matching a filter.

const { ACL_ARGUMENTS, aclBindingFilter, bindingText, main, usage } = require('./common');

const USAGE = `usage: node examples/admin/describe_acls.js [-X key=value]... ${ACL_ARGUMENTS}`;

main(USAGE, async (admin, args) => {
  const positional = args.positional();
  if (positional.length !== 7) usage('expected exactly one filter of 7 arguments');
  // One future for the whole call.
  const lines = (await admin.describeAcls(aclBindingFilter(positional)).values().get()).map(bindingText).sort();
  for (const line of lines) console.log(`ACL ${line}`);
  console.log(`${lines.length} ACLs match`);
});

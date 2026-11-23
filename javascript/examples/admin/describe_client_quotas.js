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

// Describes the client quotas of the entities matching a filter.

const { ClientQuotaFilter, ClientQuotaFilterComponent } = require('@confluentinc/kafka-javascript/common/quota');
const { entityText, groups, main } = require('./common');

const USAGE = 'usage: node examples/admin/describe_client_quotas.js [-X key=value]... [--strict] [<entity_type> <name|DEFAULT|ANY>]...';

main(USAGE, async (admin, args) => {
  const strict = args.flag('strict');
  const components = groups(args.positional(), 2, false).map(([entityType, name]) => {
    if (name === 'DEFAULT') return ClientQuotaFilterComponent.ofDefaultEntity(entityType);
    if (name === 'ANY') return ClientQuotaFilterComponent.ofEntityType(entityType);
    return ClientQuotaFilterComponent.ofEntity({ entityType, entityName: name });
  });
  let filter;
  if (strict) filter = ClientQuotaFilter.containsOnly(components);
  else filter = components.length === 0 ? ClientQuotaFilter.all() : ClientQuotaFilter.contains(components);
  const entities = await admin.describeClientQuotas(filter).entities().get();
  const lines = [...(entities ?? [])]
    .map(
      ([entity, quotas]) =>
        `${entityText(entity)}: ${[...quotas]
          .map(([key, value]) => `${key}=${value}`)
          .sort()
          .join(', ')}`,
    )
    .sort();
  for (const line of lines) console.log(line);
  console.log(`${lines.length} entities match`);
});

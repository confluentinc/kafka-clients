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

// Lists the config resources of the given types, or of every type.

const { ListConfigResourcesOptions } = require('@confluentinc/kafka-javascript/admin');
const { RESOURCE_TYPES, main, resourceType } = require('./common');

const USAGE = `usage: node examples/admin/list_config_resources.js [-X key=value]... [<type>...]\n${RESOURCE_TYPES}`;

main(USAGE, async (admin, args) => {
  const types = new Set(args.positional().map(resourceType));
  const result =
    types.size === 0
      ? admin.listConfigResources()
      : admin.listConfigResources({ configResourceTypes: types, options: new ListConfigResourcesOptions() });
  const resources = (await result.all().get()).map((resource) => `${resource.type().name().toLowerCase()} ${resource.name()}`);
  for (const line of resources.sort()) console.log(line);
});

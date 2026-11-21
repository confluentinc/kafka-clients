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

// Sets or removes an entity's client quotas.

const { AlterClientQuotasOptions } = require('@confluentinc/kafka-javascript/admin');
const { ClientQuotaAlteration, ClientQuotaEntity } = require('@confluentinc/kafka-javascript/common/quota');
const { commaList, entityText, fail, main, parseDouble, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/alter_client_quotas.js [-X key=value]... [--validate-only] ' +
  '<entity_type>=<name|DEFAULT>[,<entity_type>=<name|DEFAULT>]... <key>=<value|REMOVE>...';

main(USAGE, async (admin, args) => {
  const validateOnly = args.flag('validate-only');
  const [entitySpec, ...opSpecs] = args.positional();
  if (entitySpec === undefined) usage('no entity given');
  if (opSpecs.length === 0) usage('no quota given');
  // DEFAULT is the default entity, Java's null name.
  /** @type {Map<string, string | null>} */
  const entries = new Map();
  for (const entry of commaList(entitySpec)) {
    const at = entry.indexOf('=');
    if (at < 0) usage(`invalid entity ${JSON.stringify(entry)}: expected type=name`);
    const name = entry.slice(at + 1);
    entries.set(entry.slice(0, at), name === 'DEFAULT' ? null : name);
  }
  const entity = new ClientQuotaEntity(entries);
  // A null value removes the quota.
  const ops = opSpecs.map((spec) => {
    const at = spec.indexOf('=');
    if (at < 0) usage(`invalid quota ${JSON.stringify(spec)}: expected key=value`);
    const value = spec.slice(at + 1);
    return new ClientQuotaAlteration.Op({ key: spec.slice(0, at), value: value === 'REMOVE' ? null : parseDouble('quota value', value) });
  });
  const result = admin.alterClientQuotas({
    entries: [new ClientQuotaAlteration({ entity, ops })],
    options: new AlterClientQuotasOptions({ validateOnly }),
  });
  try {
    await result.values()?.get(entity)?.get();
    console.log(`Client quotas of ${entityText(entity)} ${validateOnly ? 'validated' : 'altered'}`);
  } catch (error) {
    fail(`Failed to alter client quotas of ${entityText(entity)}`, error);
  }
});

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

// Upgrades or downgrades finalized features, or with --validate-only checks
// that the change would be accepted.

const { FeatureUpdate, UpdateFeaturesOptions } = require('@confluentinc/kafka-javascript/admin');
const { choose, describe, fail, futureOf, groups, main, parseInt16, usage } = require('./common');

const USAGE =
  'usage: node examples/admin/update_features.js [-X key=value]... [--validate-only] ' +
  '(<feature> <max_level> <upgrade|safe_downgrade|unsafe_downgrade>)...';

main(USAGE, async (admin, args) => {
  const validateOnly = args.flag('validate-only');
  /** @type {Map<string, FeatureUpdate>} */
  const featureUpdates = new Map();
  for (const [name, level, type] of groups(args.positional(), 3, true)) {
    const maxVersionLevel = parseInt16('max version level', level);
    const upgradeType = choose('upgrade type', type, [
      ['upgrade', FeatureUpdate.UpgradeType.UPGRADE],
      ['safe_downgrade', FeatureUpdate.UpgradeType.SAFE_DOWNGRADE],
      ['unsafe_downgrade', FeatureUpdate.UpgradeType.UNSAFE_DOWNGRADE],
    ]);
    let update;
    try {
      update = new FeatureUpdate({ maxVersionLevel, upgradeType });
    } catch (error) {
      usage(describe(error));
    }
    if (featureUpdates.has(name)) usage(`feature ${name} given twice`);
    featureUpdates.set(name, update);
  }
  const result = admin.updateFeatures({ featureUpdates, options: new UpdateFeaturesOptions({ validateOnly }) });
  const futures = result.values();
  // In command-line order (first occurrence), as Rust's example prints.
  for (const name of featureUpdates.keys()) {
    try {
      await futureOf(futures, name).get();
      console.log(`Feature ${name} ${validateOnly ? 'validated' : 'updated'}: max level ${featureUpdates.get(name)?.maxVersionLevel()}`);
    } catch (error) {
      fail(`Failed to update feature ${name}`, error);
    }
  }
});

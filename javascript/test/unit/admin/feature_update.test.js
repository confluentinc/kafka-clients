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

// FeatureUpdate and its UpgradeType, which Java ships without tests: the Rust
// core's tests of its translation (feature_update.rs), and Java's equals,
// hashCode, toString and fromCode.

const assert = require('node:assert/strict');

const { FeatureUpdate } = require('@confluentinc/kafka-javascript/admin');

const { UpgradeType } = FeatureUpdate;

test('upgrade_type_codes_round_trip: an unrecognized code falls back to UNKNOWN', () => {
  assert.deepEqual(
    UpgradeType.values().map((t) => [t.name(), t.code(), t.ordinal()]),
    [
      ['UNKNOWN', 0, 0],
      ['UPGRADE', 1, 1],
      ['SAFE_DOWNGRADE', 2, 2],
      ['UNSAFE_DOWNGRADE', 3, 3],
    ],
  );
  for (const t of UpgradeType.values()) assert.equal(UpgradeType.fromCode(t.code()), t);
  assert.equal(UpgradeType.fromCode({ code: 42 }), UpgradeType.UNKNOWN);
  assert.equal(UpgradeType.fromCode(-1), UpgradeType.UNKNOWN);
  assert.equal(UpgradeType.valueOf('SAFE_DOWNGRADE'), UpgradeType.SAFE_DOWNGRADE);
  assert.throws(() => UpgradeType.valueOf('NONE'), {
    name: 'IllegalArgumentError',
    message: 'No enum constant org.apache.kafka.clients.admin.FeatureUpdate.UpgradeType.NONE',
  });
  assert.equal(String(UpgradeType.UPGRADE), 'UPGRADE');
  assert.equal(JSON.stringify(UpgradeType.UPGRADE), '"UPGRADE"');
});

test('new_valid_update and new_deletion_with_downgrade_is_allowed', () => {
  const update = new FeatureUpdate({ maxVersionLevel: 2, upgradeType: UpgradeType.UPGRADE });
  assert.equal(update.maxVersionLevel(), 2);
  assert.equal(update.upgradeType(), UpgradeType.UPGRADE);
  const deletion = new FeatureUpdate({ maxVersionLevel: 0, upgradeType: UpgradeType.SAFE_DOWNGRADE });
  assert.equal(deletion.maxVersionLevel(), 0);
  assert.equal(deletion.upgradeType(), UpgradeType.SAFE_DOWNGRADE);
});

test("new_rejects_deletion_with_upgrade_flag and new_rejects_negative_version_level: Java's messages", () => {
  assert.throws(() => new FeatureUpdate({ maxVersionLevel: 0, upgradeType: UpgradeType.UPGRADE }), {
    name: 'IllegalArgumentError',
    message: 'The upgradeType flag should be set to SAFE_DOWNGRADE or UNSAFE_DOWNGRADE when the provided maxVersionLevel:0 is < 1.',
  });
  assert.throws(() => new FeatureUpdate({ maxVersionLevel: -1, upgradeType: UpgradeType.SAFE_DOWNGRADE }), {
    name: 'IllegalArgumentError',
    message: 'Cannot specify a negative version level.',
  });
  assert.throws(() => new FeatureUpdate({ maxVersionLevel: 32768, upgradeType: UpgradeType.UPGRADE }), {
    name: 'RangeError',
    message: 'maxVersionLevel must be from -32768 to 32767',
  });
});

test('a null upgrade type: Java dereferences it for a deletion, and stores it otherwise', () => {
  // @ts-expect-error a null upgrade type
  assert.throws(() => new FeatureUpdate({ maxVersionLevel: 0, upgradeType: null }), {
    name: 'TypeError',
    message: 'upgradeType must not be null',
  });
  // @ts-expect-error a null upgrade type
  const stored = new FeatureUpdate({ maxVersionLevel: 2, upgradeType: null });
  assert.equal(stored.upgradeType(), null);
  assert.equal(String(stored), 'FeatureUpdate{maxVersionLevel:2, upgradeType:null}');
  // Java's equals dereferences it once the levels match.
  assert.throws(() => stored.equals(new FeatureUpdate({ maxVersionLevel: 2, upgradeType: UpgradeType.UPGRADE })), {
    name: 'TypeError',
    message: 'upgradeType must not be null',
  });
});

test('to_string_follows_java; equals, hashCode and toJSON', () => {
  const update = new FeatureUpdate({ maxVersionLevel: 0, upgradeType: UpgradeType.SAFE_DOWNGRADE });
  assert.equal(String(update), 'FeatureUpdate{maxVersionLevel:0, upgradeType:SAFE_DOWNGRADE}');
  const same = new FeatureUpdate({ maxVersionLevel: 0, upgradeType: UpgradeType.SAFE_DOWNGRADE });
  assert.ok(update.equals(same));
  assert.equal(update.hashCode(), same.hashCode());
  assert.ok(!update.equals(new FeatureUpdate({ maxVersionLevel: 0, upgradeType: UpgradeType.UNSAFE_DOWNGRADE })));
  assert.ok(!update.equals({ other: null }));
  assert.equal(JSON.stringify(update), '{"maxVersionLevel":0,"upgradeType":"SAFE_DOWNGRADE"}');
});

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

// FeatureMetadata, which Java ships without tests: the Rust core's test of
// its translation (rust/src/admin/feature_metadata.rs), over Java's
// package-private constructor (the library's internalCreate), and one read
// from the mock's describeFeatures, with Java's getters returning new maps,
// equals, hashCode and toString.

const assert = require('node:assert/strict');

const { FeatureMetadata, FinalizedVersionRange, MockAdminClient } = require('@confluentinc/kafka-javascript/admin');

test('equality_is_order_insensitive', () => {
  const create =
    /** @type {{ internalCreate(f: Map<string, import('@confluentinc/kafka-javascript/admin').FinalizedVersionRange>, e: bigint | null, s: Map<string, import('@confluentinc/kafka-javascript/admin').SupportedVersionRange>): import('@confluentinc/kafka-javascript/admin').FeatureMetadata }} */ (
      /** @type {unknown} */ (FeatureMetadata)
    );
  const range = (/** @type {number} */ min, /** @type {number} */ max) =>
    new FinalizedVersionRange({ minVersionLevel: min, maxVersionLevel: max });
  const a = create.internalCreate(
    new Map([
      ['f1', range(1, 2)],
      ['f2', range(2, 3)],
    ]),
    1n,
    new Map(),
  );
  const b = create.internalCreate(
    new Map([
      ['f2', range(2, 3)],
      ['f1', range(1, 2)],
    ]),
    1n,
    new Map(),
  );
  assert.ok(a.equals(b));
  assert.equal(a.hashCode(), b.hashCode());
});

test("the mock's metadata: new maps each read, Java's toString and an epoch as a bigint", async () => {
  const mock = MockAdminClient.create()
    .featureLevels(new Map([['f', 2]]))
    .minSupportedFeatureLevels(new Map([['f', 1]]))
    .maxSupportedFeatureLevels(new Map([['f', 3]]))
    .build();
  try {
    const metadata = await mock.describeFeatures().featureMetadata().get();
    assert.ok(metadata instanceof FeatureMetadata);
    const finalized = metadata.finalizedFeatures();
    assert.notEqual(metadata.finalizedFeatures(), finalized);
    finalized.clear();
    assert.equal(metadata.finalizedFeatures().size, 1);
    assert.ok(new FinalizedVersionRange({ minVersionLevel: 2, maxVersionLevel: 2 }).equals(metadata.finalizedFeatures().get('f')));
    assert.equal(metadata.finalizedFeaturesEpoch(), 123n);
    assert.equal(
      String(metadata),
      'FeatureMetadata{finalizedFeatures:{(f -> FinalizedVersionRange[min_version_level:2, max_version_level:2])}, ' +
        'finalizedFeaturesEpoch:123, supportedFeatures:{(f -> SupportedVersionRange[min_version:1, max_version:3])}}',
    );
    assert.equal(
      JSON.stringify(metadata),
      '{"finalizedFeatures":{"f":{"minVersionLevel":2,"maxVersionLevel":2}},"finalizedFeaturesEpoch":"123",' +
        '"supportedFeatures":{"f":{"minVersion":1,"maxVersion":3}}}',
    );
    const again = await mock.describeFeatures().featureMetadata().get();
    assert.notEqual(again, metadata);
    assert.ok(again.equals(metadata));
    assert.equal(again.hashCode(), metadata.hashCode());
  } finally {
    await mock.close();
  }
});

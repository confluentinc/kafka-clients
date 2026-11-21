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

// The values the offset calls hand the addon (src/internal/admin_offset_calls.ts),
// as Python's test_admin_client_topics.py::test_offset_spec_values and
// test_election_reassignment_and_offset_args pin its own: each OffsetSpec as
// the number whose case of native/admin_values.c's offset_spec_of builds the
// spec of the same name, and an empty set of partitions as an empty list, not
// Java's null (every partition).

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const { OffsetSpec } = require('@confluentinc/kafka-javascript/admin');
const { TopicPartition } = require('@confluentinc/kafka-javascript/common');

const untyped = require('../../../dist/internal/admin_offset_calls');

// The conversions' signatures, from src/ (dist/internal/ has no declarations).
const calls = {
  offsetSpecFields: /** @type {typeof import('../../../src/internal/admin_offset_calls').offsetSpecFields} */ (
    /** @type {unknown} */ (untyped.offsetSpecFields)
  ),
  partitionPairs: /** @type {typeof import('../../../src/internal/admin_offset_calls').partitionPairs} */ (
    /** @type {unknown} */ (untyped.partitionPairs)
  ),
};

/** @type {Array<[OffsetSpec | null, number, bigint, string]>} */
const SPECS = [
  [OffsetSpec.earliest(), 0, 0n, 'earliest'],
  [OffsetSpec.latest(), 1, 0n, 'latest'],
  [OffsetSpec.maxTimestamp(), 2, 0n, 'max_timestamp'],
  [OffsetSpec.earliestLocal(), 3, 0n, 'earliest_local'],
  [OffsetSpec.latestTiered(), 4, 0n, 'latest_tiered'],
  [OffsetSpec.earliestPendingUpload(), 5, 0n, 'earliest_pending_upload'],
  [OffsetSpec.forTimestamp(1234n), 6, 1234n, 'for_timestamp'],
  // Java's getOffsetFromSpec falls through to the latest offset.
  [new OffsetSpec(), 1, 0n, 'latest'],
  [null, 1, 0n, 'latest'],
];

test('each OffsetSpec crosses as a number and a timestamp', () => {
  for (const [spec, number, timestamp] of SPECS) assert.deepEqual(calls.offsetSpecFields(spec), [number, timestamp]);
});

// Reads native/, which the tests of the installed package have not
// (tools/package-smoke/installed-suites.js lists it as repository-only).
test("each OffsetSpec's number is offset_spec_of's case building the spec of its name", () => {
  const source = fs.readFileSync(path.join(__dirname, '..', '..', '..', 'native', 'admin_values.c'), 'utf8');
  const body = source.slice(source.indexOf('offset_spec_of(int32_t spec'));
  for (const [, number, , name] of SPECS) {
    const built =
      name === 'latest'
        ? /default: return kafka_admin_OffsetSpec_latest_err/
        : new RegExp(`case ${number}: return kafka_admin_OffsetSpec_${name}_err`);
    assert.match(body.slice(0, body.indexOf('\n}')), built);
  }
});

test('an empty set of partitions crosses as an empty list, not as every partition', () => {
  assert.deepEqual(calls.partitionPairs([], 'partitions'), []);
  const tp = new TopicPartition({ topic: 't', partition: 1 });
  // A Set<TopicPartition>: equal partitions once.
  assert.deepEqual(calls.partitionPairs([tp, new TopicPartition({ topic: 't', partition: 1 })], 'partitions'), [['t', 1]]);
});

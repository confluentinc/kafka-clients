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

// UpdateFeaturesOptions, which Java ships without tests: validateOnly's
// getter / setter pair, false until set.

const assert = require('node:assert/strict');

const { UpdateFeaturesOptions } = require('@confluentinc/kafka-javascript/admin');

test('validateOnly reads false until set', () => {
  const o = new UpdateFeaturesOptions();
  assert.equal(o.validateOnly(), false);
  assert.equal(o.validateOnly(true), o);
  assert.equal(o.validateOnly(), true);
  assert.equal(o.validateOnly({ validateOnly: false }).validateOnly(), false);
  assert.equal(new UpdateFeaturesOptions({ validateOnly: true }).validateOnly(), true);
  // @ts-expect-error a boolean Java's setter takes
  assert.throws(() => o.validateOnly({ validateOnly: 1 }), { name: 'TypeError', message: 'validateOnly must be a boolean, not a number' });
  // @ts-expect-error a key the class has not
  assert.throws(() => new UpdateFeaturesOptions({ nodeId: 1 }), {
    name: 'IllegalArgumentError',
    message: 'UpdateFeaturesOptions() takes an object of the keys (timeoutMs, validateOnly); got (nodeId)',
  });
});

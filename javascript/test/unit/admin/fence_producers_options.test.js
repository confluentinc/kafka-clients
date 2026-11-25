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

// FenceProducersOptions, which Java ships without tests: its keyed constructor and Java's
// toString.

const assert = require('node:assert/strict');

const { FenceProducersOptions } = require('@confluentinc/kafka-javascript/admin');

test("the timeout key and Java's toString", () => {
  assert.equal(String(new FenceProducersOptions()), 'FenceProducersOptions{timeoutMs=null}');
  assert.equal(String(new FenceProducersOptions({ timeoutMs: 100 })), 'FenceProducersOptions{timeoutMs=100}');
  // @ts-expect-error a key the class has not
  assert.throws(() => new FenceProducersOptions({ brokerId: 1 }), {
    name: 'IllegalArgumentError',
    message: 'FenceProducersOptions() takes an object of the keys (timeoutMs); got (brokerId)',
  });
});

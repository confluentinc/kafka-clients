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

// SslClientAuth, as python/test/unit/test_config_classes.py checks the Python one.

const test = require('node:test');
const assert = require('node:assert/strict');

const { SslClientAuth } = require('@confluentinc/kafka-javascript/common/config');

test('SslClientAuth', () => {
  assert.deepEqual(SslClientAuth.VALUES.map((a) => a.name), ['REQUIRED', 'REQUESTED', 'NONE']);
  assert.deepEqual(SslClientAuth.VALUES, [SslClientAuth.REQUIRED, SslClientAuth.REQUESTED, SslClientAuth.NONE]);
  assert.equal(SslClientAuth.VALUES[0], SslClientAuth.REQUIRED);
  assert.ok(Object.isFrozen(SslClientAuth.VALUES));
  assert.ok(Object.isFrozen(SslClientAuth.NONE));
  assert.equal(String(SslClientAuth.REQUESTED), 'requested');
  assert.equal(SslClientAuth.forConfig(null), SslClientAuth.NONE);
  assert.equal(SslClientAuth.forConfig(undefined), SslClientAuth.NONE);
  assert.equal(SslClientAuth.forConfig('required'), SslClientAuth.REQUIRED);
  assert.equal(SslClientAuth.forConfig('ReQuEsTeD'), SslClientAuth.REQUESTED);
  assert.equal(SslClientAuth.forConfig('other'), null);
  assert.throws(() => SslClientAuth.forConfig(5), { name: 'TypeError', message: 'key must be a string, not number' });
  assert.throws(() => new SslClientAuth(), { name: 'TypeError', message: 'SslClientAuth is an enum: use its constants' });
});

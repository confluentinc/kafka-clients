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

// SecurityProtocol, as python/test/unit/test_config_classes.py checks the
// Python one.

const test = require('node:test');
const assert = require('node:assert/strict');

const { IllegalArgumentError } = require('@confluentinc/kafka-javascript');
const { SecurityProtocol } = require('@confluentinc/kafka-javascript/common/security/auth');

test('SecurityProtocol', () => {
  assert.deepEqual(
    SecurityProtocol.values().map((p) => [p.name, p.id]),
    [['PLAINTEXT', 0], ['SSL', 1], ['SASL_PLAINTEXT', 2], ['SASL_SSL', 3]],
  );
  assert.deepEqual(SecurityProtocol.names(), ['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']);
  assert.equal(SecurityProtocol.forId(3), SecurityProtocol.SASL_SSL);
  assert.equal(SecurityProtocol.forId(9), null);
  assert.equal(SecurityProtocol.forName('sasl_plaintext'), SecurityProtocol.SASL_PLAINTEXT);
  assert.equal(String(SecurityProtocol.SSL), 'SSL');
  assert.ok(Object.isFrozen(SecurityProtocol.SSL));
  assert.throws(() => SecurityProtocol.forName('tls'), (e) => {
    assert.ok(e instanceof IllegalArgumentError);
    assert.equal(e.message, 'No enum constant org.apache.kafka.common.security.auth.SecurityProtocol.TLS');
    return true;
  });
  assert.throws(() => SecurityProtocol.forName(null), { name: 'TypeError', message: 'name must be a string, not null' });
  assert.throws(() => new SecurityProtocol(), { name: 'TypeError', message: 'SecurityProtocol is an enum: use its constants' });
});

// Copyright 2025 Confluent Inc.
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

const test = require('node:test');
const assert = require('node:assert/strict');

const { KafkaError, KafkaProducer } = require('../..');

test('an invalid config is a KafkaError with the core message', () => {
  assert.throws(() => new KafkaProducer({ 'bootstrap.servers': 'localhost:9092', acks: 'bogus' }), (e) => {
    assert.ok(e instanceof KafkaError);
    assert.ok(e instanceof Error);
    assert.equal(e.code, -10);
    assert.equal(e.name, 'CONFIG');
    assert.equal(e.message, 'Invalid value bogus for configuration acks: String must be one of: all, -1, 0, 1');
    assert.equal(e.retriable, false);
    assert.equal(e.apiError, false);
    assert.equal(e.cause, undefined);
    return true;
  });
  assert.throws(() => new KafkaProducer({}), {
    name: 'CONFIG',
    message: 'Missing required configuration "bootstrap.servers" which has no default value.',
  });
});

test('the cause chain follows Error_source', () => {
  assert.throws(
    () =>
      new KafkaProducer({
        'bootstrap.servers': 'localhost:9092',
        'security.protocol': 'SSL',
        'ssl.truststore.location': '/nonexistent/truststore.jks',
      }),
    (e) => {
      assert.ok(e instanceof KafkaError);
      assert.equal(e.name, 'UNKNOWN_SERVER_ERROR');
      assert.equal(e.code, -1);
      assert.equal(e.message, 'Failed to construct kafka producer');
      assert.ok(e.cause instanceof KafkaError);
      assert.equal(e.cause.name, 'CONFIG');
      assert.equal(
        e.cause.message,
        "Failed to create channel builder: Failed to open truststore file '/nonexistent/truststore.jks': " +
          'No such file or directory (os error 2)',
      );
      return true;
    },
  );
});

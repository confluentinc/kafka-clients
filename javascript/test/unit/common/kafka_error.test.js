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

const { KafkaError } = require('@confluentinc/kafka-javascript/common');
const { ConfigError } = require('@confluentinc/kafka-javascript/common/config');
const { KafkaProducer } = require('@confluentinc/kafka-javascript/producer');
const { errorFfiId } = require('../../../dist/internal/errors');

test('an invalid config is a KafkaError with the core message', () => {
  assert.throws(() => new KafkaProducer({ configs: { 'bootstrap.servers': 'localhost:9092', acks: 'bogus' } }), (e) => {
    assert.ok(e instanceof ConfigError);
    assert.ok(e instanceof KafkaError);
    assert.ok(e instanceof Error);
    assert.equal(e.name, 'ConfigError');
    assert.equal(e.message, 'Invalid value bogus for configuration acks: String must be one of: all, -1, 0, 1');
    assert.equal(e.cause, undefined);
    // The FFI id is private (`_ffiId`, left out of the declarations): only
    // `name`, `message` and `cause` are public.
    assert.equal(errorFfiId(e), -10);
    assert.equal(ConfigError._ffiId, -10);
    assert.ok(!Object.keys(e).includes('_ffiId'));
    assert.ok(!('code' in e) && !('errorName' in e) && !('retriable' in e) && !('apiError' in e));
    return true;
  });
  assert.throws(() => new KafkaProducer({ configs: {} }), {
    name: 'ConfigError',
    message: 'Missing required configuration "bootstrap.servers" which has no default value.',
  });
});

test('the cause chain follows Error_source', () => {
  assert.throws(
    () =>
      new KafkaProducer({
        configs: {
          'bootstrap.servers': 'localhost:9092',
          'security.protocol': 'SSL',
          'ssl.truststore.location': '/nonexistent/truststore.jks',
        },
      }),
    (e) => {
      // A bare KafkaException: reported as UNKNOWN_SERVER_ERROR, but not an
      // ApiException, so not UnknownServerError.
      assert.equal(e.constructor, KafkaError);
      assert.equal(e.name, 'KafkaError');
      assert.equal(errorFfiId(e), -1);
      assert.equal(e.message, 'Failed to construct kafka producer');
      assert.ok(e.cause instanceof ConfigError);
      assert.equal(e.cause.name, 'ConfigError');
      assert.equal(
        e.cause.message,
        "Failed to create channel builder: Failed to open truststore file '/nonexistent/truststore.jks': " +
          'No such file or directory (os error 2)',
      );
      return true;
    },
  );
});

test("Java's INSTANCE singletons: an instance of the class, built by Java's no-argument chain", () => {
  const errors = require('@confluentinc/kafka-javascript/common/errors');
  for (const cls of [errors.DisconnectError, errors.CoordinatorNotAvailableError]) {
    const instance = cls.INSTANCE;
    assert.ok(instance instanceof cls, cls.name);
    assert.equal(instance.constructor, cls);
    assert.equal(instance.name, cls.name);
    assert.equal(instance.message, '');
    assert.equal(instance.cause, undefined);
    // Java's final field: the same instance every read, not writable.
    assert.equal(cls.INSTANCE, instance);
    assert.throws(() => {
      'use strict';
      cls.INSTANCE = null;
    }, TypeError);
    assert.equal(cls.INSTANCE, instance);
  }
  // CoordinatorNotAvailableException's no-argument constructor is private:
  // only INSTANCE is built by it.
  assert.throws(() => new errors.CoordinatorNotAvailableError(), {
    name: 'IllegalArgumentError',
    message: 'CoordinatorNotAvailableError() takes one of (message), ({ message, cause }); got ()',
  });
});
